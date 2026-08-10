import asyncio
import json
import logging
import time
import datetime
import threading
import sqlite3
import gc
import socketio
import pyotp
import pytz
from aiohttp import web
from requests.exceptions import ReadTimeout

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- 🕒 TIMEZONE & LOGGING SETUP ---
IST = pytz.timezone('Asia/Kolkata')

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MUNH_TITAN_PROD")

# --- ⚙️ INTERNAL APP STATE ENGINE ---
class AppState:
    def __init__(self):
        self.smart_api = None
        self.db_path = "angel_master.db"
        self.score = 0

state = AppState()

# --- 🔑 CREDENTIALS ---
API_KEY = "Z80wG5Sg"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"

# --- 🚀 GLOBAL STATES & SCORE TRACKING ---
LTP_CACHE = {}               
SUBSCRIBED_TOKENS_REGISTRY = {1: set(), 2: set(), 3: set(), 4: set(), 5: set()}
BROKER_SOCKET_CONNECTED = False
USER_SCORE = 0 
IS_RECONNECTING = False  # Guard against thread spams and rate limits

# --- 📊 CANDLE ENGINE CONFIGURATION (200 CANDLES MAX) ---
TIMEFRAMES = {
    "1M": 60,
    "3M": 180,
    "5M": 300,
    "15M": 900,
    "25M": 1500,
    "30M": 1800,
    "1H": 3600,
    "1D": 86400
}
MAX_CANDLES_LIMIT = 200
CANDLE_CACHE = {}  # Format: { token_str: { tf_key: [ {time, open, high, low, close}, ... ] } }

BROKER_JWT_TOKEN = None
BROKER_FEED_TOKEN = None
LAST_BROKER_LOGIN_TIME = 0

main_loop = None
sws_client = None

# Socket.IO & Aiohttp Setup
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# --- 🔄 EXCHANGE CODE NORMALIZER ---
def normalize_exchange(exch):
    """
    Returns string exchange format required by Angel One API
    """
    ex_str = str(exch).upper().strip()
    if ex_str in ["5", "MCX", "MCX_FO", "MCXFO"]:
        return "MCX"
    elif ex_str in ["2", "NFO", "NSE_FO"]:
        return "NFO"
    elif ex_str in ["3", "BSE", "BSE_CM", "BFO"]:
        return "BFO"
    elif ex_str in ["4", "CDS", "CNO"]:
        return "CDS"
    return "NSE"

def get_exchange_code_num(exch):
    ex_str = str(exch).upper().strip()
    if ex_str in ["5", "MCX", "MCX_FO", "MCXFO"]:
        return 5
    elif ex_str in ["2", "NFO", "NSE_FO"]:
        return 2
    elif ex_str in ["3", "BSE", "BSE_CM", "BFO"]:
        return 3
    elif ex_str in ["4", "CDS", "CNO"]:
        return 4
    return 1

# --- 🎯 SCORE LOGIC ---
def update_user_score(points=1):
    global USER_SCORE
    USER_SCORE += points
    state.score = USER_SCORE
    logger.info(f"📊 Current User Score: {USER_SCORE}")
    return USER_SCORE

# --- 🔐 SMART API LOGIN & RECONNECT SYSTEM ---
def ensure_smart_api_session():
    global BROKER_JWT_TOKEN, BROKER_FEED_TOKEN, LAST_BROKER_LOGIN_TIME
    now = time.time()
    
    # Re-login every 6 hours if session expires
    if state.smart_api and (now - LAST_BROKER_LOGIN_TIME < 21600):
        return True

    try:
        logger.info("🔒 Initializing SmartAPI Login...")
        state.smart_api = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_STR).now()
        data = state.smart_api.generateSession(CLIENT_CODE, MPIN, totp)
        
        if data and data.get('status'):
            BROKER_JWT_TOKEN = data['data']['jwtToken']
            BROKER_FEED_TOKEN = state.smart_api.getfeedToken()
            LAST_BROKER_LOGIN_TIME = now
            logger.info("✅ SmartAPI Login Successful!")
            return True
        else:
            logger.error(f"❌ SmartAPI Login Failed: {data}")
            return False
    except Exception as e:
        logger.error(f"❌ Exception in SmartAPI Login: {e}")
        return False

# --- 📈 LIVE TICK CANDLE AGGREGATOR (REALTIME ONLY) ---
def update_token_candles(token_str, price_val):
    if price_val <= 0:
        return

    now_sec = int(time.time())
    if token_str not in CANDLE_CACHE:
        CANDLE_CACHE[token_str] = {}

    token_candles = CANDLE_CACHE[token_str]

    for tf_key, interval_sec in TIMEFRAMES.items():
        bucket_time = (now_sec // interval_sec) * interval_sec

        if tf_key not in token_candles:
            token_candles[tf_key] = []

        tf_list = token_candles[tf_key]

        if not tf_list:
            tf_list.append({
                "time": bucket_time,
                "open": price_val,
                "high": price_val,
                "low": price_val,
                "close": price_val
            })
        else:
            last_candle = tf_list[-1]
            if last_candle["time"] == bucket_time:
                last_candle["high"] = max(last_candle["high"], price_val)
                last_candle["low"] = min(last_candle["low"], price_val)
                last_candle["close"] = price_val
            elif bucket_time > last_candle["time"]:
                tf_list.append({
                    "time": bucket_time,
                    "open": price_val,
                    "high": price_val,
                    "low": price_val,
                    "close": price_val
                })
                if len(tf_list) > MAX_CANDLES_LIMIT:
                    tf_list.pop(0)

# --- 📡 SMART WEBSOCKET V2 HANDLERS ---
def on_data(ws, message):
    try:
        if isinstance(message, dict) and "token" in message and "last_traded_price" in message:
            token = str(message["token"])
            # Angel One sends LTP multiplied by 100
            ltp = float(message["last_traded_price"]) / 100.0 if message["last_traded_price"] > 10000 else float(message["last_traded_price"])
            
            LTP_CACHE[token] = ltp
            update_token_candles(token, ltp)

            # Broadcast to connected Android App clients via Socket.IO
            if main_loop and main_loop.is_running():
                tick_payload = {"token": token, "ltp": ltp}
                asyncio.run_coroutine_threadsafe(sio.emit('tick_update', tick_payload), main_loop)
    except Exception as e:
        logger.error(f"❌ Error processing websocket tick: {e}")

def on_open(ws):
    global BROKER_SOCKET_CONNECTED
    BROKER_SOCKET_CONNECTED = True
    logger.info("⚡ SmartAPI WebSocket Connected Successfully!")
    subscribe_registered_tokens()

def on_error(ws, error):
    global BROKER_SOCKET_CONNECTED
    BROKER_SOCKET_CONNECTED = False
    logger.error(f"❌ WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    global BROKER_SOCKET_CONNECTED
    BROKER_SOCKET_CONNECTED = False
    logger.warning(f"⚠️ WebSocket Connection Closed: {close_status_code} - {close_msg}")

def start_broker_websocket():
    global sws_client
    if not ensure_smart_api_session():
        return

    try:
        sws_client = SmartWebSocketV2(BROKER_JWT_TOKEN, API_KEY, CLIENT_CODE, BROKER_FEED_TOKEN)
        sws_client.on_data = on_data
        sws_client.on_open = on_open
        sws_client.on_error = on_error
        sws_client.on_close = on_close

        # Run socket in background thread
        w_thread = threading.Thread(target=sws_client.connect, daemon=True)
        w_thread.start()
    except Exception as e:
        logger.error(f"❌ Failed to start SmartWebSocketV2: {e}")

def subscribe_registered_tokens():
    if not sws_client or not BROKER_SOCKET_CONNECTED:
        return

    for exch_num, tokens in SUBSCRIBED_TOKENS_REGISTRY.items():
        if tokens:
            token_list = list(tokens)
            # Action: 1 = Subscribe, Mode: 1 = LTP
            token_payload = [{"exchangeType": exch_num, "tokens": token_list}]
            try:
                sws_client.subscribe("smartapi_ltp", 1, token_payload)
                logger.info(f"📡 Subscribed {len(token_list)} tokens on Exchange {exch_num}")
            except Exception as e:
                logger.error(f"❌ Subscription failed for Exch {exch_num}: {e}")

# --- 🌐 SOCKET.IO CLIENT EVENTS ---
@sio.event
async def connect(sid, environ):
    logger.info(f"📱 Android Client Connected: {sid}")
    # Send cached LTPs immediately on connect
    if LTP_CACHE:
        await sio.emit('initial_ltps', LTP_CACHE, room=sid)

@sio.event
async def subscribe_tokens(sid, data):
    """
    Data format expected from Android app:
    {"tokens": [{"token": "288509", "exch": "NSE"}, ...]}
    """
    try:
        tokens_input = data.get("tokens", [])
        for item in tokens_input:
            token = str(item.get("token")).strip()
            exch_num = get_exchange_code_num(item.get("exch", "NSE"))
            if token:
                SUBSCRIBED_TOKENS_REGISTRY[exch_num].add(token)

        subscribe_registered_tokens()
    except Exception as e:
        logger.error(f"❌ Error in subscribe_tokens event: {e}")

@sio.event
async def disconnect(sid):
    logger.info(f"📱 Android Client Disconnected: {sid}")

# --- 🚀 API ROUTE: GET HISTORICAL CHART DATA ---
async def handle_get_chart_data(request):
    try:
        req_data = await request.json()
        token = str(req_data.get("token", "")).strip()
        symbol = str(req_data.get("symbol", "")).strip()
        exch_raw = str(req_data.get("exch", "NSE")).strip()
        interval = str(req_data.get("interval", "FIVE_MINUTE")).strip()
        from_date = req_data.get("fromdate")
        to_date = req_data.get("todate")

        if not token:
            return web.json_response({"status": False, "message": "Missing token parameter"}, status=400)

        exchange_str = normalize_exchange(exch_raw)

        # Fallback date generation if not sent by Android app
        now = datetime.datetime.now(IST)
        if not to_date:
            to_date = now.strftime("%Y-%m-%d %H:%M")
        if not from_date:
            from_date = (now - datetime.timedelta(days=5)).strftime("%Y-%m-%d %H:%M")

        # Ensure active API session
        if not ensure_smart_api_session():
            return web.json_response({"status": False, "message": "SmartAPI authentication failed"}, status=500)

        # SmartAPI Parameter Mapping
        historic_param = {
            "exchange": exchange_str,
            "symboltoken": token,
            "interval": interval,
            "fromdate": from_date,
            "todate": to_date
        }

        logger.info(f"📊 Fetching Candle Data with params: {historic_param}")
        
        # Execute Angel One Historical Data Call
        response = state.smart_api.getCandleData(historic_param)

        if response and response.get("status") and response.get("data"):
            raw_candles = response["data"]
            
            formatted_candles = []
            for candle in raw_candles:
                formatted_candles.append({
                    "time": candle[0],
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4])
                })

            return web.json_response({
                "status": True,
                "message": "SUCCESS",
                "data": formatted_candles
            })
        else:
            logger.error(f"❌ SmartAPI Historical Error Response: {response}")
            return web.json_response({
                "status": False,
                "message": response.get("message", "Failed to fetch historical data"),
                "data": []
            })

    except Exception as e:
        logger.error(f"❌ Exception in handle_get_chart_data: {e}", exc_info=True)
        return web.json_response({"status": False, "message": str(e), "data": []}, status=500)

# Attach Routes
app.router.add_post('/api/get_chart_data', handle_get_chart_data)

# Background Task for SmartWebSocket Start
async def start_background_tasks(app_instance):
    global main_loop
    main_loop = asyncio.get_event_loop()
    threading.Thread(target=start_broker_websocket, daemon=True).start()

app.on_startup.append(start_background_tasks)

# App startup logic
if __name__ == '__main__':
    ensure_smart_api_session()
    web.run_app(app, host='0.0.0.0', port=5000)
