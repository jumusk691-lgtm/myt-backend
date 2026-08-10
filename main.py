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
SUBSCRIBED_TOKENS_REGISTRY = {1: set(), 2: set(), 3: set(), 4: set(), 5: set(), 13: set()}
BROKER_SOCKET_CONNECTED = False
USER_SCORE = 0 

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
CANDLE_CACHE = {}

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
    ex_str = str(exch).upper().strip()
    if ex_str in ["5", "MCX", "MCX_FO", "MCXFO"]:
        return "MCX"
    elif ex_str in ["2", "NFO", "NSE_FO"]:
        return "NFO"
    elif ex_str in ["3", "BSE", "BSE_CM"]:
        return "BSE"
    elif ex_str in ["4", "CDS", "CNO"]:
        return "CDS"
    return "NSE"

def get_exchange_code_num(exch):
    ex_str = str(exch).upper().strip()
    if ex_str in ["5", "MCX", "MCX_FO", "MCXFO"]:
        return 5
    elif ex_str in ["2", "NFO", "NSE_FO"]:
        return 2
    elif ex_str in ["3", "BSE", "BSE_CM"]:
        return 3
    elif ex_str in ["4", "CDS", "CNO", "13"]:
        return 13
    return 1

# --- 🎯 SCORE LOGIC ---
def update_user_score(points=1):
    global USER_SCORE
    USER_SCORE += points
    state.score = USER_SCORE
    logger.info(f"📊 Current User Score: {USER_SCORE}")
    return USER_SCORE

# --- 📈 LIVE TICK CANDLE AGGREGATOR ---
def update_token_candles(token_str, price_val):
    if price_val <= 0:
        return

    now_sec = int(time.time())
    if token_str not in CANDLE_CACHE:
        CANDLE_CACHE[token_str] = {}

    token_candles = CANDLE_CACHE[token_str]

    for tf_key, interval_sec in TIMEFRAMES.items():
        if tf_key not in token_candles:
            token_candles[tf_key] = []
            
        tf_list = token_candles[tf_key]
        bucket_time = (now_sec // interval_sec) * interval_sec

        if not tf_list:
            token_candles[tf_key].append({
                "time": bucket_time, "open": price_val,
                "high": price_val, "low": price_val, "close": price_val
            })
        else:
            last_candle = tf_list[-1]
            if bucket_time > last_candle["time"]:
                token_candles[tf_key].append({
                    "time": bucket_time, "open": last_candle["close"],
                    "high": max(last_candle["close"], price_val),
                    "low": min(last_candle["close"], price_val),
                    "close": price_val
                })
                if len(tf_list) > MAX_CANDLES_LIMIT:
                    tf_list.pop(0)
            else:
                last_candle["close"] = price_val
                last_candle["high"] = max(last_candle["high"], price_val)
                last_candle["low"] = min(last_candle["low"], price_val)

# --- 🔐 RETRY & LOGIN LOGIC ---
def login_with_retry(smart_conn, client_code, mpin, totp, retries=3):
    for i in range(retries):
        try:
            session = smart_conn.generateSession(client_code, mpin, totp)
            if session and session.get('status'):
                return session
        except Exception as e:
            logger.error(f"Login Attempt {i+1} failed: {e}")
            time.sleep(2)
    return None

def force_broker_socket_restart():
    global sws_client, BROKER_SOCKET_CONNECTED
    if sws_client:
        try:
            sws_client.close()
        except Exception:
            pass

# --- 📡 WEBSOCKET HANDLERS ---
def on_data_received(ws, message):
    try:
        if isinstance(message, dict) and "token" in message and "last_traded_price" in message:
            token = str(message["token"]).strip()
            raw_ltp = float(message["last_traded_price"])
            
            # Angel One Paisa -> Rupee Conversion
            ltp = raw_ltp / 100.0 if raw_ltp > 10000 else raw_ltp
            
            LTP_CACHE[token] = ltp
            update_token_candles(token, ltp)

            if main_loop and main_loop.is_running():
                tick_payload = {"token": token, "ltp": ltp}
                asyncio.run_coroutine_threadsafe(sio.emit('tick_update', tick_payload), main_loop)
    except Exception as e:
        logger.error(f"❌ Error in on_data_received: {e}")

def on_websocket_open(wsapp):
    global BROKER_SOCKET_CONNECTED
    BROKER_SOCKET_CONNECTED = True
    logger.info("✅ Broker WebSocket Connected!")
    subscribe_registered_tokens()

def on_websocket_close(wsapp, code, msg):
    global BROKER_SOCKET_CONNECTED
    BROKER_SOCKET_CONNECTED = False
    logger.warning("⚠️ Broker WebSocket Closed. Reconnecting in 5s...")
    threading.Thread(target=lambda: (time.sleep(5), start_angel_one_websocket_worker(BROKER_JWT_TOKEN, BROKER_FEED_TOKEN)), daemon=True).start()

def subscribe_registered_tokens():
    if not sws_client or not BROKER_SOCKET_CONNECTED:
        return

    payload = []
    for exch_code, tokens_set in SUBSCRIBED_TOKENS_REGISTRY.items():
        if tokens_set:
            payload.append({"exchangeType": int(exch_code), "tokens": list(tokens_set)})

    if payload:
        try:
            sws_client.subscribe("munh_titan_live", 1, payload)
            logger.info(f"📡 Subscribed to tokens: {payload}")
        except Exception as e:
            logger.error(f"❌ WebSocket Subscription Error: {e}")

def start_angel_one_websocket_worker(auth_token, feed_token):
    global sws_client
    if not auth_token or not feed_token:
        return
    try:
        sws_client = SmartWebSocketV2(auth_token=auth_token, client_code=CLIENT_CODE, api_key=API_KEY, feed_token=feed_token)
        sws_client.on_data = on_data_received
        sws_client.on_open = on_websocket_open
        sws_client.on_close = on_websocket_close
        sws_client.connect()
    except Exception as e:
        logger.error(f"❌ Failed starting SmartWebSocketV2: {e}")

# --- 🌐 SOCKET.IO HANDLERS ---
@sio.event
async def connect(sid, environ):
    logger.info(f"📱 Android Client Connected: {sid}")
    if LTP_CACHE:
        await sio.emit('initial_ltps', LTP_CACHE, room=sid)

@sio.event
async def subscribe_tokens(sid, data):
    try:
        tokens_input = data.get("tokens", [])
        for item in tokens_input:
            token = str(item.get("token")).strip()
            exch_raw = item.get("exch", "NSE")
            exch_num = get_exchange_code_num(exch_raw)
            if token:
                SUBSCRIBED_TOKENS_REGISTRY[exch_num].add(token)

        subscribe_registered_tokens()
    except Exception as e:
        logger.error(f"❌ Error in subscribe_tokens: {e}")

@sio.event
async def disconnect(sid):
    logger.info(f"📱 Android Client Disconnected: {sid}")

# --- 🌐 REST HTTP API ENDPOINTS ---
async def fetch_chart_data(request: web.Request):
    try:
        d = await request.json()
        token = str(d.get('token', '')).strip()
        exch_raw = str(d.get('exch', 'NSE')).strip()

        if not token:
            return web.json_response({"status": False, "message": "Missing token"}, status=400)

        exchange_str = normalize_exchange(exch_raw)
        interval = str(d.get('interval', "FIVE_MINUTE")).strip()
        from_date = d.get('fromdate')
        to_date = d.get('todate')

        now = datetime.datetime.now(IST)
        if not to_date:
            to_date = now.strftime("%Y-%m-%d %H:%M")
        if not from_date:
            from_date = (now - datetime.timedelta(days=5)).strftime("%Y-%m-%d %H:%M")

        historic_param = {
            "exchange": exchange_str,
            "symboltoken": token,
            "interval": interval,
            "fromdate": from_date,
            "todate": to_date
        }

        if state.smart_api:
            try:
                response = state.smart_api.getCandleData(historic_param)
                if response and response.get("status") and response.get("data"):
                    formatted_candles = [
                        {"time": c[0], "open": float(c[1]), "high": float(c[2]), "low": float(c[3]), "close": float(c[4])}
                        for c in response["data"]
                    ]
                    return web.json_response({"status": True, "message": "SUCCESS", "data": formatted_candles})
            except Exception as api_err:
                logger.error(f"Historical API Error: {api_err}")

        return web.json_response({"status": False, "message": "Historical data unavailable", "data": []})
    except Exception as e:
        logger.error(f"Exception in fetch_chart_data: {e}")
        return web.json_response({"status": False, "message": str(e), "data": []}, status=500)

app.router.add_post('/api/get_chart_data', fetch_chart_data)

# --- 🔄 BACKGROUND TASKS ---
async def broker_auto_login_task():
    global BROKER_JWT_TOKEN, BROKER_FEED_TOKEN, LAST_BROKER_LOGIN_TIME
    while True:
        try:
            if BROKER_JWT_TOKEN is None or (time.time() - LAST_BROKER_LOGIN_TIME >= 36000):
                totp_crypto = pyotp.TOTP(TOTP_STR)
                smart_conn = SmartConnect(api_key=API_KEY)
                
                session_data = login_with_retry(smart_conn, CLIENT_CODE, MPIN, totp_crypto.now())
                
                if session_data and isinstance(session_data, dict) and session_data.get('status'):
                    BROKER_JWT_TOKEN = session_data['data']['jwtToken']
                    BROKER_FEED_TOKEN = session_data['data']['feedToken']
                    LAST_BROKER_LOGIN_TIME = time.time()
                    smart_conn.setAccessToken(BROKER_JWT_TOKEN)
                    state.smart_api = smart_conn
                    force_broker_socket_restart()
        except Exception as e:
            logger.error(f"Broker auto-login task error: {e}")
        await asyncio.sleep(600)

async def start_background_tasks(app):
    global main_loop
    main_loop = asyncio.get_event_loop()
    
    try:
        totp_crypto = pyotp.TOTP(TOTP_STR)
        smart_conn = SmartConnect(api_key=API_KEY)
        
        session_data = login_with_retry(smart_conn, CLIENT_CODE, MPIN, totp_crypto.now())
        
        if session_data and isinstance(session_data, dict) and session_data.get('status'):
            global BROKER_JWT_TOKEN, BROKER_FEED_TOKEN, LAST_BROKER_LOGIN_TIME
            BROKER_JWT_TOKEN = session_data['data']['jwtToken']
            BROKER_FEED_TOKEN = session_data['data']['feedToken']
            LAST_BROKER_LOGIN_TIME = time.time()
            smart_conn.setAccessToken(BROKER_JWT_TOKEN)
            state.smart_api = smart_conn
            threading.Thread(target=start_angel_one_websocket_worker, args=(BROKER_JWT_TOKEN, BROKER_FEED_TOKEN), daemon=True).start()
    except Exception as e:
        logger.error(f"Error starting background tasks: {e}")
    
    app['auto_login'] = asyncio.create_task(broker_auto_login_task())

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
