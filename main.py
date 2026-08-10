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
            
            # Format: [["2026-08-10T09:15:00+05:30", open, high, low, close, volume], ...]
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

# App startup logic
if __name__ == '__main__':
    ensure_smart_api_session()
    web.run_app(app, host='0.0.0.0', port=5000)
