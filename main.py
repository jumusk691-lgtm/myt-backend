import asyncio
import json
import logging
import time
import datetime
import threading
import jwt
import socketio
import pyotp
from aiohttp import web
from requests.exceptions import ReadTimeout

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- 📝 LOGGING (MINIMAL TO SAVE CPU CYCLES) ---
logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MUNH_TITAN_REALTIME_PROD")

# --- 🔑 CREDENTIALS ---
API_KEY = "Z80wG5Sg"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"

# --- 🚀 GLOBAL STATES ---
LTP_CACHE = {}               
SUBSCRIBED_TOKENS_REGISTRY = {1: set(), 2: set(), 3: set(), 4: set(), 5: set()}
BROKER_SOCKET_CONNECTED = False
USER_SCORE = 0  # भाई का स्कोर ट्रैकिंग वेरिएबल

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

# JWT Management
JWT_SECRET = "MUNH_TITAN_SUPER_SECRET_KEY_2026"
BROKER_JWT_TOKEN = None
BROKER_FEED_TOKEN = None
LAST_BROKER_LOGIN_TIME = 0

main_loop = None
sws_client = None

# Socket.IO Setup
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# --- 🎯 SCORE LOGIC ---
def update_user_score(points):
    global USER_SCORE
    USER_SCORE += points
    logger.info(f"📊 Current User Score: {USER_SCORE}")
    return USER_SCORE

# --- 📈 MULTI-TIMEFRAME CANDLE AGGREGATOR ---
def update_token_candles(token_str, price_val):
    """
    Ticks se real-time 1M, 3M, 5M, 15M, 25M, 30M, 1H, 1D timeframes ki exact 200 candles build karta hai.
    """
    if price_val <= 0:
        return

    now_sec = int(time.time())

    if token_str not in CANDLE_CACHE:
        CANDLE_CACHE[token_str] = {tf: [] for tf in TIMEFRAMES}

    token_candles = CANDLE_CACHE[token_str]

    for tf_key, interval_sec in TIMEFRAMES.items():
        tf_list = token_candles[tf_key]
        bucket_time = (now_sec // interval_sec) * interval_sec

        if not tf_list:
            # First candle initialization
            new_candle = {
                "time": bucket_time,
                "open": price_val,
                "high": price_val,
                "low": price_val,
                "close": price_val
            }
            tf_list.append(new_candle)
        else:
            last_candle = tf_list[-1]
            if bucket_time > last_candle["time"]:
                # New Candle boundary reached
                new_candle = {
                    "time": bucket_time,
                    "open": last_candle["close"],
                    "high": max(last_candle["close"], price_val),
                    "low": min(last_candle["close"], price_val),
                    "close": price_val
                }
                tf_list.append(new_candle)
                # Keep maximum 200 candles only
                if len(tf_list) > MAX_CANDLES_LIMIT:
                    tf_list.pop(0)
            else:
                # Update existing candle in real-time
                last_candle["close"] = price_val
                if price_val > last_candle["high"]:
                    last_candle["high"] = price_val
                if price_val < last_candle["low"]:
                    last_candle["low"] = price_val

# --- 🛡️ ANGEL ONE LOGIN RETRY LOGIC ---
def login_with_retry(smart_conn, client_code, mpin, totp_val):
    """Angel One API में 7 सेकंड टाइमआउट को हैंडल करने के लिए रिट्राई लॉजिक"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            session_data = smart_conn.generateSession(client_code, mpin, totp_val)
            if session_data and session_data.get('status'):
                return session_data
        except ReadTimeout:
            logger.warning(f"Timeout on login attempt {attempt + 1}. Retrying in 2 seconds...")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Login error on attempt {attempt + 1}: {e}")
            time.sleep(2)
    return None

# --- 📡 CORE REALTIME ENGINE ---
def on_data_received(wsapp, message):
    """
    ULTRA-LOW LATENCY: डेटा आते ही P&L/LTP Emit करें और 200 Candles Update करें।
    """
    global main_loop
    try:
        tick_data = json.loads(message) if isinstance(message, str) else message
        token_str = str(tick_data.get("token", tick_data.get("t", "")))
        raw_ltp = tick_data.get("last_traded_price", tick_data.get("ltp", 0))

        # प्राइस डिवाइडर (एंजेल वन फॉर्मेट के लिए)
        try:
            val = float(raw_ltp)
            price_val = val / 100 if val > 100000 else val
            price_str = f"{price_val:.2f}"
        except:
            price_val = 0.0
            price_str = str(raw_ltp)

        # Cache update
        LTP_CACHE[token_str] = price_str

        # Update 200 candles engine across 1M, 3M, 5M, 15M, 25M, 30M, 1H, 1D
        if price_val > 0:
            update_token_candles(token_str, price_val)

        # 🚀 ZERO DELAY EMIT: सीधा रूम में भेजो
        if main_loop:
            asyncio.run_coroutine_threadsafe(
                sio.emit("live_data", {"token": token_str, "ltp": price_str}, room=token_str), 
                main_loop
            )
    except: pass

@sio.event
async def subscribe_request(sid, data):
    global sws_client
    try:
        payload = json.loads(data) if isinstance(data, str) else data
        action = payload.get("action", "")
        exchange_code = payload.get("exchange")
        tokens_list = payload.get("tokens", [])

        if action == "sub":
            # स्कोर अपडेट
            update_user_score(1) 
            
            for token in tokens_list:
                str_token = str(token)
                await sio.enter_room(sid, str_token)
                if str_token in LTP_CACHE:
                    await sio.emit("live_data", {"token": str_token, "ltp": LTP_CACHE[str_token]}, room=sid)
                
                if exchange_code in SUBSCRIBED_TOKENS_REGISTRY and str_token not in SUBSCRIBED_TOKENS_REGISTRY[exchange_code]:
                    SUBSCRIBED_TOKENS_REGISTRY[exchange_code].add(str_token)

            # Bulk Subscribe to broker
            if BROKER_SOCKET_CONNECTED and sws_client:
                sws_client.subscribe("munh_titan_live", 1, [{"exchangeType": exchange_code, "tokens": tokens_list}])
    except: pass

@sio.event
async def get_candles(sid, data):
    """
    Android App se request aane par 200 real calculated candles emit karta hai.
    """
    try:
        payload = json.loads(data) if isinstance(data, str) else data
        token_str = str(payload.get("token", ""))
        tf_key = str(payload.get("timeframe", "5M")).upper()

        update_user_score(1)

        candles_response = []
        if token_str in CANDLE_CACHE and tf_key in CANDLE_CACHE[token_str]:
            candles_response = CANDLE_CACHE[token_str][tf_key]

        await sio.emit("candles_response", {
            "token": token_str,
            "timeframe": tf_key,
            "candles": candles_response
        }, room=sid)
    except Exception as e:
        logger.error(f"Error fetching candles: {e}")

# --- 🛠️ SESSION & CONNECTION HANDLING ---
def force_broker_socket_restart():
    global sws_client, BROKER_SOCKET_CONNECTED
    if sws_client and BROKER_SOCKET_CONNECTED:
        try: sws_client.close()
        except: pass

async def broker_auto_login_task():
    global BROKER_JWT_TOKEN, BROKER_FEED_TOKEN, LAST_BROKER_LOGIN_TIME
    while True:
        try:
            # 10 घंटे बाद रिलॉगिन
            if BROKER_JWT_TOKEN is None or (time.time() - LAST_BROKER_LOGIN_TIME >= 36000):
                totp_crypto = pyotp.TOTP(TOTP_STR)
                smart_conn = SmartConnect(api_key=API_KEY)
                
                # नया रिट्राई लॉजिक इस्तेमाल किया गया
                session_data = login_with_retry(smart_conn, CLIENT_CODE, MPIN, totp_crypto.now())
                
                if session_data and session_data.get('status'):
                    BROKER_JWT_TOKEN = session_data['data']['jwtToken']
                    BROKER_FEED_TOKEN = session_data['data']['feedToken']
                    LAST_BROKER_LOGIN_TIME = time.time()
                    force_broker_socket_restart()
        except: pass
        await asyncio.sleep(600)

def on_websocket_open(wsapp):
    global BROKER_SOCKET_CONNECTED
    BROKER_SOCKET_CONNECTED = True
    # Re-subscribe tokens on connection
    for exch_code, tokens_set in SUBSCRIBED_TOKENS_REGISTRY.items():
        if tokens_set:
            sws_client.subscribe("munh_titan_live", 1, [{"exchangeType": exch_code, "tokens": list(tokens_set)}])

def on_websocket_close(wsapp, code, msg):
    global BROKER_SOCKET_CONNECTED
    BROKER_SOCKET_CONNECTED = False
    # Reconnection mechanism
    threading.Thread(target=lambda: (time.sleep(2), start_angel_one_websocket_worker(BROKER_JWT_TOKEN, BROKER_FEED_TOKEN)), daemon=True).start()

def start_angel_one_websocket_worker(auth_token, feed_token):
    global sws_client
    if not auth_token or not feed_token: return
    sws_client = SmartWebSocketV2(auth_token=auth_token, client_code=CLIENT_CODE, api_key=API_KEY, feed_token=feed_token)
    sws_client.on_data = on_data_received
    sws_client.on_open = on_websocket_open
    sws_client.on_close = on_websocket_close
    sws_client.connect()

async def start_background_tasks(app):
    global main_loop
    main_loop = asyncio.get_event_loop()
    
    # इनिशियल लॉगिन
    try:
        totp_crypto = pyotp.TOTP(TOTP_STR)
        smart_conn = SmartConnect(api_key=API_KEY)
        
        # नया रिट्राई लॉजिक इस्तेमाल किया गया
        session_data = login_with_retry(smart_conn, CLIENT_CODE, MPIN, totp_crypto.now())
        
        if session_data and session_data.get('status'):
            global BROKER_JWT_TOKEN, BROKER_FEED_TOKEN, LAST_BROKER_LOGIN_TIME
            BROKER_JWT_TOKEN = session_data['data']['jwtToken']
            BROKER_FEED_TOKEN = session_data['data']['feedToken']
            LAST_BROKER_LOGIN_TIME = time.time()
            threading.Thread(target=start_angel_one_websocket_worker, args=(BROKER_JWT_TOKEN, BROKER_FEED_TOKEN), daemon=True).start()
    except: pass
    
    app['auto_login'] = asyncio.create_task(broker_auto_login_task())

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
