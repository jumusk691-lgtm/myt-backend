import asyncio
import json
import logging
import time
import datetime
import threading
import sqlite3
import gc
import jwt
import socketio
import pyotp
from aiohttp import web
from requests.exceptions import ReadTimeout

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from brain import state, logger, IST

# --- 📝 LOGGING (MINIMAL TO SAVE CPU CYCLES) ---
logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")

# --- 🔑 CREDENTIALS ---
API_KEY = "Z80wG5Sg"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"

# --- 🚀 GLOBAL STATES & SCORE TRACKING ---
LTP_CACHE = {}               
SUBSCRIBED_TOKENS_REGISTRY = {1: set(), 2: set(), 3: set(), 4: set(), 5: set()}
BROKER_SOCKET_CONNECTED = False
USER_SCORE = 0  # Bhai ka score tracking variable

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

# Socket.IO & Aiohttp Setup
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# --- 🎯 SCORE LOGIC ---
def update_user_score(points=1):
    global USER_SCORE
    USER_SCORE += points
    if hasattr(state, 'score'):
        state.score = USER_SCORE
    logger.info(f"📊 Current User Score: {USER_SCORE}")
    return USER_SCORE

# --- 🌱 AUTO-SEED ENGINE (PREVENTS BLANK CHART ON SERVER RESTART) ---
def seed_candles_if_empty(token_str, base_price):
    """
    Server restart hone par agar cache khali ho toh immediate 200 candles seed kar deta hai.
    """
    if token_str not in CANDLE_CACHE or not CANDLE_CACHE[token_str].get("5M"):
        CANDLE_CACHE[token_str] = {}
        now_sec = int(time.time())
        
        for tf_key, interval_sec in TIMEFRAMES.items():
            tf_candles = []
            price_tracker = base_price
            current_bucket = (now_sec // interval_sec) * interval_sec
            start_bucket = current_bucket - ((MAX_CANDLES_LIMIT - 1) * interval_sec)

            for i in range(MAX_CANDLES_LIMIT):
                c_time = start_bucket + (i * interval_sec)
                # Small variance to generate natural chart view
                delta = ((i % 5) - 2) * (base_price * 0.0005)
                c_close = round(price_tracker + delta, 2)
                c_open = round(price_tracker, 2)
                c_high = round(max(c_open, c_close) + (base_price * 0.0002), 2)
                c_low = round(min(c_open, c_close) - (base_price * 0.0002), 2)

                tf_candles.append({
                    "time": c_time,
                    "open": c_open,
                    "high": c_high,
                    "low": c_low,
                    "close": c_close
                })
                price_tracker = c_close

            CANDLE_CACHE[token_str][tf_key] = tf_candles

# --- 📈 MULTI-TIMEFRAME CANDLE AGGREGATOR ---
def update_token_candles(token_str, price_val):
    """
    Ticks se real-time 1M, 3M, 5M, 15M, 25M, 30M, 1H, 1D timeframes ki exact 200 candles build karta hai.
    """
    if price_val <= 0:
        return

    # Cache Empty Guard
    seed_candles_if_empty(token_str, price_val)

    now_sec = int(time.time())
    token_candles = CANDLE_CACHE[token_str]

    for tf_key, interval_sec in TIMEFRAMES.items():
        tf_list = token_candles[tf_key]
        bucket_time = (now_sec // interval_sec) * interval_sec

        if not tf_list:
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
                new_candle = {
                    "time": bucket_time,
                    "open": last_candle["close"],
                    "high": max(last_candle["close"], price_val),
                    "low": min(last_candle["close"], price_val),
                    "close": price_val
                }
                tf_list.append(new_candle)
                if len(tf_list) > MAX_CANDLES_LIMIT:
                    tf_list.pop(0)
            else:
                last_candle["close"] = price_val
                if price_val > last_candle["high"]:
                    last_candle["high"] = price_val
                if price_val < last_candle["low"]:
                    last_candle["low"] = price_val

# ==============================================================================
# --- 🌐 REST HTTP API ENDPOINTS (AIOHTTP COMPATIBLE) ---
# ==============================================================================

async def fetch_chart_data(request: web.Request):
    """
    Fetches historical candles for APK Charts.
    Zero-RAM Optimized with Manual GC.
    """
    try:
        d = await request.json()
        token = str(d.get('token'))
        exch = d.get('exch', 'NSE')
        
        # Interval Validation
        requested_interval = d.get('interval', "FIVE_MINUTE")
        valid_intervals = {
            "ONE_MINUTE": "ONE_MINUTE",
            "THREE_MINUTE": "THREE_MINUTE",
            "FIVE_MINUTE": "FIVE_MINUTE",
            "FIFTEEN_MINUTE": "FIFTEEN_MINUTE",
            "THIRTY_MINUTE": "THIRTY_MINUTE",
            "ONE_HOUR": "ONE_HOUR",
            "ONE_DAY": "ONE_DAY"
        }
        interval = valid_intervals.get(requested_interval, "FIVE_MINUTE")
        
        # Session check
        if not state.smart_api:
            logger.error("❌ [History] SmartApi Session is NULL!")
            return web.json_response({"status": False, "message": "API Session Expired"})

        # Time Calculation: Strictly Last 10 Days
        now = datetime.datetime.now(IST)
        to_date = now.strftime('%Y-%m-%d %H:%M')
        from_date = (now - datetime.timedelta(days=10)).strftime('%Y-%m-%d %H:%M')

        # AngelOne API Payload
        params = {
            "exchange": exch,
            "symboltoken": token,
            "interval": interval,
            "fromdate": from_date,
            "todate": to_date
        }
        
        logger.info(f"📊 [History] Fetching {interval} for Token: {token}")
        
        # API Call
        historic_data = state.smart_api.getCandleData(params)
        
        if historic_data and historic_data.get('status'):
            # --- SCORE LOGIC ---
            current_score = update_user_score(1)
            
            result = {
                "status": True,
                "token": token,
                "interval": interval,
                "score": current_score,
                "data": historic_data.get('data', [])
            }
            
            # --- MEMORY CLEANUP ---
            del historic_data
            gc.collect() 
            
            return web.json_response(result)
        else:
            msg = historic_data.get('message', 'No data from API') if historic_data else 'No data from API'
            return web.json_response({"status": False, "message": msg})

    except Exception as e:
        logger.error(f"❌ [History Error]: {e}")
        return web.json_response({"status": False, "error": str(e)})

async def get_expiry(request: web.Request):
    """Returns list of expiry dates from SQLite master"""
    try:
        d = await request.json()
        name = d.get('name', '').upper()
        if not name:
            return web.json_response({"expiries": [], "status": False})

        if not state.db_path:
            return web.json_response({"expiries": [], "status": False, "msg": "DB not ready"})

        # Context manager for auto-closing connection
        with sqlite3.connect(state.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT expiry 
                FROM symbols 
                WHERE name = ? AND expiry != '' 
                ORDER BY expiry ASC
            """, (name,))
            exps = [r[0] for r in cursor.fetchall()]
        
        return web.json_response({
            "status": True,
            "name": name,
            "expiries": exps
        })
    except Exception as e:
        logger.error(f"❌ [Expiry API Error]: {e}")
        return web.json_response({"expiries": [], "status": False})

# Register HTTP REST Routes
app.router.add_post('/api/get_chart_data', fetch_chart_data)
app.router.add_post('/api/expiry_list', get_expiry)

# --- 🛡️ ANGEL ONE LOGIN RETRY LOGIC ---
def login_with_retry(smart_conn, client_code, mpin, totp_val):
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
    global main_loop
    try:
        tick_data = json.loads(message) if isinstance(message, str) else message
        token_str = str(tick_data.get("token", tick_data.get("t", "")))
        raw_ltp = tick_data.get("last_traded_price", tick_data.get("ltp", 0))

        try:
            val = float(raw_ltp)
            price_val = val / 100 if val > 100000 else val
            price_str = f"{price_val:.2f}"
        except:
            price_val = 0.0
            price_str = str(raw_ltp)

        LTP_CACHE[token_str] = price_str

        if price_val > 0:
            update_token_candles(token_str, price_val)

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
            update_user_score(1) 
            
            for token in tokens_list:
                str_token = str(token)
                await sio.enter_room(sid, str_token)
                if str_token in LTP_CACHE:
                    await sio.emit("live_data", {"token": str_token, "ltp": LTP_CACHE[str_token]}, room=sid)
                
                if exchange_code in SUBSCRIBED_TOKENS_REGISTRY and str_token not in SUBSCRIBED_TOKENS_REGISTRY[exchange_code]:
                    SUBSCRIBED_TOKENS_REGISTRY[exchange_code].add(str_token)

            if BROKER_SOCKET_CONNECTED and sws_client:
                sws_client.subscribe("munh_titan_live", 1, [{"exchangeType": exchange_code, "tokens": tokens_list}])
    except: pass

@sio.event
async def get_candles(sid, data):
    try:
        payload = json.loads(data) if isinstance(data, str) else data
        token_str = str(payload.get("token", ""))
        tf_key = str(payload.get("timeframe", "5M")).upper()

        update_user_score(1)

        # Fallback Seed Check
        if token_str in LTP_CACHE:
            try:
                base_p = float(LTP_CACHE[token_str])
                seed_candles_if_empty(token_str, base_p)
            except: pass

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
            if BROKER_JWT_TOKEN is None or (time.time() - LAST_BROKER_LOGIN_TIME >= 36000):
                totp_crypto = pyotp.TOTP(TOTP_STR)
                smart_conn = SmartConnect(api_key=API_KEY)
                
                session_data = login_with_retry(smart_conn, CLIENT_CODE, MPIN, totp_crypto.now())
                
                if session_data and session_data.get('status'):
                    BROKER_JWT_TOKEN = session_data['data']['jwtToken']
                    BROKER_FEED_TOKEN = session_data['data']['feedToken']
                    LAST_BROKER_LOGIN_TIME = time.time()
                    state.smart_api = smart_conn  # Keep state sync
                    force_broker_socket_restart()
        except: pass
        await asyncio.sleep(600)

def on_websocket_open(wsapp):
    global BROKER_SOCKET_CONNECTED
    BROKER_SOCKET_CONNECTED = True
    for exch_code, tokens_set in SUBSCRIBED_TOKENS_REGISTRY.items():
        if tokens_set:
            sws_client.subscribe("munh_titan_live", 1, [{"exchangeType": exch_code, "tokens": list(tokens_set)}])

def on_websocket_close(wsapp, code, msg):
    global BROKER_SOCKET_CONNECTED
    BROKER_SOCKET_CONNECTED = False
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
    
    try:
        totp_crypto = pyotp.TOTP(TOTP_STR)
        smart_conn = SmartConnect(api_key=API_KEY)
        
        session_data = login_with_retry(smart_conn, CLIENT_CODE, MPIN, totp_crypto.now())
        
        if session_data and session_data.get('status'):
            global BROKER_JWT_TOKEN, BROKER_FEED_TOKEN, LAST_BROKER_LOGIN_TIME
            BROKER_JWT_TOKEN = session_data['data']['jwtToken']
            BROKER_FEED_TOKEN = session_data['data']['feedToken']
            LAST_BROKER_LOGIN_TIME = time.time()
            state.smart_api = smart_conn
            threading.Thread(target=start_angel_one_websocket_worker, args=(BROKER_JWT_TOKEN, BROKER_FEED_TOKEN), daemon=True).start()
    except: pass
    
    app['auto_login'] = asyncio.create_task(broker_auto_login_task())

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
