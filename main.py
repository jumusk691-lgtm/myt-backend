import asyncio
import json
import logging
import time
import datetime
import threading
import sqlite3
import pytz
import socketio
from aiohttp import web

# --- FYERS OFFICIAL SDK IMPORTS ---
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# --- 🕒 TIMEZONE & LOGGING SETUP ---
IST = pytz.timezone('Asia/Kolkata')

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("FYERS_TITAN_PROD")

# --- ⚙️ INTERNAL APP STATE ENGINE ---
class AppState:
    def __init__(self):
        self.fyers = None
        self.db_path = "angel_master.db"
        self.score = 0

state = AppState()

# --- 🔑 FYERS CREDENTIALS ---
CLIENT_ID = "BC7D6R1O7-100"       # App ID
SECRET_KEY = "6AEEEFZDT7"         # Secret ID
REDIRECT_URI = "https://myt-backend-1.onrender.com"

# --- 🚀 GLOBAL STATES & SCORE TRACKING ---
LTP_CACHE = {}               
SUBSCRIBED_TOKENS_REGISTRY = set()
BROKER_SOCKET_CONNECTED = False
USER_SCORE = 0 

TIMEFRAMES = {
    "1M": 60, "3M": 180, "5M": 300, "15M": 900,
    "25M": 1500, "30M": 1800, "1H": 3600, "1D": 86400
}

FYERS_RESOLUTION_MAP = {
    "ONE_MINUTE": "1", "1M": "1",
    "THREE_MINUTE": "3", "3M": "3",
    "FIVE_MINUTE": "5", "5M": "5",
    "TEN_MINUTE": "10", "10M": "10",
    "FIFTEEN_MINUTE": "15", "15M": "15",
    "THIRTY_MINUTE": "30", "30M": "30",
    "ONE_HOUR": "60", "1H": "60",
    "ONE_DAY": "D", "1D": "D"
}

MAX_CANDLES_LIMIT = 200
CANDLE_CACHE = {}  

FYERS_ACCESS_TOKEN = None

main_loop = None
fyers_ws = None

# Socket.IO & Aiohttp Setup
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# --- 🔄 SYMBOL FORMATTER FOR FYERS ---
def format_fyers_symbol(symbol_str, exch="NSE"):
    """
    Ensures symbol is in Fyers format e.g. NSE:SBIN-EQ, NFO:BANKNIFTY23AUGFUT, MCX:CRUDEOIL23SEPFUT
    """
    sym = str(symbol_str).strip()
    if ":" in sym:
        return sym.upper()
    
    ex_str = str(exch).upper().strip()
    if ex_str in ["5", "MCX", "MCX_FO", "MCXFO"]:
        return f"MCX:{sym}".upper()
    elif ex_str in ["2", "NFO", "NSE_FO"]:
        return f"NFO:{sym}".upper()
    elif ex_str in ["3", "BSE", "BSE_CM", "BFO"]:
        return f"BSE:{sym}".upper()
    
    # Default Equity / NSE
    if not sym.endswith("-EQ") and not sym.endswith("-INDEX"):
        sym = f"{sym}-EQ"
    return f"NSE:{sym}".upper()

# --- 🎯 SCORE LOGIC ENGINE ---
def update_user_score(points=1):
    global USER_SCORE
    USER_SCORE += points
    state.score = USER_SCORE
    logger.info(f"📊 Current User Score: {USER_SCORE}")
    return USER_SCORE

# --- 📈 REALTIME TICK CANDLE AGGREGATOR ---
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
            tf_list.append({"time": bucket_time, "open": price_val, "high": price_val, "low": price_val, "close": price_val})
        else:
            last_candle = tf_list[-1]
            if bucket_time > last_candle["time"]:
                tf_list.append({"time": bucket_time, "open": last_candle["close"], "high": max(last_candle["close"], price_val), "low": min(last_candle["close"], price_val), "close": price_val})
                if len(tf_list) > MAX_CANDLES_LIMIT:
                    tf_list.pop(0)
            else:
                last_candle["close"] = price_val
                last_candle["high"] = max(last_candle["high"], price_val)
                last_candle["low"] = min(last_candle["low"], price_val)

# ==============================================================================
# --- 🌐 REST HTTP API ENDPOINTS ---
# ==============================================================================

async def handle_fyers_login(request: web.Request):
    """
    Receives auth_code from frontend/client and generates FYERS_ACCESS_TOKEN
    """
    global FYERS_ACCESS_TOKEN, state
    try:
        d = await request.json()
        auth_code = d.get("auth_code")
        if not auth_code:
            return web.json_response({"status": False, "error": "auth_code is required"})

        session = fyersModel.SessionModel(
            client_id=CLIENT_ID,
            secret_key=SECRET_KEY,
            redirect_uri=REDIRECT_URI,
            response_type="code",
            grant_type="authorization_code"
        )
        session.set_token(auth_code)
        response = session.generate_token()

        if response and isinstance(response, dict) and response.get("s") == "ok":
            FYERS_ACCESS_TOKEN = response.get("access_token")
            state.fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=FYERS_ACCESS_TOKEN, log_path="")
            
            # Start WebSocket Thread upon successful login
            threading.Thread(target=start_fyers_websocket_worker, daemon=True).start()
            
            current_score = update_user_score(5)
            logger.info("✅ FYERS Login & Access Token Successful!")
            return web.json_response({"status": True, "message": "Logged in successfully", "score": current_score})
        else:
            return web.json_response({"status": False, "error": response})
    except Exception as e:
        logger.error(f"❌ Login Endpoint Error: {e}")
        return web.json_response({"status": False, "error": str(e)})

async def fetch_chart_data(request: web.Request):
    try:
        d = await request.json()
        token = str(d.get('token', d.get('symbol', '')))
        raw_exch = str(d.get('exch', 'NSE')).upper().strip()
        fyers_symbol = format_fyers_symbol(token, raw_exch)

        requested_interval = str(d.get('interval', "FIVE_MINUTE")).upper()
        resolution = FYERS_RESOLUTION_MAP.get(requested_interval, "5")

        now = datetime.datetime.now(IST)
        range_to = now.strftime('%Y-%m-%d')
        range_from = (now - datetime.timedelta(days=15)).strftime('%Y-%m-%d')

        logger.info(f"📥 Chart Req: Symbol={fyers_symbol}, Res={resolution}")

        if state.fyers:
            data_param = {
                "symbol": fyers_symbol,
                "resolution": resolution,
                "date_format": "1",
                "range_from": range_from,
                "range_to": range_to,
                "cont_flag": "1"
            }
            response = state.fyers.history(data=data_param)

            if response and isinstance(response, dict) and response.get("s") == "ok":
                current_score = update_user_score(1)
                raw_candles = response.get("candles", [])
                
                formatted_candles = [{
                    "time": item[0],
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": int(item[5]) if len(item) > 5 else 0
                } for item in raw_candles]

                return web.json_response({
                    "status": True,
                    "token": token,
                    "symbol": fyers_symbol,
                    "interval": requested_interval,
                    "score": current_score,
                    "data": formatted_candles
                })

        current_score = update_user_score(1)
        return web.json_response({
            "status": False,
            "token": token,
            "score": current_score,
            "error": "No data or FYERS not authenticated",
            "data": []
        })

    except Exception as e:
        logger.error(f"❌ Chart Error: {e}")
        return web.json_response({"status": False, "error": str(e)})

async def fetch_historical_oi_data(request: web.Request):
    try:
        d = await request.json()
        token = str(d.get('token', ''))
        raw_exch = str(d.get('exch', 'NFO')).upper().strip()
        fyers_symbol = format_fyers_symbol(token, raw_exch)

        requested_interval = str(d.get('interval', "THREE_MINUTE")).upper()
        resolution = FYERS_RESOLUTION_MAP.get(requested_interval, "3")

        now = datetime.datetime.now(IST)
        range_to = now.strftime('%Y-%m-%d')
        range_from = (now - datetime.timedelta(days=5)).strftime('%Y-%m-%d')

        if state.fyers:
            data_param = {
                "symbol": fyers_symbol,
                "resolution": resolution,
                "date_format": "1",
                "range_from": range_from,
                "range_to": range_to,
                "cont_flag": "1",
                "oi_flag": "1"
            }
            response = state.fyers.history(data=data_param)

            if response and isinstance(response, dict) and response.get("s") == "ok":
                current_score = update_user_score(1)
                return web.json_response({
                    "status": True,
                    "token": token,
                    "score": current_score,
                    "data": response.get("candles", [])
                })

        return web.json_response({"status": False, "token": token, "data": []})
    except Exception as e:
        logger.error(f"❌ OI Error: {e}")
        return web.json_response({"status": False, "error": str(e)})

async def get_expiry(request: web.Request):
    try:
        d = await request.json()
        name = d.get('name', '').upper()
        if not name or not state.db_path:
            return web.json_response({"expiries": [], "status": False})

        with sqlite3.connect(state.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT expiry FROM symbols WHERE name = ? AND expiry != '' ORDER BY expiry ASC", (name,))
            exps = [r[0] for r in cursor.fetchall()]
        
        return web.json_response({"status": True, "name": name, "expiries": exps})
    except Exception as e:
        return web.json_response({"expiries": [], "status": False})

app.router.add_post('/api/login', handle_fyers_login)
app.router.add_post('/api/get_chart_data', fetch_chart_data)
app.router.add_post('/api/get_oi_data', fetch_historical_oi_data)
app.router.add_post('/api/expiry_list', get_expiry)

# ==============================================================================
# --- 📡 REALTIME WEBSOCKET & SOCKET.IO ENGINE ---
# ==============================================================================

def on_message_received(ticks):
    global main_loop
    try:
        symbol = ticks.get("symbol", ticks.get("n", ""))
        raw_ltp = ticks.get("ltp", ticks.get("v", {}).get("lp", 0))

        try:
            price_val = float(raw_ltp)
            price_str = f"{price_val:.2f}"
        except Exception:
            price_val = 0.0
            price_str = str(raw_ltp)

        LTP_CACHE[symbol] = price_str

        if price_val > 0:
            update_token_candles(symbol, price_val)

        if main_loop:
            asyncio.run_coroutine_threadsafe(
                sio.emit("live_data", {"token": symbol, "ltp": price_str}, room=symbol), 
                main_loop
            )
    except Exception:
        pass

def on_ws_open():
    global BROKER_SOCKET_CONNECTED
    BROKER_SOCKET_CONNECTED = True
    logger.info("✅ FYERS Realtime WebSocket Connected!")
    if fyers_ws and SUBSCRIBED_TOKENS_REGISTRY:
        fyers_ws.subscribe(symbols=list(SUBSCRIBED_TOKENS_REGISTRY), data_type="symbolUpdate")

def on_ws_error(msg):
    logger.error(f"❌ FYERS WebSocket Error: {msg}")

def on_ws_close():
    global BROKER_SOCKET_CONNECTED
    BROKER_SOCKET_CONNECTED = False
    logger.warning("⚠️ FYERS WebSocket Closed")

@sio.event
async def subscribe_request(sid, data):
    global fyers_ws
    try:
        payload = json.loads(data) if isinstance(data, str) else data
        action = payload.get("action", "sub")
        raw_exch = payload.get("exchange", "NSE")
        tokens_list = payload.get("tokens", [])

        if action == "sub":
            update_user_score(1)
            formatted_symbols = []

            for token in tokens_list:
                fyers_symbol = format_fyers_symbol(str(token), raw_exch)
                formatted_symbols.append(fyers_symbol)
                
                await sio.enter_room(sid, fyers_symbol)
                SUBSCRIBED_TOKENS_REGISTRY.add(fyers_symbol)

                if fyers_symbol in LTP_CACHE:
                    await sio.emit("live_data", {"token": fyers_symbol, "ltp": LTP_CACHE[fyers_symbol]}, room=sid)

            if BROKER_SOCKET_CONNECTED and fyers_ws and formatted_symbols:
                fyers_ws.subscribe(symbols=formatted_symbols, data_type="symbolUpdate")

    except Exception as e:
        logger.error(f"Subscribe Error: {e}")

@sio.event
async def get_candles(sid, data):
    try:
        payload = json.loads(data) if isinstance(data, str) else data
        token_str = str(payload.get("token", ""))
        tf_key = str(payload.get("timeframe", "5M")).upper()

        update_user_score(1)
        candles_response = CANDLE_CACHE.get(token_str, {}).get(tf_key, [])

        await sio.emit("candles_response", {
            "token": token_str,
            "timeframe": tf_key,
            "candles": candles_response
        }, room=sid)
    except Exception as e:
        logger.error(f"Get Candles Error: {e}")

def start_fyers_websocket_worker():
    global fyers_ws
    if not FYERS_ACCESS_TOKEN:
        return
    try:
        full_token = f"{CLIENT_ID}:{FYERS_ACCESS_TOKEN}"
        fyers_ws = data_ws.FyersDataSocket(
            access_token=full_token,
            log_path="",
            l_type="symbolUpdate",
            on_connect=on_ws_open,
            on_close=on_ws_close,
            on_error=on_ws_error,
            on_message=on_message_received
        )
        fyers_ws.connect()
    except Exception as e:
        logger.error(f"WebSocket Worker Exception: {e}")

async def start_background_tasks(app_instance):
    global main_loop
    main_loop = asyncio.get_event_loop()

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
