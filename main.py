import asyncio
import json
import logging
import time
import datetime
import threading
import sqlite3
import pytz
import os
import hashlib
import requests
import pyotp
from urllib import parse
import aiohttp
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

# --- 🔑 FYERS CREDENTIALS & AUTOMATED PARAMS ---
CLIENT_ID = "BC7D6RF107-100"       # App ID
SECRET_KEY = "6AEEEFZDT7"         # Secret ID
REDIRECT_URI = "https://myt-backend-1.onrender.com"
TOKEN_CACHE_FILE = "token_cache.json"

# Automated Credentials
FY_ID = "FAI41352"
PIN = "9853"
TOTP_SECRET = "L6UYLWJQSYLVVZYP3ODHKQITDYZXFUQC"
APP_TYPE = "100"

# --- 🚀 GLOBAL STATES & CACHE ---
LTP_CACHE = {}               
SUBSCRIBED_TOKENS_REGISTRY = set()
BROKER_SOCKET_CONNECTED = False
USER_SCORE = 0 
LOT_SIZE_CACHE = {}

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

# --- 🎯 GET LOT SIZE FROM DATABASE ---
def fetch_lot_size(symbol_str):
    if symbol_str in LOT_SIZE_CACHE:
        return LOT_SIZE_CACHE[symbol_str]
    
    clean_sym = symbol_str.split(":")[-1] if ":" in symbol_str else symbol_str
    lot_size = 1

    if os.path.exists(state.db_path):
        try:
            with sqlite3.connect(state.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT lotsize FROM symbols WHERE symbol = ? OR name = ? LIMIT 1", (clean_sym, clean_sym))
                row = cursor.fetchone()
                if row and row[0]:
                    lot_size = int(row[0])
        except Exception:
            pass

    LOT_SIZE_CACHE[symbol_str] = lot_size
    return lot_size

# --- 🔄 FIXED FYERS SYMBOL FORMATTER ---
def format_fyers_symbol(symbol_str, exch="NSE"):
    if not symbol_str:
        return ""
    
    sym = str(symbol_str).strip().upper()
    clean_sym = sym.split(":")[-1]

    if any(opt in clean_sym for opt in ["CE", "PE", "FUT"]):
        return f"NFO:{clean_sym}"
    
    raw_exch = str(exch).strip().upper()
    if raw_exch in ["MCX", "MCX_FO", "MCXFO"] or any(comm in clean_sym for comm in ["GOLD", "SILVER", "CRUDE"]):
        return f"MCX:{clean_sym}"

    if clean_sym in ["NIFTY50-INDEX", "NIFTY 50", "NIFTY", "NSE:NIFTY50-INDEX"]:
        return "NSE:NIFTY50-INDEX"
    if clean_sym in ["SENSEX-INDEX", "SENSEX", "BSE:SENSEX-INDEX"]:
        return "BSE:SENSEX-INDEX"
    if clean_sym in ["NIFTY BANK", "BANKNIFTY", "NIFTYBANK-INDEX"]:
        return "NSE:NIFTYBANK-INDEX"
    if "INDEX" in clean_sym:
        return f"NSE:{clean_sym}"

    if raw_exch in ["BSE", "BSE_CM", "BFO"]:
        return f"BSE:{clean_sym}"

    if not clean_sym.endswith("-EQ"):
        clean_sym = f"{clean_sym}-EQ"
        
    return f"NSE:{clean_sym}"

def update_user_score(points=1):
    global USER_SCORE
    USER_SCORE += points
    state.score = USER_SCORE
    return USER_SCORE

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

def save_token_to_file(token):
    try:
        with open(TOKEN_CACHE_FILE, "w") as f:
            json.dump({"access_token": token, "updated_at": str(datetime.datetime.now(IST))}, f)
    except Exception as e:
        logger.error(f"Failed to save token: {e}")

def get_app_id_hash():
    input_str = f"{CLIENT_ID}:{SECRET_KEY}"
    return hashlib.sha256(input_str.encode()).hexdigest()

# --- 🤖 AUTOMATED HEADLESS LOGIN FUNCTION ---
def perform_automated_login():
    global FYERS_ACCESS_TOKEN, state
    try:
        logger.info("🤖 Starting Fully Automated Headless Fyers Login...")
        base_url = "https://api-t2.fyers.in/vagator/v2"
        base_url_2 = "https://api-t1.fyers.in/api/v3"

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

        # 1. Send OTP
        res1 = requests.post(f"{base_url}/send_login_otp", json={"fy_id": FY_ID, "app_id": "2"}, headers=headers)
        if res1.status_code != 200:
            logger.error(f"❌ OTP Send Failed: {res1.text}")
            return False
        req_key1 = res1.json().get("request_key")

        # 2. Generate & Verify TOTP
        totp_val = pyotp.TOTP(TOTP_SECRET).now()
        res2 = requests.post(f"{base_url}/verify_otp", json={"request_key": req_key1, "otp": totp_val}, headers=headers)
        if res2.status_code != 200:
            logger.error(f"❌ TOTP Verify Failed: {res2.text}")
            return False
        req_key2 = res2.json().get("request_key")

        # 3. Verify PIN
        res3 = requests.post(f"{base_url}/verify_pin", json={"request_key": req_key2, "identity_type": "pin", "identifier": PIN}, headers=headers)
        if res3.status_code != 200:
            logger.error(f"❌ PIN Verify Failed: {res3.text}")
            return False
        jwt_token = res3.json().get("data", {}).get("access_token")

        # 4. Get Auth Code via v3 token endpoint (Fixed app_id to full CLIENT_ID)
        payload_token = {
            "fyers_id": FY_ID,
            "app_id": CLIENT_ID,
            "client_id": CLIENT_ID,
            "redirect_uri": REDIRECT_URI,
            "appType": APP_TYPE,
            "code_challenge": "",
            "state": "sample_state",
            "scope": "",
            "nonce": "",
            "response_type": "code",
            "create_cookie": True
        }
        token_headers = {'Authorization': f'Bearer {jwt_token}', **headers}
        res4 = requests.post(f"{base_url_2}/token", json=payload_token, headers=token_headers)
        if res4.status_code != 308 and res4.status_code != 200:
            logger.error(f"❌ Token Step Failed: {res4.text}")
            return False
        
        url_target = res4.json().get("Url", "")
        auth_code = parse.parse_qs(parse.urlparse(url_target).query).get('auth_code', [None])[0]
        if not auth_code:
            logger.error("❌ Auth code extraction failed!")
            return False

        # 5. Validate Auth Code to get Final Access Token
        payload_validate = {
            "grant_type": "authorization_code",
            "appIdHash": get_app_id_hash(),
            "code": auth_code,
        }
        res5 = requests.post(f"{base_url_2}/validate-authcode", json=payload_validate, headers=headers)
        if res5.status_code != 200:
            logger.error(f"❌ Validate AuthCode Failed: {res5.text}")
            return False

        final_token = res5.json().get("access_token")
        if final_token:
            FYERS_ACCESS_TOKEN = final_token
            save_token_to_file(FYERS_ACCESS_TOKEN)
            state.fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=FYERS_ACCESS_TOKEN, log_path="")
            logger.info("🎉 Fully Automated Fyers Login & Token Generation Successful!")
            threading.Thread(target=start_fyers_websocket_worker, daemon=True).start()
            return True

        return False
    except Exception as e:
        logger.error(f"❌ Automated Login Exception: {e}")
        return False

def load_cached_token():
    global FYERS_ACCESS_TOKEN, state
    if os.path.exists(TOKEN_CACHE_FILE):
        try:
            with open(TOKEN_CACHE_FILE, "r") as f:
                data = json.load(f)
                token = data.get("access_token")
                if token:
                    FYERS_ACCESS_TOKEN = token
                    state.fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=FYERS_ACCESS_TOKEN, log_path="")
                    logger.info("🔑 Loaded cached FYERS Access Token successfully!")
                    threading.Thread(target=start_fyers_websocket_worker, daemon=True).start()
                    return
        except Exception as e:
            logger.error(f"Error loading cached token: {e}")
    
    perform_automated_login()

# ==============================================================================
# --- 🌐 REST HTTP API ENDPOINTS ---
# ==============================================================================

async def handle_ping_stream(request):
    response = web.StreamResponse(
        status=200,
        reason='OK',
        headers={
            'Content-Type': 'audio/mpeg',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive'
        }
    )
    await response.prepare(request)
    silent_mp3_frame = b'\xff\xe3\x18\xc4\x00\x00\x00\x03\x48\x00\x00\x00\x00' + b'\x00' * 100
    try:
        while True:
            await response.write(silent_mp3_frame)
            await asyncio.sleep(10)
    except Exception:
        pass
    return response

def keep_alive_self_ping():
    while True:
        try:
            time.sleep(120)
            import urllib.request
            urllib.request.urlopen("https://myt-backend-1.onrender.com/ping").read()
            logger.info("🔊 Anti-Sleep Heartbeat Ping Executed")
        except Exception:
            pass

async def handle_ping(request):
    return web.json_response({
        "status": "active", 
        "fyers_ws_connected": BROKER_SOCKET_CONNECTED,
        "timestamp": str(datetime.datetime.now(IST))
    })

async def handle_debug_status(request):
    return web.json_response({
        "status": True,
        "fyers_authenticated": state.fyers is not None,
        "fyers_ws_connected": BROKER_SOCKET_CONNECTED,
        "subscribed_tokens_count": len(SUBSCRIBED_TOKENS_REGISTRY),
        "subscribed_tokens": list(SUBSCRIBED_TOKENS_REGISTRY),
        "ltp_cache": LTP_CACHE,
        "timestamp": str(datetime.datetime.now(IST))
    })

# --- ROUTE REGISTRATIONS ---
app.router.add_get('/ping', handle_ping)
app.router.add_get('/api/debug_status', handle_debug_status)
app.router.add_get('/silent_stream', handle_ping_stream)

# ==============================================================================
# --- 📡 REALTIME WEBSOCKET & SOCKET.IO ENGINE ---
# ==============================================================================

@sio.event
async def connect(sid, environ):
    logger.info(f"📱 Android Client Connected via Socket.IO: {sid}")
    status_msg = "connected" if BROKER_SOCKET_CONNECTED else "disconnected"
    await sio.emit("market_status", {"status": status_msg, "fyers_ready": BROKER_SOCKET_CONNECTED}, room=sid)

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

        lot_size = fetch_lot_size(symbol)

        if main_loop:
            payload = {
                "token": symbol, 
                "symbol": symbol,
                "ltp": price_str, 
                "price": price_str,
                "lot_size": lot_size
            }
            asyncio.run_coroutine_threadsafe(sio.emit("live_data", payload), main_loop)
            asyncio.run_coroutine_threadsafe(sio.emit("live_data", payload, room=symbol), main_loop)

    except Exception as e:
        logger.error(f"Tick Broadcast Error: {e}")

def on_ws_open():
    global BROKER_SOCKET_CONNECTED, main_loop
    BROKER_SOCKET_CONNECTED = True
    logger.info("✅ FYERS Realtime WebSocket Connected successfully!")
    
    if main_loop:
        asyncio.run_coroutine_threadsafe(
            sio.emit("market_status", {"status": "connected", "fyers_ready": True}), 
            main_loop
        )

    if fyers_ws and SUBSCRIBED_TOKENS_REGISTRY:
        fyers_ws.subscribe(symbols=list(SUBSCRIBED_TOKENS_REGISTRY), data_type="symbolUpdate")

def on_ws_error(msg):
    logger.error(f"❌ FYERS WebSocket Error: {msg}")

def on_ws_close():
    global BROKER_SOCKET_CONNECTED, main_loop
    BROKER_SOCKET_CONNECTED = False
    logger.warning("⚠️ FYERS WebSocket Closed!")
    
    if main_loop:
        asyncio.run_coroutine_threadsafe(
            sio.emit("market_status", {"status": "disconnected", "fyers_ready": False}), 
            main_loop
        )

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
                if str(token) != fyers_symbol:
                    await sio.enter_room(sid, str(token))
                    
                SUBSCRIBED_TOKENS_REGISTRY.add(fyers_symbol)

                if fyers_symbol in LTP_CACHE:
                    cached_price = LTP_CACHE[fyers_symbol]
                    await sio.emit("live_data", {
                        "token": token,
                        "symbol": fyers_symbol,
                        "ltp": cached_price,
                        "price": cached_price,
                        "lot_size": fetch_lot_size(fyers_symbol)
                    }, room=sid)

            if state.fyers and formatted_symbols:
                try:
                    sym_query = ",".join(formatted_symbols[:50])
                    quote_data = state.fyers.quotes({"symbols": sym_query})
                    
                    if quote_data and quote_data.get("s") == "ok":
                        for item in quote_data.get("d", []):
                            q_sym = item.get("n", "")
                            q_v = item.get("v", {})
                            q_ltp = str(q_v.get("lp", "0.00"))
                            
                            if q_sym and q_ltp != "0.00":
                                price_str = f"{float(q_ltp):.2f}"
                                LTP_CACHE[q_sym] = price_str
                                
                                await sio.emit("live_data", {
                                    "token": q_sym,
                                    "symbol": q_sym,
                                    "ltp": price_str,
                                    "price": price_str,
                                    "lot_size": fetch_lot_size(q_sym)
                                }, room=sid)
                except Exception as q_err:
                    logger.error(f"❌ Quotes API Fetch Error: {q_err}")

            if BROKER_SOCKET_CONNECTED and fyers_ws and formatted_symbols:
                fyers_ws.subscribe(symbols=formatted_symbols, data_type="symbolUpdate")

    except Exception as e:
        logger.error(f"❌ Subscribe Error: {e}")

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
        logger.error(f"❌ WebSocket Worker Exception: {e}")

async def start_background_tasks(app_instance):
    global main_loop
    main_loop = asyncio.get_event_loop()
    load_cached_token()
    threading.Thread(target=keep_alive_self_ping, daemon=True).start()

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
