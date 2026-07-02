# File Name: main.py
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

# एंजेल वन ऑफिशियल स्मार्टएपीआई इम्पोर्ट्स
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- 📝 LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MUNH_TITAN_REALTIME_PROD")

# --- 🔑 HARDCODED BROKER CREDENTIALS ---
API_KEY = "Z80wG5Sg"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"

# --- 🚀 GLOBAL REALTIME STATES & CACHE ---
LTP_CACHE = {}               
LTP_LAST_UPDATE_TIME = {}    

SUBSCRIBED_TOKENS_REGISTRY = {
    1: set(), 2: set(), 3: set(), 4: set(), 5: set()
}

BROKER_SOCKET_CONNECTED = False
USER_SCORE = 0

# JWT टोकन मैनेजमेंट
JWT_SECRET = "MUNH_TITAN_SUPER_SECRET_KEY_2026"
JWT_ALGORITHM = "HS256"
BROKER_JWT_TOKEN = None
BROKER_FEED_TOKEN = None
LAST_BROKER_LOGIN_TIME = 0

main_loop = None
sws_client = None

# --- 🌐 ASYNC SOCKET.IO SERVER SETUP ---
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

EXCHANGE_MAPPING = {
    1: "NSE", 2: "NFO", 3: "BSE", 4: "BFO", 5: "MCX"
}

# --- 🔐 LOGIC: SESSION GENERATION ---
def generate_new_jwt_session():
    global USER_SCORE
    payload = {
        "server": "Munh_Titan",
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=24),
        "iat": datetime.datetime.utcnow()
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    USER_SCORE += 15 
    logger.info(f"New App JWT Token Created. Current Code Score: {USER_SCORE}")
    return token

def force_broker_socket_restart():
    global sws_client, BROKER_SOCKET_CONNECTED
    logger.info("🔄 Forcing WebSocket restart with fresh broker tokens...")
    if sws_client and BROKER_SOCKET_CONNECTED:
        try:
            sws_client.close()
        except Exception as e:
            logger.error(f"Error while forcing socket close: {e}")

async def broker_auto_login_task():
    global BROKER_JWT_TOKEN, BROKER_FEED_TOKEN, LAST_BROKER_LOGIN_TIME
    while True:
        try:
            # अगर सेशन 10 घंटे से पुराना है, तो रिलॉगिन करें
            if BROKER_JWT_TOKEN is None or (time.time() - LAST_BROKER_LOGIN_TIME >= 36000):
                logger.info("🔄 Initiating Auto-Login/Session Refresh...")
                totp_crypto = pyotp.TOTP(TOTP_STR)
                smart_conn = SmartConnect(api_key=API_KEY)
                session_data = smart_conn.generateSession(CLIENT_CODE, MPIN, totp_crypto.now())

                if session_data.get('status') and session_data.get('data'):
                    BROKER_JWT_TOKEN = session_data['data']['jwtToken']
                    BROKER_FEED_TOKEN = session_data['data']['feedToken']
                    LAST_BROKER_LOGIN_TIME = time.time()
                    logger.info("✅ [Login Manager]: Session Refreshed Successfully.")
                    force_broker_socket_restart()
                else:
                    logger.error(f"❌ Login Failed: {session_data.get('message')}")
            
            await asyncio.sleep(600) # हर 10 मिनट में चेक करें
        except Exception as e:
            logger.error(f"Critical error in login loop: {e}")
            await asyncio.sleep(60)

# --- 🧠 LOGIC: RAM CLEANER ---
async def ram_cleaner_task():
    while True:
        await asyncio.sleep(300)
        current_time = time.time()
        keys_to_delete = [token for token, last_time in LTP_LAST_UPDATE_TIME.items() if current_time - last_time > 300]
        for token in keys_to_delete:
            LTP_CACHE.pop(token, None)
            LTP_LAST_UPDATE_TIME.pop(token, None)
        logger.info(f"RAM Cleaning Sequence Ended. Active Cache: {len(LTP_CACHE)}")

# --- 🔇 LOGIC: ANTI-SLEEP MODE ---
async def serve_silent_mp3(request):
    return web.Response(body=b'\xff\xfb\x90\x44\x00', content_type="audio/mpeg")

async def render_self_ping_task():
    import aiohttp
    await asyncio.sleep(20)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("http://localhost:10000/silent.mp3") as response:
                    pass
        except: pass
        await asyncio.sleep(600)

# --- 🔌 SOCKET.IO EVENTS ---
@sio.event
async def subscribe_request(sid, data):
    global sws_client
    try:
        payload = json.loads(data) if isinstance(data, str) else data
        action = payload.get("action", "")
        exchange_code = payload.get("exchange")
        tokens_list = payload.get("tokens", [])

        if not isinstance(exchange_code, int): return

        if action == "sub":
            for token in tokens_list:
                str_token = str(token)
                await sio.enter_room(sid, str_token)
                if str_token in LTP_CACHE:
                    await sio.emit("live_data", {"token": str_token, "ltp": LTP_CACHE[str_token]}, room=sid)
                
                if str_token not in SUBSCRIBED_TOKENS_REGISTRY[exchange_code]:
                    SUBSCRIBED_TOKENS_REGISTRY[exchange_code].add(str_token)

            if BROKER_SOCKET_CONNECTED and sws_client:
                sws_client.subscribe("munh_titan_live", 1, [{"exchangeType": exchange_code, "tokens": tokens_list}])
    except Exception as e:
        logger.error(f"Error subscribe logic: {e}")

# --- 📡 ANGEL ONE CALLBACK ENGINE (PRICE DIVIDER FIX) ---
def on_data_received(wsapp, message):
    global main_loop
    try:
        tick_data = json.loads(message) if isinstance(message, str) else message
        token_str = str(tick_data.get("token", tick_data.get("t", "")))
        raw_ltp = tick_data.get("last_traded_price", tick_data.get("ltp", 0))

        # 💡 FIX: प्राइस को 100 से डिवाइड करके 2 डेसिमल में फिक्स किया
        try:
            val = float(raw_ltp)
            price_str = f"{val / 100:.2f}"
        except:
            price_str = str(raw_ltp)

        LTP_CACHE[token_str] = price_str
        LTP_LAST_UPDATE_TIME[token_str] = time.time()

        if main_loop:
            asyncio.run_coroutine_threadsafe(
                sio.emit("live_data", {"token": token_str, "ltp": price_str}, room=token_str), 
                main_loop
            )
    except Exception as e:
        pass

def on_websocket_open(wsapp):
    global BROKER_SOCKET_CONNECTED, sws_client
    BROKER_SOCKET_CONNECTED = True
    logger.info("🌐 [WebSocket Opened]")
    for exch_code, tokens_set in SUBSCRIBED_TOKENS_REGISTRY.items():
        if tokens_set:
            sws_client.subscribe("munh_titan_live", 1, [{"exchangeType": exch_code, "tokens": list(tokens_set)}])

def on_websocket_close(wsapp, code, msg):
    global BROKER_SOCKET_CONNECTED
    BROKER_SOCKET_CONNECTED = False
    logger.warning("❌ [Stream Disconnected]. Reconnecting in 5s...")
    # क्रैश/डिस्कनेक्ट होने पर थ्रेड के जरिए फिर से कनेक्ट करें
    threading.Thread(target=lambda: (time.sleep(5), start_angel_one_websocket_worker(BROKER_JWT_TOKEN, BROKER_FEED_TOKEN)), daemon=True).start()

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
    app['ram_cleaner'] = asyncio.create_task(ram_cleaner_task())
    app['anti_sleep_ping'] = asyncio.create_task(render_self_ping_task())
    
    # इनिशियल लॉगिन
    try:
        totp_crypto = pyotp.TOTP(TOTP_STR)
        smart_conn = SmartConnect(api_key=API_KEY)
        session_data = smart_conn.generateSession(CLIENT_CODE, MPIN, totp_crypto.now())
        if session_data.get('status'):
            global BROKER_JWT_TOKEN, BROKER_FEED_TOKEN, LAST_BROKER_LOGIN_TIME
            BROKER_JWT_TOKEN = session_data['data']['jwtToken']
            BROKER_FEED_TOKEN = session_data['data']['feedToken']
            LAST_BROKER_LOGIN_TIME = time.time()
            threading.Thread(target=start_angel_one_websocket_worker, args=(BROKER_JWT_TOKEN, BROKER_FEED_TOKEN), daemon=True).start()
    except Exception as e:
        logger.error(f"Startup login failed: {e}")
    
    app['auto_login'] = asyncio.create_task(broker_auto_login_task())

app.router.add_get('/silent.mp3', serve_silent_mp3)
app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
