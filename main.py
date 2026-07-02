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
USER_SCORE = 0  # भाई के लिए स्कोर ट्रैकिंग

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
    # स्कोर यहाँ अपडेट हो रहा है, तुम इसे डैशबोर्ड पर भी भेज सकते हो
    return USER_SCORE

# --- 📡 CORE REALTIME ENGINE ---
def on_data_received(wsapp, message):
    """
    ULTRA-LOW LATENCY: डेटा आते ही बिना किसी देरी के रूम में एमिट करें।
    """
    global main_loop
    try:
        tick_data = json.loads(message) if isinstance(message, str) else message
        token_str = str(tick_data.get("token", tick_data.get("t", "")))
        raw_ltp = tick_data.get("last_traded_price", tick_data.get("ltp", 0))

        # प्राइस डिवाइडर
        try:
            val = float(raw_ltp)
            price_str = f"{val / 100:.2f}"
        except:
            price_str = str(raw_ltp)

        # Cache update
        LTP_CACHE[token_str] = price_str

        # 🚀 ZERO DELAY EMIT: सीधा रूम में भेजो, कोई बफरिंग नहीं
        if main_loop:
            asyncio.run_coroutine_threadsafe(
                sio.emit("live_data", {"token": token_str, "ltp": price_str}, room=token_str), 
                main_loop
            )
    except: pass

@sio.event
async def subscribe_request(sid, data):
    global sws_client, USER_SCORE
    try:
        payload = json.loads(data) if isinstance(data, str) else data
        action = payload.get("action", "")
        exchange_code = payload.get("exchange")
        tokens_list = payload.get("tokens", [])

        if action == "sub":
            update_user_score(1) # सब्सक्राइब करने पर स्कोर बढ़ाओ
            for token in tokens_list:
                str_token = str(token)
                await sio.enter_room(sid, str_token)
                if str_token in LTP_CACHE:
                    await sio.emit("live_data", {"token": str_token, "ltp": LTP_CACHE[str_token]}, room=sid)
                
                if str_token not in SUBSCRIBED_TOKENS_REGISTRY[exchange_code]:
                    SUBSCRIBED_TOKENS_REGISTRY[exchange_code].add(str_token)

            if BROKER_SOCKET_CONNECTED and sws_client:
                sws_client.subscribe("munh_titan_live", 1, [{"exchangeType": exchange_code, "tokens": tokens_list}])
    except: pass

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
                session_data = smart_conn.generateSession(CLIENT_CODE, MPIN, totp_crypto.now())
                if session_data.get('status'):
                    BROKER_JWT_TOKEN = session_data['data']['jwtToken']
                    BROKER_FEED_TOKEN = session_data['data']['feedToken']
                    LAST_BROKER_LOGIN_TIME = time.time()
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
    except: pass
    app['auto_login'] = asyncio.create_task(broker_auto_login_task())

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
