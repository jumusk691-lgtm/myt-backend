# File Name: 'Munh' -> Saved as main.py
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

# --- 🔑 HARDCODED BROKER CREDENTIALS (FROM YOUR IMAGE) ---
API_KEY = "Z80wG5Sg"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"

# --- 🚀 GLOBAL REALTIME STATES & CACHE ---
LTP_CACHE = {}               # केवल असली लाइव मार्केट प्राइस यहाँ स्टोर होंगे
LTP_LAST_UPDATE_TIME = {}    # रैम क्लीनर के लिए टाइमस्टैम्प ट्रैकिंग

# 💡 FIX: अब यह डिक्शनरी है ताकि एक्सचेंज कोड के साथ टोकन याद रख सके (रीकनेक्शन के लिए)
SUBSCRIBED_TOKENS_REGISTRY = {
    1: set(), # NSE
    2: set(), # NFO
    3: set(), # BSE
    4: set(), # BFO
    5: set()  # MCX
}

# 💡 FIX: सॉकेट का रियल-टाइम स्टेट ट्रैक करने के लिए फ्लैग
BROKER_SOCKET_CONNECTED = False

# भाई के रिक्वेस्ट के मुताबिक स्कोर ट्रैकिंग वेरिएबल
USER_SCORE = 0

# JWT टोकन मैनेजमेंट
JWT_SECRET = "MUNH_TITAN_SUPER_SECRET_KEY_2026"
JWT_ALGORITHM = "HS256"
BROKER_JWT_TOKEN = None
BROKER_FEED_TOKEN = None
LAST_BROKER_LOGIN_TIME = 0

# ग्लोबल एसिंक लूप वेरिएबल (वेबसॉकेट थ्रेड से डेटा ट्रांसफर करने के लिए)
main_loop = None
sws_client = None

# --- 🌐 ASYNC SOCKET.IO SERVER SETUP ---
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# --- 🗺️ EXCHANGE NUMERIC MAPPING (FROM YOUR CONFIG) ---
EXCHANGE_MAPPING = {
    1: "NSE",
    2: "NFO",
    3: "BSE",
    4: "BFO",
    5: "MCX"
}

# --- 🔐 LOGIC: REALTIME TOTP & SESSION GENERATION (24-HOUR AUTOLOGIN) ---
def generate_new_jwt_session():
    """यूजर ऐप वेरिफिकेशन के लिए इंटरनल टोकन जेनरेट करने का लॉजिक"""
    global USER_SCORE
    payload = {
        "server": "Munh_Titan",
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=24),
        "iat": datetime.datetime.utcnow()
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    USER_SCORE += 15 # स्कोर में लॉजिक के अनुसार बढ़ोतरी
    logger.info(f"New App JWT Token Created. Current Code Score: {USER_SCORE}")
    return token

def force_broker_socket_restart():
    """री-लॉगिन के बाद नए टोकन के साथ वेबसॉकेट को रिफ्रेश करने का लॉजिक"""
    global sws_client, BROKER_SOCKET_CONNECTED
    logger.info("🔄 Forcing WebSocket restart with fresh broker tokens...")
    if sws_client and BROKER_SOCKET_CONNECTED:
        try:
            sws_client.close() # यह क्लोज़ इवेंट ट्रिगर करेगा, जो ऑटो-रीकनेक्ट को चालू कर देगा
        except Exception as e:
            logger.error(f"Error while forcing socket close: {e}")

async def broker_auto_login_task():
    """pyotp का उपयोग करके ऑटोमैटिक असली एंजेल वन सेशन लॉगिन और रिफ्रेश लॉजिक"""
    global BROKER_JWT_TOKEN, BROKER_FEED_TOKEN, LAST_BROKER_LOGIN_TIME
    while True:
        current_time = time.time()
        # 💡 FIX: हर 12 घंटे (43200 सेकंड) में नया सेशन लाओ ताकि कभी एक्सपायर ना हो
        if current_time - LAST_BROKER_LOGIN_TIME >= 43200 or LAST_BROKER_LOGIN_TIME == 0:
            logger.info("🔄 Initiating Real-time Angel One Login with TOTP Engine...")
            try:
                # pyotp की मदद से लाइव TOTP जेनरेट करें
                totp_crypto = pyotp.TOTP(TOTP_STR)
                current_live_totp = totp_crypto.now()
                logger.info(f"🔑 Live TOTP Generated Successfully: {current_live_totp}")

                # एंजेल वन API कनेक्शन स्थापित करना
                smart_conn = SmartConnect(api_key=API_KEY)
                session_data = smart_conn.generateSession(CLIENT_CODE, MPIN, current_live_totp)

                if session_data.get('status') and session_data.get('data'):
                    BROKER_JWT_TOKEN = session_data['data']['jwtToken']
                    BROKER_FEED_TOKEN = session_data['data']['feedToken']
                    LAST_BROKER_LOGIN_TIME = current_time
                    
                    logger.info("✅ [Login Manager]: Angel One Real Session Generated Successfully.")
                    generate_new_jwt_session()
                    
                    # अगर लॉगिन रिफ्रेश हुआ है, तो सॉकेट को नए टोकन के साथ रिबूट करो
                    force_broker_socket_restart()
                else:
                    logger.error(f"❌ Broker Login Failed! Response Message: {session_data.get('message')}")
            
            except Exception as e:
                logger.error(f"❌ Critical Exception during Broker Real-Login: {e}")
            
        # हर 1 घंटे में टाइमस्टैम्प वैलिडिटी की जांच करें
        await asyncio.sleep(3600)

# --- 🧠 LOGIC: RAM CLEANER (EVERY 5 MINUTES) ---
async def ram_cleaner_task():
    """5 मिनट से इनएक्टिव टोकन्स का कचरा रैम से साफ करने का लॉजिक ताकि रेंडर कभी क्रैश न हो"""
    while True:
        await asyncio.sleep(300)
        logger.info("🧹 RAM Cleaner Activated! Scanning for inactive market tickers...")
        current_time = time.time()
        keys_to_delete = []
        
        for token, last_time in list(LTP_LAST_UPDATE_TIME.items()):
            # अगर किसी टोकन का भाव पिछले 5 मिनट से अपडेट नहीं हुआ, तो उसे रैम से हटाएं
            if current_time - last_time > 300:
                keys_to_delete.append(token)
                
        for token in keys_to_delete:
            if token in LTP_CACHE:
                del LTP_CACHE[token]
            if token in LTP_LAST_UPDATE_TIME:
                del LTP_LAST_UPDATE_TIME[token]
            logger.info(f"Removed Dead Real-Token from Memory: {token}")
            
        logger.info(f"RAM Cleaning Sequence Ended. Active Cache: {len(LTP_CACHE)}")

# --- 🔇 LOGIC: ANTI-SLEEP MODE (SILENT MP3 & SELF PING) ---
async def serve_silent_mp3(request):
    """रेंडर को लाइव रखने के लिए 1 सेकंड का साइलेंट बाइटस्ट्रीम (24 Hour Uptime)"""
    silent_mp3_bytes = b'\xff\xfb\x90\x44\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    return web.Response(body=silent_mp3_bytes, content_type="audio/mpeg")

async def render_self_ping_task():
    """रेंडर सर्वर को 24 घंटे एक्टिव रखने के लिए खुद को पिंग करने का बैकग्राउंड टास्क"""
    await asyncio.sleep(20)
    import aiohttp
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("http://localhost:10000/silent.mp3") as response:
                    status = response.status
                    logger.info(f"Anti-Sleep Self-Ping Executed. Status: {status}")
        except Exception as e:
            logger.warning(f"Self-ping warning (Server initializing): {e}")
        
        # हर 10 मिनट में पिंग करेगा
        await asyncio.sleep(600)

# --- 🔌 SOCKET.IO EVENTS (REALTIME INCOMING / OUTGOING) ---

@sio.event
async def connect(sid, environ):
    logger.info(f"Android Watchlist Connected: {sid}")

@sio.event
async def disconnect(sid):
    logger.info(f"Android Watchlist Disconnected: {sid}")

@sio.event
async def subscribe_request(sid, data):
    """
    एंड्रॉइड ऐप से आने वाले रियल रिक्वेस्ट हैंडलर।
    एक्सचेंज टाइप (1-5) को पहचान कर एंजेल वन लाइव स्ट्रीम पर टोकन रजिस्टर करेगा।
    """
    global sws_client, BROKER_SOCKET_CONNECTED
    try:
        if isinstance(data, str):
            payload = json.loads(data)
        else:
            payload = data

        action = payload.get("action", "")       
        exchange_code = payload.get("exchange")   
        tokens_list = payload.get("tokens", [])   

        if not isinstance(exchange_code, int) or exchange_code not in SUBSCRIBED_TOKENS_REGISTRY:
            logger.error(f"❌ Invalid Exchange Format! Must be valid Integer. Got: {exchange_code}")
            return

        exchange_name = EXCHANGE_MAPPING.get(exchange_code, "UNKNOWN")
        logger.info(f"🎯 Recognized Exchange Request for [{exchange_name}] (Code: {exchange_code})")

        if action == "sub":
            tokens_to_subscribe_on_broker = []
            
            for token in tokens_list:
                str_token = str(token)
                if not str_token or str_token == "None":
                    continue
                
                # 1️⃣ ऐप यूजर को उस टोकन के रूम (Room) में डालें
                await sio.enter_room(sid, str_token)
                
                # 2️⃣ अगर पहले से रैम में लाइव प्राइस मौजूद है तो तुरंत डिलीवर करें (नो लैग)
                if str_token in LTP_CACHE:
                    immediate_response = {
                        "t": str_token,
                        "ltp": LTP_CACHE[str_token]
                    }
                    await sio.emit("live_data", immediate_response, room=sid)

                # 💡 FIX: टोकन को सही एक्सचेंज कोड के सेट में रजिस्टर करें
                if str_token not in SUBSCRIBED_TOKENS_REGISTRY[exchange_code]:
                    SUBSCRIBED_TOKENS_REGISTRY[exchange_code].add(str_token)
                    tokens_to_subscribe_on_broker.append(str_token)

            # 3️⃣ 💡 FIX: केवल तभी रिक्वेस्ट भेजें जब कनेक्शन एक्टिव हो
            if tokens_to_subscribe_on_broker:
                if BROKER_SOCKET_CONNECTED and sws_client:
                    correlation_id = "munh_titan_live"
                    payload_broker = [
                        {"exchangeType": exchange_code, "tokens": tokens_to_subscribe_on_broker}
                    ]
                    sws_client.subscribe(correlation_id, 1, payload_broker)
                    logger.info(f"📡 Real-time Subscription sent to Angel One for Tokens: {tokens_to_subscribe_on_broker}")
                else:
                    logger.warning(f"⏳ Broker socket not connected yet! Added {len(tokens_to_subscribe_on_broker)} tokens to registry for auto-subscribe once connection opens.")

        elif action == "unsub":
            tokens_to_unsubscribe = []
            for token in tokens_list:
                str_token = str(token)
                await sio.leave_room(sid, str_token)
                # 💡 FIX: सही एक्सचेंज कोड से अन-सब्सक्राइब करें
                if str_token in SUBSCRIBED_TOKENS_REGISTRY[exchange_code]:
                    SUBSCRIBED_TOKENS_REGISTRY[exchange_code].remove(str_token)
                    tokens_to_unsubscribe.append(str_token)
            
            if tokens_to_unsubscribe and BROKER_SOCKET_CONNECTED and sws_client:
                sws_client.unsubscribe("munh_titan_live", 1, [{"exchangeType": exchange_code, "tokens": tokens_to_unsubscribe}])
                logger.info(f"🚫 Unsubscribed Real-Tokens from Stream: {tokens_to_unsubscribe}")

    except Exception as e:
        logger.error(f"Error handling live subscribe request logic: {e}")

# --- 📡 ANGEL ONE SMARTWEBSOCKETV2 CALLBACK ENGINE ---

def on_data_received(wsapp, message):
    """
    एंजेल वन से आने वाला असली लाइव मार्केट डेटा का कोर कॉलबैक।
    """
    global main_loop
    try:
        tick_data = json.loads(message) if isinstance(message, str) else message

        token_str = str(tick_data.get("token", tick_data.get("t", "")))
        raw_ltp = tick_data.get("last_traded_price", tick_data.get("ltp", ""))

        if token_str and raw_ltp:
            if isinstance(raw_ltp, int) or isinstance(raw_ltp, float):
                price_str = f"{float(raw_ltp) / 100:.2f}" if isinstance(raw_ltp, int) and raw_ltp > 100000 else f"{float(raw_ltp):.2f}"
            else:
                price_str = str(raw_ltp)

            LTP_CACHE[token_str] = price_str
            LTP_LAST_UPDATE_TIME[token_str] = time.time()

            outgoing_packet = {
                "t": token_str,
                "ltp": price_str
            }

            if main_loop:
                asyncio.run_coroutine_threadsafe(
                    sio.emit("live_data", outgoing_packet, room=token_str), 
                    main_loop
                )
    except Exception as e:
        pass

def on_websocket_open(wsapp):
    global BROKER_SOCKET_CONNECTED, sws_client
    BROKER_SOCKET_CONNECTED = True
    logger.info("🌐 [WebSocket Opened]: Successfully synced with Angel One Streaming Clusters.")
    
    # 💡 FIX: कनेक्शन खुलने पर रजिस्ट्री से सभी पुराने टोकन्स को फिर से ऑटो-सब्सक्राइब करना
    if sws_client:
        for exch_code, tokens_set in SUBSCRIBED_TOKENS_REGISTRY.items():
            if tokens_set:
                token_list = list(tokens_set)
                payload_broker = [{"exchangeType": exch_code, "tokens": token_list}]
                sws_client.subscribe("munh_titan_live", 1, payload_broker)
                logger.info(f"🔄 Auto Re-subscribed {len(token_list)} active tokens for Exchange Code: {exch_code}")

def on_websocket_error(wsapp, error):
    logger.error(f"⚠️ [Stream Error]: {error}")

def on_websocket_close(wsapp, close_status_code, close_msg):
    global BROKER_SOCKET_CONNECTED, USER_SCORE
    BROKER_SOCKET_CONNECTED = False
    logger.warning(f"❌ [Stream Disconnected]: Code: {close_status_code} | Msg: {close_msg}. Reconnecting in 5 seconds...")
    
    # 💡 FIX: ऑटो-रीकनेक्ट लॉजिक (बिना थ्रेड ब्लॉक किये बैकग्राउंड में दोबारा जुड़ना)
    def reconnect_worker():
        global USER_SCORE
        time.sleep(5)
        logger.info("♻️ Attempting Auto-Reconnect to Broker Stream...")
        if BROKER_JWT_TOKEN and BROKER_FEED_TOKEN:
            USER_SCORE += 10 # स्कोर अपडेट लॉजिक रीकनेक्ट पर
            logger.info(f"Auto-reconnect triggered. Code Score increased to: {USER_SCORE}")
            start_angel_one_websocket_worker(BROKER_JWT_TOKEN, BROKER_FEED_TOKEN)
        else:
            logger.error("Cannot auto-reconnect, Broker tokens are missing!")
            
    threading.Thread(target=reconnect_worker, daemon=True).start()

# --- 🏗️ REALTIME WEB STREAM WORKER THREAD ---
def start_angel_one_websocket_worker(auth_token, feed_token):
    """एंजेल वन लाइव टिक फीड को बिना रुके बैकग्राउंड थ्रेड में चलाने का लॉजिक"""
    global sws_client
    
    sws_client = SmartWebSocketV2(
        auth_token=auth_token,
        client_code=CLIENT_CODE,
        api_key=API_KEY,
        feed_token=feed_token
    )
    
    sws_client.on_data = on_data_received
    sws_client.on_open = on_websocket_open
    sws_client.on_error = on_websocket_error
    sws_client.on_close = on_websocket_close
    
    logger.info("⚡ Powering up Angel One SmartWebSocketV2 Stream Line...")
    sws_client.connect()

# --- 🛠️ STARTUP INJECTOR ---
async def start_background_tasks(app):
    """सर्वर शुरू होते ही बैकग्राउंड टास्क को सही ऑर्डर में इनिशियलाइज करने का लॉजिक"""
    global main_loop
    main_loop = asyncio.get_event_loop()
    
    # 1. पहले डेटाबेस/रैम और ऑटो लॉगिन टास्क चालू करें
    app['ram_cleaner'] = asyncio.create_task(ram_cleaner_task())
    app['anti_sleep_ping'] = asyncio.create_task(render_self_ping_task())
    
    # लॉगिन टास्क का पहला रन इमीडियेटली चलाएं ताकि टोकन मिल सकें
    await broker_auto_login_task_initialization_helper()
    app['auto_login'] = asyncio.create_task(broker_auto_login_task())
    
    # 2. लाइव वेबसॉकेट वर्कर थ्रेड को एक्टिवेट करें
    if BROKER_JWT_TOKEN and BROKER_FEED_TOKEN:
        websocket_thread = threading.Thread(
            target=start_angel_one_websocket_worker,
            args=(BROKER_JWT_TOKEN, BROKER_FEED_TOKEN),
            daemon=True
        )
        websocket_thread.start()
        logger.info("🚀 Real-time Streaming Engine Injected & Running Live!")
    else:
        logger.error("❌ Cannot launch stream thread! Initial Broker Session token generation failed.")

async def broker_auto_login_task_initialization_helper():
    """स्टार्टअप के समय तुरंत पहला टोकन लाने का सिंक्रोनस हेल्पर"""
    global BROKER_JWT_TOKEN, BROKER_FEED_TOKEN, LAST_BROKER_LOGIN_TIME
    try:
        totp_crypto = pyotp.TOTP(TOTP_STR)
        smart_conn = SmartConnect(api_key=API_KEY)
        session_data = smart_conn.generateSession(CLIENT_CODE, MPIN, totp_crypto.now())
        if session_data.get('status') and session_data.get('data'):
            BROKER_JWT_TOKEN = session_data['data']['jwtToken']
            BROKER_FEED_TOKEN = session_data['data']['feedToken']
            LAST_BROKER_LOGIN_TIME = time.time()
            logger.info("🔑 Initial Startup Angel One Login Session: SUCCESS.")
    except Exception as e:
        logger.error(f"Startup login failed: {e}")

# --- 👑 HTTP ROUTING ---
app.router.add_get('/silent.mp3', serve_silent_mp3)
app.on_startup.append(start_background_tasks)

# --- 🏁 EXECUTION MAIN ENTRY ---
if __name__ == "__main__":
    logger.info("🔥 Starting MUNH TITAN GLOBAL REALTIME ENGINE (No Dummy Code Version)...")
    # रेंडर के लिए पोर्ट 10000 पर सर्वर को लाइव अप कर रहे हैं
    web.run_app(app, host="0.0.0.0", port=10000)
