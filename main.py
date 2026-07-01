# File Name: main.py
import asyncio
import json
import logging
import time
import datetime
import jwt
import socketio
from aiohttp import web

# --- 📝 LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MUNH_TITAN_BACKEND")

# --- 🚀 GLOBAL STATES & CACHE ---
# हर टोकन का लेटेस्ट प्राइस यहाँ स्टोर रहेगा (Instant Cache रिप्लाई के लिए)
LTP_CACHE = {
    "99926000": "23500.00",  # Default Nifty Dummy
    "99919012": "77000.00"   # Default Sensex Dummy
}
# रैम क्लीनर के लिए लास्ट अपडेट टाइमस्टैम्प ट्रैकिंग
LTP_LAST_UPDATE_TIME = {}

# भाई के रिक्वेस्ट के मुताबिक स्कोर ट्रैकिंग वेरिएबल
USER_SCORE = 0

# JWT कॉन्फ़िगरेशन
JWT_SECRET = "MUNH_TITAN_SUPER_SECRET_KEY_2026"
JWT_ALGORITHM = "HS256"
BROKER_SESSION_TOKEN = "INITIAL_TOKEN"
LAST_BROKER_LOGIN_TIME = 0

# --- 🌐 ASYNC SOCKET.IO SERVER SETUP ---
# CORS अलाउ किया है ताकि एंड्रॉइड बिना किसी रुकावट के कनेक्ट हो सके
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# --- 🗺️ EXCHANGE NUMERIC MAPPING ---
# भाई के लॉजिक के अनुसार: बैकएंड इस मैप से तुरंत टाइप को पहचान लेगा
EXCHANGE_MAPPING = {
    1: "NSE",
    2: "NFO",
    3: "BSE",
    4: "BFO",
    5: "MCX"
}

# --- 🔐 LOGIC: AUTO LOGIN & JWT GENERATION (24-HOUR ROTATION) ---
def generate_new_jwt_session():
    """यूजर के लिए नया JWT टोकन जेनरेट करने का लॉजिक"""
    global USER_SCORE
    payload = {
        "server": "Munh_Titan",
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=24),
        "iat": datetime.datetime.utcnow()
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    USER_SCORE += 10 # स्कोर में लॉजिक के अनुसार बढ़ोतरी
    logger.info(f"New JWT Token Created. Current Code Score: {USER_SCORE}")
    return token

async def broker_auto_login_task():
    """24 घंटे में ऑटोमैटिक ब्रोकर सेशन लॉग इन और टोकन रिफ्रेश लॉजिक"""
    global BROKER_SESSION_TOKEN, LAST_BROKER_LOGIN_TIME
    while True:
        current_time = time.time()
        # अगर 24 घंटे (86400 सेकेंड) हो गए हैं या पहला रन है
        if current_time - LAST_BROKER_LOGIN_TIME >= 86400 or LAST_BROKER_LOGIN_TIME == 0:
            logger.info("🔄 24 Hours Completed! Triggering Auto-Login & Refreshing Session...")
            
            # यहाँ तुम्हारा ब्रोकर का लॉगिन API कॉल आएगा
            BROKER_SESSION_TOKEN = f"SESSION_REFRESHED_{int(current_time)}"
            LAST_BROKER_LOGIN_TIME = current_time
            
            # नया इंटरनल JWT भी रेडी कर लेते हैं
            new_internal_jwt = generate_new_jwt_session()
            logger.info(f"Broker Login Successful! Session Token updated to: {BROKER_SESSION_TOKEN}")
            
        # हर 1 घंटे में बैकग्राउंड चेक चलाएंगे
        await asyncio.sleep(3600)

# --- 🧠 LOGIC: RAM CLEANER (EVERY 5 MINUTES) ---
async def ram_cleaner_task():
    """5 मिनट से पुराने या इनएक्टिव डेटा को रैम से क्लियर करने का लॉजिक"""
    while True:
        await asyncio.sleep(300) # 5 मिनट का वेट टाइमस्टैम्प
        logger.info("🧹 RAM Cleaner Activated! Scanning inactive cache data...")
        current_time = time.time()
        keys_to_delete = []
        
        # उन टोकन्स को ढूंढो जिनका डेटा पिछले 5 मिनट से अपडेट नहीं हुआ है
        for token, last_time in list(LTP_LAST_UPDATE_TIME.items()):
            # मास्टर इंडेक्स को कभी डिलीट नहीं करना है
            if token in ["99926000", "99919012"]:
                continue
            if current_time - last_time > 300:
                keys_to_delete.append(token)
                
        # रैम क्लियर करो
        for token in keys_to_delete:
            if token in LTP_CACHE:
                del LTP_CACHE[token]
            if token in LTP_LAST_UPDATE_TIME:
                del LTP_LAST_UPDATE_TIME[token]
            logger.info(f"Removed Dead Token from RAM Cache: {token}")
            
        logger.info(f"RAM Cleaning Completed. Active Cache Count: {len(LTP_CACHE)}")

# --- 🔇 LOGIC: ANTI-SLEEP MODE (SILENT MP3 & SELF PING) ---
async def serve_silent_mp3(request):
    """एक छोटा 1 सेकंड का साइलेंट MP3 बाइटस्ट्रीम सर्व करता है ताकि कनेक्शन लाइव रहे"""
    silent_mp3_bytes = b'\xff\xfb\x90\x44\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    logger.info("🎵 Silent MP3 Route Triggered by Keep-Alive Pinger!")
    return web.Response(body=silent_mp3_bytes, content_type="audio/mpeg")

async def render_self_ping_task():
    """यह खुद को हर 10 मिनट में पिंग करेगा ताकि रेंडर कभी स्लीप मोड में न जाए"""
    await asyncio.sleep(10) 
    import aiohttp
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("http://localhost:10000/silent.mp3") as response:
                    status = response.status
                    logger.info(f"Anti-Sleep Ping Sent Successfully. Status: {status}")
        except Exception as e:
            logger.warning(f"Self-ping failed (Normal if initializing): {e}")
        
        await asyncio.sleep(600)

# --- 🔌 SOCKET.IO EVENTS (INCOMING / OUTGOING SEPARATED) ---

@sio.event
async def connect(sid, environ):
    logger.info(f"Client Connected: {sid}")

@sio.event
async def disconnect(sid):
    logger.info(f"Client Disconnected: {sid}")

# 🔥 जाने का रास्ता (Android इसी "subscribe_request" इवेंट पर डेटा भेजेगा)
@sio.event
async def subscribe_request(sid, data):
    """
    एंड्रॉइड से आने वाली क्वेरी को प्रोसेस करने का कोर इंजन।
    यह ऑटोमैटिक एक्सचेंज टाइप को पहचानेगा और सब्सक्राइब करेगा।
    """
    try:
        if isinstance(data, str):
            payload = json.loads(data)
        else:
            payload = data

        action = payload.get("action", "")       # 'sub' या 'unsub'
        exchange_code = payload.get("exchange")   # न्यूमेरिक कोड (1, 2, 3, 4, 5)
        tokens_list = payload.get("tokens", [])   # टोकन्स का एरे

        # 🚨 स्ट्रिक्ट चेक: अगर एक्सचेंज कोड न्यूमेरिक नहीं है, तो रिजेक्ट करो!
        if not isinstance(exchange_code, int):
            logger.error(f"❌ Subscription Rejected! Exchange Code must be numeric Int. Got: {exchange_code}")
            return

        # 🎯 एक्सचेंज टाइप पहचानने का लॉजिक
        exchange_name = EXCHANGE_MAPPING.get(exchange_code, "UNKNOWN")
        
        if exchange_name == "UNKNOWN":
            logger.error(f"⚠️ Unknown Exchange Type Received (Code: {exchange_code}). Cannot process subscribe!")
            return
        
        # सर्वर ने सफलतापूर्वक एक्सचेंज टाइप को पहचान लिया
        logger.info(f"🎯 Backend successfully recognized Exchange Type [{exchange_code}] as '{exchange_name}'")

        if action == "sub":
            # बैच या सिंगल—दोनों के लिए लूप काम करेगा बिना किसी शॉर्टकट के
            for token in tokens_list:
                str_token = str(token)
                if not str_token or str_token == "None":
                    continue
                
                # 1️⃣ लॉजिक: पहचाने गए एक्सचेंज के साथ टोकन का अलग रूम ज्वाइन कराओ
                await sio.enter_room(sid, str_token)
                logger.info(f"✅ User {sid} Subscribed to Room: {str_token} | Identified Exchange: {exchange_name}")

                # 2️⃣ लॉजिक: तुरंत कैश मेमोरी से लेटेस्ट प्राइस वापस फेंको (Instant Response)
                if str_token in LTP_CACHE:
                    current_cached_price = LTP_CACHE[str_token]
                    immediate_response = {
                        "t": str_token,
                        "ltp": current_cached_price
                    }
                    # 🔥 आने का रास्ता: तुरंत क्लाइंट को 'live_data' चैनल पर रिप्लाई भेजो
                    await sio.emit("live_data", immediate_response, room=sid)

        elif action == "unsub":
            for token in tokens_list:
                str_token = str(token)
                await sio.leave_room(sid, str_token)
                logger.info(f"🚫 User {sid} Left Room for Token: {str_token} ({exchange_name})")

    except Exception as e:
        logger.error(f"Error processing subscribe_request: {e}")

# --- 📈 SIMULATED EXCHANGE TICK FEEDER (BROADCASTER) ---
async def mock_exchange_tick_generator():
    """
    यह बैकग्राउंड टास्क एक्सचेंज के वेबसॉकेट की तरह काम करता है।
    जैसे ही नया प्राइस आएगा, यह बिना लूप के सीधे उस टोकन के रूम में ब्लास्ट करेगा।
    """
    import random
    while True:
        active_test_tokens = ["99926000", "99919012", "14366", "54321"]
        
        for token in active_test_tokens:
            base_price = float(LTP_CACHE.get(token, "100.00"))
            new_price = base_price + random.uniform(-2.0, 2.0)
            if new_price < 1: new_price = 100.0
            
            price_str = f"{new_price:.2f}"
            
            LTP_CACHE[token] = price_str
            LTP_LAST_UPDATE_TIME[token] = time.time()
            
            tick_packet = {
                "t": token,
                "ltp": price_str
            }
            
            # 🔥 सुपर-फास्ट रूम ब्रॉडकास्ट
            await sio.emit("live_data", tick_packet, room=token)
            
        await asyncio.sleep(0.2)

# --- 🛠️ STARTUP INJECTOR ---
async def start_background_tasks(app):
    """सारे बैकग्राउंड टास्क को सर्वर स्टार्टअप पर इंजेक्ट करने का लॉजिक"""
    app['ram_cleaner'] = asyncio.create_task(ram_cleaner_task())
    app['anti_sleep_ping'] = asyncio.create_task(render_self_ping_task())
    app['auto_login'] = asyncio.create_task(broker_auto_login_task())
    app['tick_feeder'] = asyncio.create_task(mock_exchange_tick_generator())
    logger.info("🚀 All Titan Engine Core Background Tasks Started Successfully!")

# --- 👑 HTTP ROUTING ---
app.router.add_get('/silent.mp3', serve_silent_mp3)
app.on_startup.append(start_background_tasks)

# --- 🏁 EXECUTION MAIN ENTRY ---
if __name__ == "__main__":
    logger.info("🔥 Initiating MUNH TITAN GLOBAL ENGINE (Backend Side)...")
    web.run_app(app, host="0.0.0.0", port=10000)
