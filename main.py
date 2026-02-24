import eventlet
eventlet.monkey_patch(all=True)

import os
import pyotp
import socketio
import redis
from supabase import create_client
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- CONFIGURATION ---
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." # Apni puri key rakhein

# --- REDIS SETUP (Scaling ke liye zaroori) ---
# Jab aap Koyeb ya VPS par honge, toh wahan Redis ka URL milega
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

# Socket.io with Redis Adapter
mgr = socketio.RedisManager(REDIS_URL)
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet', client_manager=mgr)
socketio_app = socketio.WSGIApp(sio)

# --- GLOBAL STATE ---
sws_instance = None
is_ws_ready = False 
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
subscribed_tokens = set()

# --- CALLBACKS (Optimized for 1 Lakh Users) ---
def on_data(wsapp, msg):
    if isinstance(msg, dict):
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp')
        token = msg.get('token')
        
        if ltp_raw is not None and token:
            lp = "{:.2f}".format(float(ltp_raw) / 100)
            token_str = str(token).strip()
            
            # 🔥 CRITICAL LOGIC: DB ke bajaye seedha Broadcast karein
            # Database update har 1 second mein 1 lakh baar nahi ho sakta, 
            # isliye hum seedha Socket se bhej rahe hain.
            sio.emit('livePrice', {"tk": token_str, "lp": lp})

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("✅ Angel WebSocket Active")
    subscribe_all_from_db()

# --- ANGEL LOGIN & RECONNECT ---
def login_to_angel():
    global sws_instance
    try:
        API_KEY = "85HE4VA1"
        CLIENT_ID = "S52638556"
        PIN = "0000" 
        TOTP_KEY = "XFTXZ2445N4V2UMB7EWUCBDRMU"
        
        obj = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_KEY).now()
        session = obj.generateSession(CLIENT_ID, PIN, totp)
        
        if session.get('status'):
            sws_instance = SmartWebSocketV2(
                session['data']['jwtToken'], API_KEY, CLIENT_ID, session['data']['feedToken']
            )
            sws_instance.on_data = on_data
            sws_instance.on_open = on_open
            sws_instance.on_error = lambda ws, err: print(f"Error: {err}")
            sws_instance.on_close = lambda ws: login_to_angel()
            eventlet.spawn(sws_instance.connect)
    except Exception as e:
        print(f"❌ Login Error: {e}")

def subscribe_all_from_db():
    global subscribed_tokens, sws_instance, is_ws_ready
    try:
        res = supabase.table("market_data").select("token").execute()
        tokens = [str(item['token']) for item in res.data if item['token']]
        
        if tokens and sws_instance and is_ws_ready:
            formatted_list = []
            for t in tokens:
                if t not in subscribed_tokens:
                    ex_type = 2 if len(t) > 5 else 1
                    formatted_list.append({"exchangeType": ex_type, "tokens": [t]})
                    subscribed_tokens.add(t)
            
            if formatted_list:
                sws_instance.subscribe("bhai_master", 1, formatted_list)
    except Exception as e:
        print(f"DB Error: {e}")

# --- SERVER START ---
if __name__ == '__main__':
    login_to_angel()
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), socketio_app)me jab comit changes kar rahahu ti ese arahihe
