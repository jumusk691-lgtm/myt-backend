import eventlet
eventlet.monkey_patch()

import os
import pyotp
import redis
import socketio
import threading
import time
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client, Client

# --- REDIS SETUP ---
redis_url = os.environ.get("REDIS_URL")
r = redis.from_url(redis_url, decode_responses=True)

# --- CONFIG ---
API_KEY = "85HE4VA1"
CLIENT_ID = "S52638556"
PIN = "0000" 
TOTP_KEY = "XFTXZ2445N4V2UMB7EWUCBDRMU"
SUPABASE_URL ="https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDgzOTEzNCwiZXhwIjoyMDg2NDE1MTM0fQ.5BofQbMKiMLGFjqcIGaCwpoO9pLZnuLg7nojP0aGhJw"

# Supabase Initialization
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- SERVER SETUP ---
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
socketio_app = socketio.WSGIApp(sio)

def index(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/plain')])
    return [b"MYT Pro Backend is Running with Redis & Bulk Support!"]

def app(environ, start_response):
    if environ.get('PATH_INFO') == '/':
        return index(environ, start_response)
    return socketio_app(environ, start_response)

sws_instance = None

# --- DB UPDATE HELPER (Optimized for 2000 stocks) ---
def update_db_async(token, lp):
    # Sirf tabhi update karein jab price significant ho, ya Redis ka use karein
    try:
        # Note: 2000 stocks ke liye har second Supabase update karna server ko slow karega.
        # Redis is primary now. Supabase periodic update ke liye use karein.
        supabase.table("market_data").update({"last_price": lp}).eq("token", token).execute()
    except Exception as e:
        pass # Errors ignore karein performance ke liye

# --- ANGEL WEBSOCKET HANDLER ---
def on_data(wsapp, msg):
    if isinstance(msg, dict) and 'last_traded_price' in msg:
        token = str(msg.get('token')).strip()
        lp = str(msg.get('last_traded_price') / 100)
        
        # 1. Redis mein save karein (Sabse Fast)
        r.set(f"price:{token}", lp)
        
        # 2. APK ko turant bhejein
        sio.emit('livePrice', {"tk": token, "lp": lp})

def on_open(wsapp):
    print("✅ Angel WebSocket Connected Successfully")

# --- ANGEL LOGIN ---
def login_to_angel():
    global sws_instance
    try:
        obj = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_KEY).now()
        session = obj.generateSession(CLIENT_ID, PIN, totp)
        
        if session.get('status'):
            sws_instance = SmartWebSocketV2(
                session['data']['jwtToken'], 
                API_KEY, 
                CLIENT_ID, 
                session['data']['feedToken']
            )
            sws_instance.on_data = on_data
            sws_instance.on_open = on_open
            
            threading.Thread(target=sws_instance.connect, daemon=True).start()
            print("🚀 Angel Session Live (Background Thread)")
    except Exception as e:
        print(f"❌ Login Error: {e}")

# --- SMART EXCHANGE MAPPING ---
def get_exchange_type(token):
    t = int(token)
    if t < 20000: return 1      # NSE
    if 35000 <= t <= 75000: return 1 # Indices
    if 100000 <= t <= 999999: return 5 # MCX
    return 2 # NFO/Derivatives

# --- SOCKET.IO EVENTS ---
@sio.event
def connect(sid, environ):
    print(f"📱 APK Connected: {sid}")
    if sws_instance is None:
        eventlet.spawn(login_to_angel)

@sio.event
def subscribe(sid, data):
    global sws_instance
    if data and sws_instance:
        # data: ['1062', '1234', ...] - 1500 to 2000 tokens
        print(f"📡 Subscribing to {len(data)} tokens")
        
        # Batching: 2000 stocks ko 500-500 ke groups mein baantna
        chunk_size = 500
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            subs = {}
            for t in chunk:
                etype = get_exchange_type(t)
                if etype not in subs: subs[etype] = []
                subs[etype].append(str(t))
            
            for etype, tokens in subs.items():
                subscription_list = [{"exchangeType": etype, "tokens": tokens}]
                sws_instance.subscribe("myt_pro_feed", 3, subscription_list)
            
            time.sleep(0.5) # Angel One server ko overload se bachane ke liye

# API for App to fetch Bulk prices from Redis
@sio.event
def get_all_prices_request(sid, tokens):
    # App se list aayegi tokens ki, Redis se batch mein fetch hoga
    keys = [f"price:{t}" for t in tokens]
    prices = r.mget(keys)
    result = {t: p for t, p in zip(tokens, prices) if p is not None}
    sio.emit('bulkPrices', result, room=sid)

if __name__ == '__main__':
    import eventlet.wsgi
    # Port 10000 for Render
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
