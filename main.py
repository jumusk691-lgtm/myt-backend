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

# --- SETUP ---
redis_url = os.environ.get("REDIS_URL")
r = redis.from_url(redis_url, decode_responses=True)

API_KEY = "85HE4VA1"
CLIENT_ID = "S52638556"
PIN = "0000" 
TOTP_KEY = "XFTXZ2445N4V2UMB7EWUCBDRMU"
SUPABASE_URL ="https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDgzOTEzNCwiZXhwIjoyMDg2NDE1MTM0fQ.5BofQbMKiMLGFjqcIGaCwpoO9pLZnuLg7nojP0aGhJw"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
socketio_app = socketio.WSGIApp(sio)

# --- GLOBAL STATE ---
sws_instance = None
is_ws_ready = False  # Connection status track karne ke liye

def on_data(wsapp, msg):
    if isinstance(msg, dict) and 'last_traded_price' in msg:
        token = str(msg.get('token')).strip()
        lp = str(msg.get('last_traded_price') / 100)
        r.set(f"price:{token}", lp)
        sio.emit('livePrice', {"tk": token, "lp": lp})

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("✅ Angel WebSocket Connected & Ready")

def on_error(wsapp, error):
    global is_ws_ready
    is_ws_ready = False
    print(f"❌ WS Error: {error}")

def on_close(wsapp):
    global is_ws_ready
    is_ws_ready = False
    print("🔌 Connection Closed. Retrying login...")

def login_to_angel():
    global sws_instance, is_ws_ready
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
            sws_instance.on_error = on_error
            sws_instance.on_close = on_close
            
            # Non-blocking connection
            threading.Thread(target=sws_instance.connect, daemon=True).start()
    except Exception as e:
        print(f"❌ Login Failed: {e}")

def get_exchange_type(token):
    t = int(token)
    if t < 30000: return 1           # NSE Stocks
    if 35000 <= t <= 99999: return 1 # Indices
    if 100000 <= t <= 999999: return 5 # MCX
    return 2                         # NFO/Options

@sio.event
def connect(sid, environ):
    print(f"📱 App Connected: {sid}")
    if not sws_instance:
        eventlet.spawn(login_to_angel)

@sio.event
def subscribe(sid, data):
    global sws_instance, is_ws_ready
    
    # 1. Connection check with wait
    wait_count = 0
    while not is_ws_ready and wait_count < 5:
        print("⏳ Waiting for WebSocket to stabilize...")
        time.sleep(1)
        wait_count += 1

    if data and sws_instance and is_ws_ready:
        print(f"📡 Subscribing to {len(data)} tokens")
        chunk_size = 500
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            subs = {}
            for t in chunk:
                etype = get_exchange_type(t)
                if etype not in subs: subs[etype] = []
                subs[etype].append(str(t))
            
            for etype, tokens in subs.items():
                try:
                    sws_instance.subscribe("myt_pro_feed", 3, [{"exchangeType": etype, "tokens": tokens}])
                except Exception as e:
                    print(f"Subscribe Error: {e}")
            
            time.sleep(0.8) # Thoda extra gap for stability
    else:
        print("⚠️ Failed to subscribe: WebSocket not ready.")

@sio.event
def get_all_prices_request(sid, tokens):
    keys = [f"price:{t}" for t in tokens]
    prices = r.mget(keys)
    result = {t: p for t, p in zip(tokens, prices) if p is not None}
    sio.emit('bulkPrices', result, room=sid)

def app(environ, start_response):
    if environ.get('PATH_INFO') == '/':
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b"MYT Pro Backend Live"]
    return socketio_app(environ, start_response)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
