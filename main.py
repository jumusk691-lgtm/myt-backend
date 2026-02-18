import eventlet
eventlet.monkey_patch(all=True)

import os
import pyotp
import socketio
import redis
from supabase import create_client
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- GLOBAL STATE ---
sws_instance = None
is_ws_ready = False 
r = None
supabase = None
# User ke subscribed tokens track karne ke liye
active_subscriptions = set()

def initialize_clients():
    global r, supabase
    redis_url = os.environ.get("REDIS_URL")
    r = redis.from_url(redis_url, decode_responses=True)
    
    supabase_url = "https://rcosgmsyisybusmuxzei.supabase.co"
    supabase_key = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDgzOTEzNCwiZXhwIjoyMDg2NDE1MTM0fQ.5BofQbMKiMLGFjqcIGaCwpoO9pLZnuLg7nojP0aGhJw")
    supabase = create_client(supabase_url, supabase_key)

initialize_clients()

sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
socketio_app = socketio.WSGIApp(sio)

# --- CALLBACKS ---
def on_data(wsapp, msg):
    if isinstance(msg, dict) and 'last_traded_price' in msg:
        token = str(msg.get('token')).strip()
        lp = str(msg.get('last_traded_price') / 100)
        r.set(f"price:{token}", lp)
        sio.emit('livePrice', {"tk": token, "lp": lp})

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("✅ Angel WebSocket Connected")

def on_error(wsapp, error):
    global is_ws_ready
    is_ws_ready = False
    print(f"❌ WS Error: {error}")

def on_close(wsapp):
    global is_ws_ready, active_subscriptions
    is_ws_ready = False
    active_subscriptions.clear() # Connection tootne par tracking clear
    print("🔌 Connection Closed. Reconnecting in 5s...")
    eventlet.sleep(5)
    login_to_angel()

def login_to_angel():
    global sws_instance, is_ws_ready
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
            sws_instance.on_error = on_error
            sws_instance.on_close = on_close
            
            eventlet.spawn(sws_instance.connect)
            print("🚀 Angel Login Successful")
    except Exception as e:
        print(f"❌ Login Failed: {e}")

def get_exchange_type(token):
    t = int(token)
    if t < 30000: return 1           
    if 35000 <= t <= 99999: return 1 
    if 100000 <= t <= 999999: return 5 
    return 2                         

# --- EVENTS ---
@sio.event
def connect(sid, environ):
    print(f"📱 User Connected: {sid}")
    if not is_ws_ready:
        login_to_angel()

@sio.event
def subscribe(sid, data):
    """Jab user symbol add karega, tabhi live token active hoga"""
    global sws_instance, is_ws_ready, active_subscriptions
    
    if data and sws_instance and is_ws_ready:
        def do_subscribe():
            # 1. Sirf wo tokens lein jo abhi tak subscribe nahi hue
            new_tokens = [str(t) for t in data if str(t) not in active_subscriptions]
            if not new_tokens: return

            # 2. Exchange wise grouping
            grouped = {}
            for t in new_tokens:
                etype = get_exchange_type(t)
                if etype not in grouped: grouped[etype] = []
                grouped[etype].append(t)

            # 3. 500-500 ke chunks mein subscription
            chunk_size = 500
            for etype, tokens in grouped.items():
                for i in range(0, len(tokens), chunk_size):
                    chunk = tokens[i:i + chunk_size]
                    try:
                        sws_instance.subscribe(f"req_{sid}_{i}", 3, [{"exchangeType": etype, "tokens": chunk}])
                        for t in chunk: active_subscriptions.add(t)
                        print(f"📡 Started live price for {len(chunk)} tokens")
                    except: pass
                    eventlet.sleep(0.2) # Rate limit protection

        eventlet.spawn(do_subscribe)

def app(environ, start_response):
    if environ.get('PATH_INFO') == '/':
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b"MYT Pro Backend Live"]
    return socketio_app(environ, start_response)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
