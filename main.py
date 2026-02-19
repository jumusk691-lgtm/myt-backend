import eventlet
eventlet.monkey_patch(all=True)

import os
import pyotp
import socketio
import redis
import time
from datetime import datetime
from supabase import create_client
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- CONFIGURATION (Directly Mixed) ---
# Maine aapka naya Redis URL aur Nayi Supabase Key yahan dal di hai
REDIS_URL = "redis://default:AR6NAAImcDE4YTZjMDM1ZTkzMDc0ZmJiOTM5YzhjZGI2OWY3MDA5ZXAxNzgyMQ@happy-moth-7821.upstash.io:6379"
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "XzoayMr3B5UOVjKGNjjUxU07ZOCFnjPSfe7xO2Gt1OmORLEemCvILjG5O4damYqTT3quUDGmMvgcC+i5FEhthQ=="

# --- GLOBAL STATE ---
sws_instance = None
is_ws_ready = False 
r = None
supabase = None
active_subscriptions = set()

def initialize_clients():
    global r, supabase
    try:
        # Redis connection setup
        r = redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5)
        print("✅ Redis Connected Successfully")
        
        # Supabase client setup with NEW SERVICE ROLE KEY
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Supabase Client Initialized with New Key")
    except Exception as e:
        print(f"❌ Initialization Error: {e}")

initialize_clients()

sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
socketio_app = socketio.WSGIApp(sio)

# --- DATABASE CLEANUP ---
def cleanup_expired_contracts():
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        print(f"🧹 Running Cleanup for: {today}")
        # Purane contracts delete karne ke liye
        supabase.table("watchlist_items").delete().lt("expiry", today).execute()
        print("✅ Database Cleanup Success")
    except Exception as e:
        print(f"⚠️ Cleanup note: {e}")

# --- HEARTBEAT ---
def start_heartbeat():
    global is_ws_ready, sws_instance
    while True:
        if is_ws_ready and sws_instance:
            try:
                # Nifty Pulse to keep connection alive
                sws_instance.subscribe("hb_pulse", 1, [{"exchangeType": 1, "tokens": ["26000"]}])
            except:
                pass
        eventlet.sleep(30)

# --- CALLBACKS ---
def on_data(wsapp, msg):
    if isinstance(msg, dict):
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp')
        token = msg.get('token')
        
        if ltp_raw is not None and token:
            try:
                # Price conversion (Paisa to Rupees)
                lp = str(round(float(ltp_raw) / 100, 2))
                token_str = str(token).strip()
                
                # Update Redis and Push to Frontend
                r.set(f"price:{token_str}", lp)
                sio.emit('livePrice', {"tk": token_str, "lp": lp})
            except Exception as e:
                pass 

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("✅ Angel WebSocket Connected Successfully")

def on_error(wsapp, error):
    print(f"❌ WS Error: {error}")

def on_close(wsapp, status=None, msg=None):
    global is_ws_ready, active_subscriptions
    is_ws_ready = False
    active_subscriptions.clear()
    print("🔌 Connection Closed. Reconnecting in 10s...")
    eventlet.sleep(10)
    login_to_angel()

def login_to_angel():
    global sws_instance
    try:
        # Angel One Credentials
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
        print(f"❌ Angel Login Failed: {e}")

@sio.event
def subscribe(sid, data):
    if data and sws_instance and is_ws_ready:
        tokens = data if isinstance(data, list) else [data]
        # Auto-detect exchange (NSE=1, NFO=2)
        sws_instance.subscribe(f"sub_{sid}", 1, [{"exchangeType": 1, "tokens": tokens}])

def app(environ, start_response):
    if environ.get('PATH_INFO') == '/':
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b"PULSE BACKEND IS RUNNING"]
    return socketio_app(environ, start_response)

if __name__ == '__main__':
    cleanup_expired_contracts() # Start with cleanup
    eventlet.spawn(start_heartbeat) # Run heartbeat in background
    login_to_angel()
    
    port = int(os.environ.get("PORT", 10000))
    print(f"📡 Server starting on port {port}")
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
