import eventlet
# Sabse pehle monkey_patch zaroori hai
eventlet.monkey_patch()

import os
import pyotp
import socketio
import threading
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client, Client

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
    return [b"MYT Pro Backend is Running!"]

def app(environ, start_response):
    if environ.get('PATH_INFO') == '/':
        return index(environ, start_response)
    return socketio_app(environ, start_response)

sws_instance = None

# --- DB UPDATE HELPER (Non-blocking) ---
def update_db_async(token, lp):
    try:
        supabase.table("market_data").update({"last_price": lp}).filter("token", "ilike", f"%{token}").execute()
    except Exception as e:
        print(f"❌ DB Update Error: {e}")

# --- ANGEL WEBSOCKET HANDLER ---
def on_data(wsapp, msg):
    if isinstance(msg, dict) and 'last_traded_price' in msg:
        token = str(msg.get('token')).strip()
        lp = str(msg.get('last_traded_price') / 100)
        
        # Immediate data emission to APK
        sio.emit('livePrice', {"tk": token, "lp": lp})
        
        # Background DB update
        eventlet.spawn(update_db_async, token, lp)

def on_open(wsapp):
    print("✅ Angel WebSocket Connected Successfully")

# --- ANGEL LOGIN (Fixing Blocking Error) ---
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
            
            # Using threading to avoid 'Blocking functions from mainloop' error
            threading.Thread(target=sws_instance.connect, daemon=True).start()
            print("🚀 Angel Session Live in Background Thread")
    except Exception as e:
        print(f"❌ Login Error: {e}")

# --- SMART EXCHANGE MAPPING ---
def get_exchange_type(token):
    # Yeh function token ke hisaab se exchange decide karega
    t = int(token)
    if t < 20000: return 1      # NSE Cash
    if 35000 <= t <= 75000: return 1 # Nifty/BankNifty
    if 100000 <= t <= 999999: return 5 # MCX (Natural Gas, etc.)
    if t > 1000000: return 2    # NSE Derivatives
    return 1 # Default NSE

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
        print(f"📡 Subscribing to: {data}")
        
        # Grouping tokens by exchange
        subs = {}
        for t in data:
            etype = get_exchange_type(t)
            if etype not in subs: subs[etype] = []
            subs[etype].append(str(t))
        
        for etype, tokens in subs.items():
            subscription_list = [{"exchangeType": etype, "tokens": tokens}]
            eventlet.spawn(sws_instance.subscribe, "myt_pro_feed", 3, subscription_list)

if __name__ == '__main__':
    import eventlet.wsgi
    # Render port configuration
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', 10000)), app)
