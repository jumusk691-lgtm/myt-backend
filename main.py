import eventlet
# Sabse pehle patching zaroori hai
eventlet.monkey_patch()

import os
import pyotp
import socketio
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

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- SERVER SETUP ---
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
# SocketIO ki main WSGI app
socketio_app = socketio.WSGIApp(sio)

# Home Route function: Isse 500/404 error nahi aayega
def index(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/plain')])
    return [b"MYT Backend is Running Live! Socket.io is active."]

# FIX: Custom Middleware taaki 'static_files' wala AttributeError na aaye
def app(environ, start_response):
    if environ.get('PATH_INFO') == '/':
        return index(environ, start_response)
    return socketio_app(environ, start_response)

sws_instance = None

# --- ANGEL WEBSOCKET HANDLER ---
def on_data(wsapp, msg):
    if isinstance(msg, dict) and 'last_traded_price' in msg:
        token = str(msg.get('token')).strip()
        lp = str(msg.get('last_traded_price') / 100)
        
        # Android App ko data bhej raha hai
        sio.emit('livePrice', {"tk": token, "lp": lp})
        
        # Database update logic
        try:
            supabase.table("market_data").update({"last_price": lp}).filter("token", "ilike", f"%{token}").execute()
        except Exception as e:
            print(f"❌ DB Update Error: {e}")

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
            sws_instance.on_open = lambda ws: print("✅ Angel WebSocket Connected")
            eventlet.spawn(sws_instance.connect)
            print("🚀 Angel Session Live")
    except Exception as e:
        print(f"❌ Login Error: {e}")

# --- SOCKET.IO EVENTS ---
@sio.event
def connect(sid, environ):
    print(f"📱 Android Client Connected: {sid}")
    if sws_instance is None:
        eventlet.spawn(login_to_angel)

@sio.event
def subscribe(sid, data):
    global sws_instance
    if data and sws_instance:
        print(f"📡 Subscribing to: {data}")
        subscription_list = [{"exchangeType": 1, "tokens": [str(t) for t in data]}]
        sws_instance.subscribe("myt_pro", 3, subscription_list)

if __name__ == '__main__':
    import eventlet.wsgi
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', 10000)), app)
