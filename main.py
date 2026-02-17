import eventlet
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
# ZAROORI: Yahan 'service_role' key hi dalna
SUPABASE_KEY = "sb_secret_wi99i_tvE_2DT5IK80PyYg_6nJSeZdn"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')

# Home Route: Isse 404 error khatam ho jayega
def index(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/plain')])
    return [b"MYT Backend is Running Live!"]

app = socketio.WSGIApp(sio, static_files={'/': index})

sws_instance = None

def on_data(wsapp, msg):
    if isinstance(msg, dict) and 'last_traded_price' in msg:
        token = str(msg.get('token')).strip()
        lp = str(msg.get('last_traded_price') / 100)
        sio.emit('livePrice', {"tk": token, "lp": lp})
        try:
            supabase.table("market_data").update({"last_price": lp}).filter("token", "ilike", f"%{token}").execute()
        except Exception as e:
            print(f"❌ DB Update Error: {e}")

def login_to_angel():
    global sws_instance
    try:
        obj = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_KEY).now()
        session = obj.generateSession(CLIENT_ID, PIN, totp)
        if session.get('status'):
            sws_instance = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_ID, session['data']['feedToken'])
            sws_instance.on_data = on_data
            sws_instance.on_open = lambda ws: print("✅ Angel WebSocket Connected")
            eventlet.spawn(sws_instance.connect)
            print("🚀 Angel Session Live")
    except Exception as e:
        print(f"❌ Login Error: {e}")

@sio.event
def connect(sid, environ):
    print(f"Connected: {sid}")
    eventlet.spawn(login_to_angel)

if __name__ == '__main__':
    import eventlet.wsgi
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', 10000)), app)
