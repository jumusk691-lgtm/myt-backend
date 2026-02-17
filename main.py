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

# --- SUPABASE CONFIG ---
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
# Yahan apni "Service Role Key" ya "Anon Key" dalo
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA4MzkxMzQsImV4cCI6MjA4NjQxNTEzNH0.7h-9tI7FMMRA_4YACKyPctFxfcLbEYBlhmWXfVOIOKs"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- SERVER SETUP ---
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
app = socketio.WSGIApp(sio)

sws_instance = None

# --- WEBSOCKET DATA HANDLER ---
def on_data(wsapp, msg):
    if isinstance(msg, dict) and 'last_traded_price' in msg:
        token = str(msg.get('token'))
        lp = str(msg.get('last_traded_price') / 100)
        
        # 1. Socket.io se App ko bhejo (Mobile Live)
        payload = {"tk": token, "lp": lp}
        sio.emit('livePrice', payload)
        
        # 2. Supabase DB mein update karo (Permanent Storage)
        try:
            supabase.table("market_data").update({"last_price": lp}).eq("token", token).execute()
        except Exception as e:
            print(f"DB Update Error: {e}")

def on_connect(wsapp):
    print("✅ Angel WebSocket Connected & DB Sync Ready!")

# --- START WEBSOCKET ---
def start_web_socket(session_data):
    global sws_instance
    try:
        sws_instance = SmartWebSocketV2(
            session_data['jwt'], 
            API_KEY, 
            CLIENT_ID, 
            session_data['feed']
        )
        sws_instance.on_data = on_data
        sws_instance.on_open = on_connect
        eventlet.spawn(sws_instance.connect)
    except Exception as e:
        print(f"❌ WebSocket Startup Error: {e}")

# --- ANGEL LOGIN ---
try:
    obj = SmartConnect(api_key=API_KEY)
    totp = pyotp.TOTP(TOTP_KEY).now()
    session = obj.generateSession(CLIENT_ID, PIN, totp)
    
    if session.get('status'):
        auth_data = {
            "jwt": session['data']['jwtToken'], 
            "feed": session['data']['feedToken']
        }
        start_web_socket(auth_data)
        print("🚀 Angel Session Live")
    else:
        print(f"❌ Login Failed: {session.get('message')}")
except Exception as e:
    print(f"❌ Critical Error: {e}")

# --- SOCKET.IO EVENTS ---
@sio.event
def subscribe(sid, data):
    global sws_instance
    # Fixed: is_connected() hata diya hai taaki crash na ho
    if data and sws_instance:
        subscription_list = []
        # Token ranges for NSE, NFO, MCX
        nse_cash = [str(t) for t in data if int(t) < 30000]
        nse_fo = [str(t) for t in data if 30000 <= int(t) < 50000]
        mcx = [str(t) for t in data if int(t) >= 50000]

        if nse_cash: subscription_list.append({"exchangeType": 1, "tokens": nse_cash})
        if nse_fo: subscription_list.append({"exchangeType": 2, "tokens": nse_fo})
        if mcx: subscription_list.append({"exchangeType": 5, "tokens": mcx})

        if subscription_list:
            sws_instance.subscribe("myt_pro", 3, subscription_list)
            print(f"📡 Subscribed: {subscription_list}")

@sio.on('/')
def health(sid):
    return "OK"

# --- RUN ---
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    import eventlet.wsgi
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
