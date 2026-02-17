import eventlet
# Patching for eventlet
eventlet.monkey_patch(socket=True, select=True, thread=True)

import os
import pyotp
import socketio
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client, Client

# --- ANGEL ONE CONFIG ---
API_KEY = "85HE4VA1"
CLIENT_ID = "S52638556"
PIN = "0000" 
TOTP_KEY = "XFTXZ2445N4V2UMB7EWUCBDRMU"

# --- SUPABASE CONFIG ---
SUPABASE_URL ="https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "sb_secret_wi99i_tvE_2DT5IK80PyYg_6nJSeZdn"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- SERVER SETUP ---
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
# Ye 'app' variable hi Vercel use karega
app = socketio.WSGIApp(sio)

sws_instance = None

# --- WEBSOCKET DATA HANDLER ---
def on_data(wsapp, msg):
    if isinstance(msg, dict) and 'last_traded_price' in msg:
        token = str(msg.get('token')).strip()
        lp = str(msg.get('last_traded_price') / 100)
        
        sio.emit('livePrice', {"tk": token, "lp": lp})
        
        try:
            supabase.table("market_data").update({"last_price": lp}).filter("token", "ilike", f"%{token}").execute()
        except Exception as e:
            print(f"❌ DB Update Error: {e}")

def on_connect(wsapp):
    print("✅ Angel WebSocket Connected!")

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
def login_to_angel():
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
        print(f"❌ Login Error: {e}")

# --- SOCKET.IO EVENTS ---
@sio.event
def connect(sid, environ):
    print(f"Connected: {sid}")
    # Login process start on first connection
    eventlet.spawn(login_to_angel)

@sio.event
def subscribe(sid, data):
    global sws_instance
    if data and sws_instance:
        subscription_list = []
        nse_cash = [str(t) for t in data if int(str(t)[-5:]) < 30000]
        nse_fo = [str(t) for t in data if 30000 <= int(str(t)[-5:]) < 50000]
        mcx = [str(t) for t in data if int(str(t)[-5:]) >= 50000]

        if nse_cash: subscription_list.append({"exchangeType": 1, "tokens": nse_cash})
        if nse_fo: subscription_list.append({"exchangeType": 2, "tokens": nse_fo})
        if mcx: subscription_list.append({"exchangeType": 5, "tokens": mcx})

        if subscription_list:
            sws_instance.subscribe("myt_pro", 3, subscription_list)
            print(f"📡 Subscribed: {subscription_list}")

# Vercel doesn't use the __main__ block for serving.
# But keeping it for local testing.
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    import eventlet.wsgi
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
