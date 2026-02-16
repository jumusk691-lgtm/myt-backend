import eventlet
eventlet.monkey_patch()  # सबसे ऊपर होना अनिवार्य है

import os
import pyotp
import socketio
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- CONFIG ---
API_KEY = "85HE4VA1"
CLIENT_ID = "S52638556"
PIN = "0000" 
TOTP_KEY = "XFTXZ2445N4V2UMB7EWUCBDRMU"

# --- SERVER SETUP ---
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
app = socketio.WSGIApp(sio)

sws_instance = None

# --- WEBOCKET DATA HANDLER ---
def on_data(wsapp, msg):
    if 'last_traded_price' in msg:
        # LTP को 100 से भाग देकर सही भाव निकालना
        payload = {
            "tk": str(msg.get('token')), 
            "lp": str(msg.get('last_traded_price') / 100)
        }
        sio.emit('livePrice', payload)

def on_connect(wsapp):
    print("✅ Angel WebSocket Connected Successfully!")

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
        # Background thread में चलाने के लिए eventlet.spawn
        eventlet.spawn(sws_instance.connect)
    except Exception as e:
        print(f"❌ WebSocket Startup Error: {e}")

# --- ANGEL LOGIN (GUNICORN COMPATIBLE) ---
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
        print("🚀 Angel Session Started for NSE/F&O/MCX")
    else:
        print(f"❌ Angel Login Failed: {session.get('message')}")
except Exception as e:
    print(f"❌ Critical Login Error: {e}")

# --- SOCKET.IO EVENTS ---
@sio.event
def connect(sid, environ):
    print(f"📱 Client Connected to Server: {sid}")

@sio.event
def subscribe(sid, data):
    global sws_instance
    # Check if data is valid and websocket is alive
    if data and sws_instance and sws_instance.is_connected():
        # टोकन के आधार पर एक्सचेंज बांटना
        nse_cash = [str(t) for t in data if int(t) < 30000]
        nse_fo = [str(t) for t in data if 30000 <= int(t) < 50000]
        mcx = [str(t) for t in data if int(t) >= 50000]

        subscription_list = []
        
        if nse_cash:
            subscription_list.append({"exchangeType": 1, "tokens": nse_cash})
        if nse_fo:
            subscription_list.append({"exchangeType": 2, "tokens": nse_fo})
        if mcx:
            subscription_list.append({"exchangeType": 5, "tokens": mcx})

        if subscription_list:
            sws_instance.subscribe("myt_pro", 3, subscription_list)
            print(f"📡 Subscribed: {subscription_list}")
    else:
        print("⚠️ Sub failed: WebSocket not connected or empty data")

# --- SERVER RUN ---
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    import eventlet.wsgi
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
