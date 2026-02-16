import eventlet
eventlet.monkey_patch()  # यह सबसे ऊपर होना अनिवार्य है

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
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA4MzkxMzQsImV4cCI6MjA4NjQxNTEzNH0.7h-9tI7FMMRA_4YACKyPctFxfcLbEYBlhmWXfVOIOKs"

# --- SERVER SETUP ---
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
app = socketio.WSGIApp(sio)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

sws_instance = None
subscribed_tokens = set()

@sio.event
def connect(sid, environ):
    print(f"✅ Client Connected: {sid}")

@sio.event
def subscribe(sid, data):
    global sws_instance, subscribed_tokens
    if data and sws_instance:
        # NSE/BSE के लिए (अक्सर 5 अंकों तक के टोकन)
        nse_tokens = [str(t) for t in data if int(t) < 50000]
        
        nse_fo = [str(t) for t in data if 35000 <= int(t) < 50000]
        
        mcx_tokens = [str(t) for t in data if int(t) >= 50000]

        if nse_tokens:
            token_list = [{"exchangeType": 1, "tokens": nse_tokens}] # 1 = NSE/BSE
            sws_instance.subscribe("myt_pro", 3, token_list)
            subscribed_tokens.update(nse_tokens)
            print(f"📡 Subscribed NSE: {nse_tokens}")

        if mcx_tokens:
            token_list = [{"exchangeType": 5, "tokens": mcx_tokens}] # 5 = MCX
            sws_instance.subscribe("myt_pro", 3, token_list)
            subscribed_tokens.update(mcx_tokens)
            print(f"📡 Subscribed MCX: {mcx_tokens}")

def start_web_socket(session_data):
    global sws_instance
    sws_instance = SmartWebSocketV2(session_data['jwt'], API_KEY, CLIENT_ID, session_data['feed'])
    
    def on_data(wsapp, msg):
        if 'last_traded_price' in msg:
            payload = {"tk": str(msg.get('token')), "lp": str(msg.get('last_traded_price') / 100)}
            sio.emit('livePrice', payload)

    sws_instance.on_data = on_data
    eventlet.spawn(sws_instance.connect)

# --- ANGEL LOGIN (GUNICORN COMPATIBLE) ---
# इसे 'if __name__ == "__main__":' से बाहर रखा है ताकि Gunicorn इसे रन करे
try:
    obj = SmartConnect(api_key=API_KEY)
    totp = pyotp.TOTP(TOTP_KEY).now()
    session = obj.generateSession(CLIENT_ID, PIN, totp)
    
    if session.get('status'):
        auth_data = {"jwt": session['data']['jwtToken'], "feed": session['data']['feedToken']}
        start_web_socket(auth_data)
        print("🚀 Angel Session Started for NSE/BSE/MCX")
    else:
        print("❌ Angel Login Failed")
except Exception as e:
    print(f"❌ Initial Login Error: {e}")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    import eventlet.wsgi
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
