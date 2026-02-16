import eventlet
eventlet.monkey_patch()  # इसे सबसे ऊपर ही रखें

import os
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import socketio
import eventlet.wsgi
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
        new_tokens = [str(t) for t in data if str(t) not in subscribed_tokens]
        if new_tokens:
            token_list = [{"exchangeType": 1, "tokens": [t]} for t in new_tokens]
            sws_instance.subscribe("myt_pro", 3, token_list)
            subscribed_tokens.update(new_tokens)
            print(f"📡 Subscribed to: {new_tokens}")

def start_web_socket(session_data):
    global sws_instance
    sws_instance = SmartWebSocketV2(session_data['jwt'], API_KEY, CLIENT_ID, session_data['feed'])

    def on_data(wsapp, msg):
        if 'last_traded_price' in msg:
            payload = {"tk": str(msg.get('token')), "lp": str(msg.get('last_traded_price') / 100)}
            sio.emit('livePrice', payload)

    sws_instance.on_data = on_data
    eventlet.spawn(sws_instance.connect)

# --- LOGIN & RUN ---
if __name__ == '__main__':
    try:
        obj = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_KEY).now()
        session = obj.generateSession(CLIENT_ID, PIN, totp)
        
        if session.get('status'):
            auth_data = {"jwt": session['data']['jwtToken'], "feed": session['data']['feedToken']}
            start_web_socket(auth_data)
            
            # RENDER FIX: पोर्ट को एनवायरनमेंट से उठाना और 0.0.0.0 पर बाइंड करना
            port = int(os.environ.get('PORT', 10000))
            print(f"🚀 Server starting on port {port}...")
            
            # महत्वपूर्ण: log_output को True रखें ताकि आप Render logs देख सकें
            eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
        else:
            print("❌ Angel Login Failed")
    except Exception as e:
        print(f"❌ Error: {e}")
