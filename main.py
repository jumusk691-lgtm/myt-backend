import os
import pyotp
import requests
import pandas as pd
from datetime import datetime
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import socketio
import eventlet
import eventlet.wsgi
from supabase import create_client, Client

# --- 1. CONFIGURATION ---
API_KEY = "85HE4VA1"
CLIENT_ID = "S52638556"
PIN = "0000" 
TOTP_KEY = "XFTXZ2445N4V2UMB7EWUCBDRMU"

SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA4MzkxMzQsImV4cCI6MjA4NjQxNTEzNH0.7h-9tI7FMMRA_4YACKyPctFxfcLbEYBlhmWXfVOIOKs"

# --- 2. SERVER SETUP (High Capacity Config) ---
# 'eventlet' use kar rahe hain jo hazaro concurrent connections handle kar sakta hai
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
app = socketio.WSGIApp(sio)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

sws_instance = None
subscribed_tokens = set() # Duplicate requests rokne ke liye

# --- 3. LIVE DATA BROADCAST (The Scalable Part) ---
@sio.event
def connect(sid, environ):
    print(f"✅ New User Connected: {sid}")

@sio.event
def subscribe(sid, data):
    global sws_instance, subscribed_tokens
    # data: ["token1", "token2"]
    new_to_subscribe = [str(t) for t in data if str(t) not in subscribed_tokens]
    
    if new_to_subscribe and sws_instance:
        correlation_id = "myt_broadcast"
        # Sabhi users ke liye common subscription (Broadcasting)
        token_list = [{"exchangeType": 1, "tokens": [t]} for t in new_to_subscribe]
        sws_instance.subscribe(correlation_id, 3, token_list)
        subscribed_tokens.update(new_to_subscribe)
        print(f"📡 Now Broadcasting {len(subscribed_tokens)} tokens")

def start_web_socket(session_data):
    global sws_instance
    sws_instance = SmartWebSocketV2(
        auth_token=session_data['jwt'],
        api_key=API_KEY,
        client_code=CLIENT_ID,
        feed_token=session_data['feed']
    )

    def on_data(wsapp, msg):
        # Ye part har user ko data bhejta hai
        if 'last_traded_price' in msg:
            payload = {
                "tk": str(msg.get('token')),
                "lp": str(msg.get('last_traded_price') / 100)
            }
            # Broadcaster: Ek hi baar bhejta hai, socket.io khud handle karta hai sabko distribute karna
            sio.emit('livePrice', payload)

    sws_instance.on_data = on_data
    sws_instance.on_open = lambda ws: print("🚀 Angel One WebSocket Active")
    eventlet.spawn(sws_instance.connect)

# --- 4. AUTH & RUN ---
if __name__ == '__main__':
    # Login Logic
    try:
        obj = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_KEY).now()
        session = obj.generateSession(CLIENT_ID, PIN, totp)
        
        if session.get('status'):
            auth_data = {"jwt": session['data']['jwtToken'], "feed": session['data']['feedToken']}
            start_web_socket(auth_data)
            
            port = int(os.environ.get('PORT', 5000))
            print(f"🔥 PRO-SERVER LIVE ON PORT {port}")
            eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
    except Exception as e:
        print(f"❌ Critical Error: {e}")
