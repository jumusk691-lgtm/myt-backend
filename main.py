import eventlet
# Render पर पोर्ट बाइंडिंग और नेटवर्किंग समस्याओं को रोकने के लिए इसे सबसे ऊपर रखना जरूरी है
eventlet.monkey_patch()

import os
import pyotp
import requests
import pandas as pd
from datetime import datetime
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
# async_mode='eventlet' का उपयोग यहाँ महत्वपूर्ण है
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet', logger=True, engineio_logger=True)
app = socketio.WSGIApp(sio)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

sws_instance = None
subscribed_tokens = set()

@sio.event
def connect(sid, environ):
    print(f"✅ Client Connected: {sid}")

@sio.event
def disconnect(sid):
    print(f"❌ Client Disconnected: {sid}")

@sio.event
def subscribe(sid, data):
    global sws_instance, subscribed_tokens
    if not data or not sws_instance: 
        print("⚠️ Subscription failed: Data empty or WebSocket not ready")
        return
    
    new_tokens = [str(t) for t in data if str(t) not in subscribed_tokens]
    if new_tokens:
        token_list = [{"exchangeType": 1, "tokens": [t]} for t in new_tokens]
        sws_instance.subscribe("myt_pro", 3, token_list)
        subscribed_tokens.update(new_tokens)
        print(f"📡 Now Streaming: {len(subscribed_tokens)} tokens")

def start_web_socket(session_data):
    global sws_instance
    sws_instance = SmartWebSocketV2(
        auth_token=session_data['jwt'], 
        api_key=API_KEY, 
        client_code=CLIENT_ID, 
        feed_token=session_data['feed']
    )

    def on_data(wsapp, msg):
        if 'last_traded_price' in msg:
            payload = {
                "tk": str(msg.get('token')), 
                "lp": str(msg.get('last_traded_price') / 100)
            }
            sio.emit('livePrice', payload)

    def on_open(ws):
        print("🚀 Angel One Socket Active and Ready")

    def on_error(ws, error):
        print(f"❌ WebSocket Error: {error}")

    sws_instance.on_data = on_data
    sws_instance.on_open = on_open
    sws_instance.on_error = on_error
    
    # WebSocket को एक अलग थ्रेड (Greenlet) में चलाने के लिए
    eventlet.spawn(sws_instance.connect)

# --- EXECUTION ---
if __name__ == '__main__':
    try:
        print("🔄 Logging into Angel One...")
        obj = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_KEY).now()
        session = obj.generateSession(CLIENT_ID, PIN, totp)
        
        if session.get('status'):
            print("✅ Angel One Login Successful")
            auth_data = {
                "jwt": session['data']['jwtToken'], 
                "feed": session['data']['feedToken']
            }
            start_web_socket(auth_data)
            
            # Render Fix: Port 0.0.0.0 पर बाइंड होना अनिवार्य है
            port = int(os.environ.get('PORT', 10000))
            print(f"🔥 Starting server on 0.0.0.0:{port}")
            
            # Listener और WSGI Server को सही तरीके से शुरू करना
            listener = eventlet.listen(('0.0.0.0', port))
            eventlet.wsgi.server(listener, app, log_output=True)
        else:
            print(f"❌ Angel Login Failed: {session.get('message')}")
    except Exception as e:
        print(f"❌ Critical Error: {e}")
 
