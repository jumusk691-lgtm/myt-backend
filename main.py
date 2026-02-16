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

# --- 2. SERVER & DATABASE SETUP ---
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
app = socketio.WSGIApp(sio)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

sws_instance = None
subscribed_tokens = set() # Duplicate check ke liye

# --- 3. LOGIC: SYMBOL UPDATE (Daily Fix) ---
def update_symbols_to_supabase():
    print("--- STEP 1: Syncing Market Symbols ---")
    try:
        url = "https://margincalculator.angelbroking.com/OpenAPI_Standard/token/OpenAPIScripMaster.json"
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            # Aaj ki expiry ya equity filter (NSE only for now)
            # Aap chahen toh filter hata sakte hain agar saare symbols chahiye
            records = df.to_dict(orient='records')
            # Supabase upsert logic (Sahi format mein)
            # Note: Table mein 'symbol' aur 'token' columns hone chahiye
            print(f"Total symbols fetched: {len(records)}. Syncing...")
            # Chote batches mein bhejte hain taaki timeout na ho
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                supabase.table("market_data").upsert(records[i:i+batch_size]).execute()
            print("✅ Supabase Sync Complete!")
    except Exception as e:
        print(f"❌ Symbol Sync Error: {e}")

# --- 4. LOGIC: LIVE DATA BROADCAST ---
@sio.event
def connect(sid, environ):
    print(f"✅ User Connected: {sid}")

@sio.event
def subscribe(sid, data):
    global sws_instance, subscribed_tokens
    # data format from Android: ["token1", "token2"]
    if not data: return
    
    new_tokens = [str(t) for t in data if str(t) not in subscribed_tokens]
    
    if new_tokens and sws_instance:
        correlation_id = "myt_pro_stream"
        action = 1 # Subscribe
        mode = 3   # Full Mode (LTP, etc.)
        
        # Mapping for Angel One
        token_list = [{"exchangeType": 1, "tokens": [t]} for t in new_tokens]
        sws_instance.subscribe(correlation_id, mode, token_list)
        subscribed_tokens.update(new_tokens)
        print(f"📡 Now Streaming: {len(subscribed_tokens)} tokens")

def start_web_socket(session_data):
    global sws_instance
    print("--- STEP 3: Initializing WebSocket ---")
    
    sws_instance = SmartWebSocketV2(
        auth_token=session_data['jwt'],
        api_key=API_KEY,
        client_code=CLIENT_ID,
        feed_token=session_data['feed']
    )

    def on_data(wsapp, msg):
        # Android format: tk (token), lp (last price)
        if 'last_traded_price' in msg:
            payload = {
                "tk": str(msg.get('token')),
                "lp": str(msg.get('last_traded_price') / 100)
            }
            sio.emit('livePrice', payload)

    def on_open(wsapp):
        print("🚀 Angel One WebSocket Connected & Ready")

    def on_error(wsapp, error):
        print(f"⚠️ WebSocket Error: {error}")

    def on_close(wsapp):
        print("🔴 WebSocket Closed")

    sws_instance.on_data = on_data
    sws_instance.on_open = on_open
    sws_instance.on_error = on_error
    sws_instance.on_close = on_close
    
    eventlet.spawn(sws_instance.connect)

# --- 5. EXECUTION ---
if __name__ == '__main__':
    # 1. Sabse pehle symbols update karein
    update_symbols_to_supabase()
    
    # 2. Login
    try:
        obj = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_KEY).now()
        session = obj.generateSession(CLIENT_ID, PIN, totp)
        
        if session.get('status'):
            print("✅ Login Successful!")
            auth_data = {
                "jwt": session['data']['jwtToken'],
                "feed": session['data']['feedToken']
            }
            
            # 3. WebSocket start karein
            start_web_socket(auth_data)
            
            # 4. Server Start
            port = int(os.environ.get('PORT', 5000))
            print(f"🔥 SERVER LIVE ON PORT {port}")
            eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
        else:
            print(f"❌ Login Failed: {session.get('message')}")
    except Exception as e:
        print(f"❌ Startup Error: {e}")
