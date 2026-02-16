import os
import pyotp
import requests
import pandas as pd
import time
from datetime import datetime
from SmartApi import SmartConnect
# V2 ke liye sahi import path
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
sio = socketio.Server(cors_allowed_origins='*')
app = socketio.WSGIApp(sio)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 3. LOGIC: SYMBOL MANAGEMENT ---
def update_symbols_logic():
    print("--- STEP 1: Starting Symbol Update Logic ---")
    try:
        url = "https://margincalculator.angelbroking.com/OpenAPI_Standard/token/OpenAPIScripMaster.json"
        response = requests.get(url, timeout=30) 
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            
            aaj = datetime.now().strftime('%d%b%Y').upper()
            df_new = df[df['expiry'] == aaj]
            
            if not df_new.empty:
                print(f"Found {len(df_new)} new symbols.")
                records = df_new.to_dict(orient='records')
                supabase.table("market_data").upsert(records).execute()
                print("Supabase Updated!")
            else:
                print("No new symbols for today.")
    except Exception as e:
        print(f"Symbol Update Error: {str(e)}")

# --- 4. LOGIC: AUTHENTICATION ---
def get_angel_session():
    print("--- STEP 2: Authenticating with Angel One ---")
    try:
        obj = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_KEY).now()
        session = obj.generateSession(CLIENT_ID, PIN, totp)
        
        if session.get('status'):
            print("Login Successful!")
            return {
                "jwt": session['data']['jwtToken'],
                "feed": session['data']['feedToken'],
                "obj": obj
            }
        else:
            print(f"Login Failed: {session.get('message')}")
            return None
    except Exception as e:
        print(f"Auth Error: {str(e)}")
        return None

# --- 5. LOGIC: LIVE DATA BROADCAST ---
def start_web_socket(session_data):
    print("--- STEP 3: Starting Live Broadcast Engine (V2) ---")
    
    # FIX: V2 mein auth_token (JWT) pehle aata hai aur parameters ka order fix hai
    sws = SmartWebSocketV2(
        auth_token=session_data['jwt'],
        api_key=API_KEY,
        client_code=CLIENT_ID,
        feed_token=session_data['feed']
    )

    def on_data(wsapp, msg):
        # Socket.io ke zariye frontend ko data bhejna
        sio.emit('livePrice', msg)

    def on_open(wsapp):
        print("WEB-SOCKET V2 CONNECTED!")
        # Data subscribe karne ke liye (Nifty example)
        correlation_id = "myt_stream_01"
        action = 1 # 1 for Subscribe
        mode = 3   # 3 for Full Mode (LTP, Volume, etc.)
        tokens = [{"exchangeType": 1, "tokens": ["10626"]}] # 10626 = Nifty 50
        sws.subscribe(correlation_id, mode, tokens)

    def on_error(wsapp, error):
        print(f"Websocket Error: {error}")

    def on_close(wsapp):
        print("Websocket Closed")

    # Callbacks bind karna
    sws.on_data = on_data
    sws.on_open = on_open
    sws.on_error = on_error
    sws.on_close = on_close
    
    # Background thread mein run karna
    eventlet.spawn(sws.connect)

# --- 6. EXECUTION ---
if __name__ == '__main__':
    # Symbols update karein
    update_symbols_logic()
    
    # Login karein
    auth_data = get_angel_session()
    
    if auth_data:
        # WebSocket start karein
        start_web_socket(auth_data)
        
        # Render/Server start
        port = int(os.environ.get('PORT', 5000))
        print(f"--- SERVER LIVE ON PORT {port} ---")
        eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
    else:
        print("Critical Error: Login failed. Server not started.")
