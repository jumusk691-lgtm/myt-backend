import os
import pyotp
import requests
import pandas as pd
import time
from datetime import datetime
from SmartApi import SmartConnect
# Import ka sabse robust tarika taaki Render fail na ho
import SmartApi.smartConnect as smartConnect
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
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            
            # Smart Filter: Sirf aaj ki expiry ke naye symbols
            aaj = datetime.now().strftime('%d%b%Y').upper()
            df_new = df[df['expiry'] == aaj]
            
            if not df_new.empty:
                print(f"Found {len(df_new)} new symbols for today.")
                records = df_new.to_dict(orient='records')
                # Bulk update in Supabase
                supabase.table("market_data").upsert(records).execute()
                print("Supabase Market Data Updated Successfully!")
            else:
                print("No new symbols found for today's filter.")
        else:
            print("Failed to fetch symbols from Angel Server.")
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
    print("--- STEP 3: Starting Live Broadcast Engine ---")
    # Using the most stable SmartWebSocket class
    sws = smartConnect.SmartWebSocket(
        session_data['jwt'], 
        API_KEY, 
        CLIENT_ID, 
        session_data['feed']
    )

    def on_data(wsapp, msg):
        # YAHAN SE MILLION USERS KO DATA JAYEGA
        # msg mein live price, volume, etc. hota hai
        sio.emit('livePrice', msg)

    def on_open(wsapp):
        print("WEB-SOCKET CONNECTED: Broadcasting live data to all users...")

    def on_error(wsapp, error):
        print(f"Websocket Error: {error}")

    sws.on_data = on_data
    sws.on_open = on_open
    sws.on_error = on_error
    
    # Run websocket in background
    eventlet.spawn(sws.connect)

# --- 6. EXECUTION & SERVER START ---
if __name__ == '__main__':
    # Run the full logic sequence
    update_symbols_logic()
    
    auth_data = get_angel_session()
    if auth_data:
        start_web_socket(auth_data)
        
        # Start the final Socket.io server
        port = int(os.environ.get('PORT', 5000))
        print(f"--- SERVER LIVE ON PORT {port} ---")
        eventlet.wsgi.server(eventlet.listen(('', port)), app)
    else:
        print("Critical Error: Could not start server without Login.")
