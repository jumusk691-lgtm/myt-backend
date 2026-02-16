import os
import pyotp
import requests
import pandas as pd
from datetime import datetime
from SmartApi import SmartConnect
# V3 ke bajaye stable version use kar rahe hain jo Render pe support hai
from SmartApi.smartConnect import SmartWebSocket 
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

# --- 2. SETUP ---
sio = socketio.Server(cors_allowed_origins='*')
app = socketio.WSGIApp(sio)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 3. SMART AUTO-UPDATE (Optimized for Lakhon Users) ---
def auto_update_new_symbols():
    print("Checking for NEW symbols today...")
    try:
        url = "https://margincalculator.angelbroking.com/OpenAPI_Standard/token/OpenAPIScripMaster.json"
        response = requests.get(url).json()
        df = pd.DataFrame(response)
        
        # Aaj ki date filter - Takki load kam ho
        aaj_ki_date = datetime.now().strftime('%d%b%Y').upper()
        df_new = df[df['expiry'] == aaj_ki_date] 
        
        if not df_new.empty:
            data_to_save = df_new.to_dict(orient='records')
            # Upsert logic - Sirf naye values add honge
            supabase.table("market_data").upsert(data_to_save).execute()
            print(f"Success: {len(data_to_save)} new symbols synced!")
    except Exception as e:
        print(f"Auto-update skip (File not ready or error): {e}")

# --- 4. ANGEL ONE LOGIN ---
obj = SmartConnect(api_key=API_KEY)
token = pyotp.TOTP(TOTP_KEY).now()
session = obj.generateSession(CLIENT_ID, PIN, token)

if session.get('status'):
    jwtToken = session['data']['jwtToken']
    feedToken = session['data']['feedToken']
    print("Angel One Login Success!")
else:
    print("Login Failed! Please check your keys.")
    exit()

# --- 5. LIVE BROADCAST (Lakhon Users Capability) ---
# Stable SmartWebSocket version
sws = SmartWebSocket(jwtToken, API_KEY, CLIENT_ID, feedToken)

def on_data(wsapp, msg):
    # Million users broadcast logic - Socket.io handle karega
    sio.emit('livePrice', msg)

def on_open(wsapp):
    print("WebSocket Connected! Broadcasting Live...")

sws.on_data = on_data
sws.on_open = on_open

# Background thread for websocket
eventlet.spawn(sws.connect)

# --- 6. SERVER START ---
if __name__ == '__main__':
    # Initial sync
    auto_update_new_symbols()
    
    port = int(os.environ.get('PORT', 5000))
    print(f"Broadcaster Live on Port {port}")
    eventlet.wsgi.server(eventlet.listen(('', port)), app)
