import os
import pyotp
import requests
import pandas as pd
from SmartApi import SmartConnect
from supabase import create_client, Client
import socketio
import eventlet
import eventlet.wsgi

# --- 1. CONFIGURATION (Apni details yahan bharein) ---
API_KEY = "85HE4VA1"
CLIENT_ID = "S52638556"
PIN = "0000"  # Apna 4-digit PIN yahan likhein
TOTP_KEY = "XFTXZ2445N4V2UMB7EWUCBDRMU"

SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA4MzkxMzQsImV4cCI6MjA4NjQxNTEzNH0.7h-9tI7FMMRA_4YACKyPctFxfcLbEYBlhmWXfVOIOKs"

# --- 2. SETUP ---
sio = socketio.Server(cors_allowed_origins='*')
app = socketio.WSGIApp(sio)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 3. SYMBOL UPDATE LOGIC (Sirf naye symbols ke liye) ---
def update_symbols_to_supabase():
    print("Checking for new symbols...")
    url = "https://margincalculator.angelbroking.com/OpenAPI_Standard/token/OpenAPIScripMaster.json"
    response = requests.get(url).json()
    
    # Filter: Maan lo aapko sirf Nifty/BankNifty ke symbols chahiye
    # Isse poori file download hone ke baad bhi data chota ho jayega
    df = pd.DataFrame(response)
    df = df[df['name'].isin(['NIFTY', 'BANKNIFTY'])] 
    
    # Supabase mein data bhej raha hai (Upsert = Update if exists)
    data_to_save = df.to_dict(orient='records')
    supabase.table("symbols").upsert(data_to_save).execute()
    print("Symbols Updated in Supabase!")

# --- 4. ANGEL ONE LOGIN & LIVE STREAM ---
obj = SmartConnect(api_key=API_KEY)
token = pyotp.TOTP(TOTP_KEY).now()
session = obj.generateSession(CLIENT_ID, PIN, token)

print("Angel One Login Success!")

# Jab Angel One se naya price aaye
def on_data(wsapp, msg):
    # Ye line lakhon users ko broadcast karegi
    sio.emit('livePrice', msg)

sws = obj.smartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_ID, session['data']['feedToken'])
sws.on_data = on_data

# Background mein stream shuru karein
eventlet.spawn(sws.connect)

# --- 5. SERVER START ---
if __name__ == '__main__':
    # Pehle symbols update karein
    try:
        update_symbols_to_supabase()
    except Exception as e:
        print(f"Symbol update error: {e}")

    port = int(os.environ.get('PORT', 5000))
    print(f"Broadcaster Server Live on Port {port}")
    eventlet.wsgi.server(eventlet.listen(('', port)), app)
