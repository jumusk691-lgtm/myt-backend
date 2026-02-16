import os
import pyotp
import requests
import pandas as pd
from SmartApi import SmartConnect
from supabase import create_client, Client
import socketio
import eventlet
import eventlet.wsgi

# --- 1. CONFIGURATION (Aapki details safe hain) ---
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

# --- 3. SYMBOL UPDATE LOGIC ---
def update_symbols_to_supabase():
    print("Checking for new symbols...")
    url = "https://margincalculator.angelbroking.com/OpenAPI_Standard/token/OpenAPIScripMaster.json"
    response = requests.get(url).json()
    
    df = pd.DataFrame(response)
    # Sirf Nifty aur BankNifty filter kar rahe hain taaki load kam ho
    df = df[df['name'].isin(['NIFTY', 'BANKNIFTY'])] 
    
    data_to_save = df.to_dict(orient='records')
    # Supabase table ka naam 'symbols' hona chahiye
    supabase.table("symbols").upsert(data_to_save).execute()
    print("Symbols Updated in Supabase!")

# --- 4. ANGEL ONE LOGIN ---
obj = SmartConnect(api_key=API_KEY)
token = pyotp.TOTP(TOTP_KEY).now()
session = obj.generateSession(CLIENT_ID, PIN, token)

if session.get('status'):
    print("Angel One Login Success!")
else:
    print(f"Login Failed: {session.get('message')}")

# --- 5. LIVE STREAM (FIXED VERSION) ---
# Error fix: V2 ki jagah stable smartWebSocket use kiya hai
sws = obj.smartWebSocket(session['data']['jwtToken'], API_KEY, CLIENT_ID, session['data']['feedToken'])

def on_data(wsapp, msg):
    # Live price broadcast ho raha hai
    sio.emit('livePrice', msg)

def on_open(wsapp):
    print("WebSocket Connected!")
    # Aap yahan symbols subscribe kar sakte hain
    # sws.subscribe("correlation_id", mode, token_list)

sws.on_data = on_data
sws.on_open = on_open

# Background mein connection chalane ke liye
eventlet.spawn(sws.connect)

# --- 6. SERVER START ---
if __name__ == '__main__':
    # Pehle symbols update karein
    try:
        update_symbols_to_supabase()
    except Exception as e:
        print(f"Symbol update error: {e}")

    port = int(os.environ.get('PORT', 5000))
    print(f"Broadcaster Server Live on Port {port}")
    eventlet.wsgi.server(eventlet.listen(('', port)), app)
