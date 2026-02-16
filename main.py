import os
import pyotp
import requests
import pandas as pd
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV3 import SmartWebSocketV3 # Naya Tarika
from supabase import create_client, Client
import socketio
import eventlet
import eventlet.wsgi

# --- CONFIGURATION ---
API_KEY = "85HE4VA1"
CLIENT_ID = "S52638556"
PIN = "0000" 
TOTP_KEY = "XFTXZ2445N4V2UMB7EWUCBDRMU"
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA4MzkxMzQsImV4cCI6MjA4NjQxNTEzNH0.7h-9tI7FMMRA_4YACKyPctFxfcLbEYBlhmWXfVOIOKs"

# --- SETUP ---
sio = socketio.Server(cors_allowed_origins='*')
app = socketio.WSGIApp(sio)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- ANGEL ONE LOGIN ---
obj = SmartConnect(api_key=API_KEY)
token = pyotp.TOTP(TOTP_KEY).now()
session = obj.generateSession(CLIENT_ID, PIN, token)
jwtToken = session['data']['jwtToken']
feedToken = session['data']['feedToken']

print("Angel One Login Success!")

# --- LIVE STREAM (V3 METHOD) ---
sws = SmartWebSocketV3(jwtToken, API_KEY, CLIENT_ID, feedToken)

def on_data(msg):
    # Live data sabhi users ko bhej rahe hain
    sio.emit('livePrice', msg)

def on_open():
    print("WebSocket Connected via V3!")

sws.on_data = on_data
sws.on_open = on_open

# Background mein connection
eventlet.spawn(sws.connect)

# --- SERVER START ---
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f"Server Live on Port {port}")
    eventlet.wsgi.server(eventlet.listen(('', port)), app)
