import os
import time
from flask import Flask, request
from SmartApi import SmartConnect
import pyotp
from supabase import create_client

app = Flask(__name__)

# --- CONFIGURATION ---
API_KEY = "UglQ6MMH"
CLIENT_ID = "S52638556"
PASSWORD = "0000"
TOTP_SECRET = "XFTXZ2445N4V2UMB7EWUCBDRMU"

SUPABASE_URL = "https://rcosgnsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA4MzkxMzQsImV4cCI6MjA4NjQxNTEzNH0.7h-9tI7FMMRA_4YACKyPctFxfcLbEYBlhmWXfVOIOKs"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.route('/')
def home():
    return "Angel One Backend is Live!"

@app.route('/login')
def login():
    try:
        # Angel One Login Logic
        smart_api = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = smart_api.generateSession(CLIENT_ID, PASSWORD, totp)

        if data['status']:
            jwt_token = data['data']['jwtToken']
            # Supabase mein token save karna
            supabase.table("admin_config").upsert({
                "id": 1,
                "access_token": jwt_token,
                "status": "Active",
                "last_sync": str(time.time())
            }).execute()
            return "<h1>✅ Login Success!</h1><p>Token saved to Supabase.</p>"
        else:
            return f"<h1>❌ Login Failed</h1><p>{data['message']}</p>"
    except Exception as e:
        return f"<h1>⚠️ Error</h1><p>{str(e)}</p>"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
