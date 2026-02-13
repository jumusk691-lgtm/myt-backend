import os, time, pyotp
from flask import Flask
from SmartApi import SmartConnect
from supabase import create_client

app = Flask(__name__)

# --- CONFIG ---
API_KEY = "UglQ6MMH"
CLIENT_ID = "S52638556"
PASSWORD = "0000"
TOTP_SECRET = "XFTXZ2445N4V2UMB7EWUCBDRMU"
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA4MzkxMzQsImV4cCI6MjA4NjQxNTEzNH0.7h-9tI7FMMRA_4YACKyPctFxfcLbEYBlhmWXfVOIOKs"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.route('/login')
def login():
    try:
        smart_api = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = smart_api.generateSession(CLIENT_ID, PASSWORD, totp)
        if data['status']:
            token = data['data']['jwtToken']
            supabase.table("admin_config").upsert({"id": 1, "access_token": token, "status": "Active"}).execute()
            return "✅ Login Success! Token saved to Supabase."
        return f"❌ Error: {data['message']}"
    except Exception as e:
        return f"⚠️ Crash: {str(e)}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
