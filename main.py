import os
import time
import threading
from flask import Flask, request
from fyers_apiv3 import fyersModel
from supabase import create_client

app = Flask(__name__)

# --- CONFIGURATION ---
client_id = "BC7D6RF1O7-100"
secret_key = "J6QR47A85N"
# DHAYAN DEIN: Ye URL wahi honi chahiye jo Fyers Dashboard aur App mein hai
redirect_uri = "https://myt-backend.onrender.com/auth" 

SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA4MzkxMzQsImV4cCI6MjA4NjQxNTEzNH0.7h-9tI7FMMRA_4YACKyPctFxfcLbEYBlhmWXfVOIOKs"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.route('/')
def home():
    return "Server is Live! Go to Admin App to Login."

# --- 1. AUTH HANDLER (Fixed -437 Error) ---
@app.route('/auth')
def fyers_auth():
    auth_code = request.args.get('code')
    if not auth_code:
        return "<h1>❌ Code Missing!</h1>", 400
    
    try:
        # V3 Session Model
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_uri,
            response_type="code",
            grant_type="authorization_code"
        )
        
        session.set_token(auth_code)
        response = session.generate_token()
        
        if response.get('s') == 'ok':
            access_token = response.get('access_token')
            # Save to 'admin_config' (consistent with Admin App)
            supabase.table("admin_config").upsert({
                "id": 1, 
                "access_token": access_token,
                "status": "Active",
                "last_sync": str(time.time())
            }).execute()
            return "<h1>🎉 Login Success! Token Saved to Supabase.</h1><p>Ab aap Admin App band kar sakte hain.</p>"
        else:
            return f"<h1>❌ Fyers Error:</h1> {response}", 500
            
    except Exception as e:
        return f"<h1>⚠️ Backend Error:</h1> {str(e)}", 500

# --- 2. LIVE PRICE SYNC (WebSockets Ready) ---
def start_price_sync():
    print("🚀 Sync Thread Started...")
    current_token = None
    fyers = None
    
    while True:
        try:
            # Fetch latest token
            res = supabase.table("admin_config").select("access_token").eq("id", 1).execute()
            token_from_db = res.data[0]['access_token'] if res.data else None
            
            if token_from_db and token_from_db != current_token:
                current_token = token_from_db
                fyers = fyersModel.FyersModel(client_id=client_id, token=current_token, is_async=False, log_path="/tmp/")
                print("🆕 New Fyers Session Established!")

            if fyers:
                # Polling for now (Every 3 seconds for speed)
                quotes = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX,NSE:NIFTYBANK-INDEX"})
                if quotes.get('s') == 'ok':
                    for item in quotes['d']:
                        sym = item['v']['short_name']
                        lp = item['v']['lp']
                        # REALTIME UPDATE
                        supabase.table("market_updates").upsert({
                            "symbol": sym, 
                            "price": float(lp),
                            "updated_at": "now()"
                        }).execute()
                    print(f"✅ Live Price Update: {time.strftime('%H:%M:%S')}")
                else:
                    print(f"⚠️ Token Expired or Invalid: {quotes}")
                    
        except Exception as e:
            print(f"❌ Loop Error: {e}")
        
        time.sleep(3) # Ise 1-3 second ke beech rakho fast update ke liye

# Start thread
threading.Thread(target=start_price_sync, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host='0.0.0.0', port=port)
