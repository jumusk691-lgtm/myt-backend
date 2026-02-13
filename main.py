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
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "EyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." # Poori key dalo

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 1. ANDROID REDIRECT HANDLER (Fixes "Not Found") ---
@app.route('/auth')
def fyers_auth():
    auth_code = request.args.get('code')
    if not auth_code:
        return "Auth Code Missing!", 400
    
    try:
        # Validate Auth Code to get Access Token
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri="https://myt.onrender.com/auth",
            response_type="code",
            grant_type="authorization_code"
        )
        session.set_token(auth_code)
        response = session.generate_token()
        
        access_token = response.get('access_token')
        if access_token:
            # Save to Supabase
            supabase.table("trading_config").upsert({
                "id": 1, 
                "access_token": access_token,
                "updated_at": "now()"
            }).execute()
            return "<h1>🎉 Success! Token saved. Ab app check karein.</h1>"
        return f"Error: {response}", 500
    except Exception as e:
        return f"Backend Error: {str(e)}", 500

# --- 2. PRICE SYNC LOGIC (Runs in Background) ---
def start_price_sync():
    print("🚀 Sync Thread Started...")
    current_token = None
    fyers = None
    
    while True:
        try:
            # Fetch latest token from DB
            res = supabase.table("trading_config").select("access_token").eq("id", 1).execute()
            token_from_db = res.data[0]['access_token'] if res.data else None
            
            if token_from_db and token_from_db != current_token:
                current_token = token_from_db
                fyers = fyersModel.FyersModel(client_id=client_id, token=current_token, is_async=False, log_path="")
                print("🆕 New Connection established with Fyers!")

            if fyers:
                quotes = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX,NSE:NIFTYBANK-INDEX"})
                if quotes.get('s') == 'ok':
                    for item in quotes['d']:
                        sym = item['n'].replace("NSE:", "")
                        lp = item['v']['lp']
                        supabase.table("market_updates").upsert({"symbol": sym, "price": float(lp)}).execute()
                    print(f"✅ Synced: {time.strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"❌ Sync Error: {e}")
        time.sleep(15)

# Start Sync in a separate thread so Flask can keep running
threading.Thread(target=start_price_sync, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host='0.0.0.0', port=port)
