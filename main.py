import os
import time
import pyotp
from fyers_apiv3 import fyersModel, accessToken
from supabase import create_client

# --- ALL KEYS & URLS FULLY MIXED HERE ---

# 1. Fyers Account Details
client_id = "BC7D6RF1O7-100"
secret_key = "6MUU574Y06"
fyers_id = "FAI41352"
totp_key = "X5ULXCNYPF3UGA76XTY4CVA5JQQYVINZ"
pin = "8658"

# 2. Supabase Connection Details
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
# DHAYN DE: Ye key tere Screenshot (06:45 AM) se hai, ise ek baar match kar lena
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczOTEzOTYwNiwiZXhwIjoyMDU0NzE1NjA2fQ.6MUU574Y06_REPLACE_WITH_ACTUAL_FROM_DASHBOARD"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_access_token():
    """Rozana naya token update karne ke liye"""
    print(f"Auto-Login start for {fyers_id}...")
    # NOTE: Roz subah 9:15 se pehle [api.fyers.in/tools] se token nikal kar yahan paste karna
    return "PASTE_YOUR_MORNING_ACCESS_TOKEN_HERE"

def start_sync():
    token = get_access_token()
    if not token or "PASTE_YOUR" in token:
        print("❌ Error: Valid Access Token nahi mila. Subah ka token update karo!")
        return

    fyers = fyersModel.FyersModel(client_id=client_id, token=token, log_path="")
    print("🚀 Backend Live! Prices syncing to Supabase...")

    symbols = ["NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX"]

    while True:
        try:
            res = fyers.quotes({"symbols": ",".join(symbols)})
            if res['s'] == 'ok':
                for item in res['d']:
                    sym = item['n'].replace("NSE:", "")
                    price = item['v']['lp']
                    
                    # Supabase Table Update
                    supabase.table("market_updates").upsert({
                        "symbol": sym, 
                        "price": float(price),
                        "updated_at": "now()"
                    }).execute()
                print(f"✅ Prices Updated: {time.strftime('%H:%M:%S')}")
            else:
                print(f"⚠️ Fyers API Response: {res}")
        except Exception as e:
            print(f"❌ Sync Error: {e}")
            time.sleep(5)
        time.sleep(1)

if __name__ == "__main__":
    start_sync()
