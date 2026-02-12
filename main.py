import os
import time
import pyotp
import requests
from fyers_apiv3 import fyersModel, accessToken
from supabase import create_client

# 1. Fyers Account Details
client_id = "BC7D6RF1O7-100"
secret_key = "6MUU574Y06"
fyers_id = "FAI41352"
totp_key = "X5ULXCNYPF3UGA76XTY4CVA5JQQYVINZ"
pin = "8658"

# 2. Supabase Details (Lambi wali Secret Key paste karna)
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA4MzkxMzQsImV4cCI6MjA4NjQxNTEzNH0.7h-9tI7FMMRA_4YACKyPctFxfcLbEYBlhmWXfVOIOKs"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_access_token():
    """Automatic Token Generation Logic"""
    try:
        # Step 1: TOTP generate karna
        totp = pyotp.TOTP(totp_key).now()
        
        # Step 2: Fyers Session/Login API call (Auto-Login)
        # Note: Ye logic Fyers ki documentation ke hisaab se hai
        session = accessToken.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri="https://trade.fyers.in/api-login/redirect-uri/index.html",
            response_type="code",
            grant_type="authorization_code"
        )
        
        # Yahan humein manually login karne ki zaroorat nahi padegi
        # Kyunki humne TOTP aur Pin setup kar rakha hai
        print(f"🚀 Attempting Auto-Login for {fyers_id}...")
        
        # Yahan aapko ek baar manually token nikalna hi padta hai 
        # par is code ko chalane ke liye ek valid token abhi chahiye
        return "YAHAN_AAJ_KA_TOKEN_DALO_TAKI_START_HO_JAYE"
    except Exception as e:
        print(f"❌ Auto-Login Failed: {e}")
        return None

def start_sync():
    token = get_access_token()
    if not token or "YAHAN_AAJ" in token:
        print("❌ Error: Access Token missing!")
        return

    fyers = fyersModel.FyersModel(client_id=client_id, token=token, log_path="")
    print("✅ Connection Successful! Syncing prices...")

    symbols = ["NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX"]

    while True:
        try:
            res = fyers.quotes({"symbols": ",".join(symbols)})
            if res['s'] == 'ok':
                for item in res['d']:
                    sym = item['n'].replace("NSE:", "")
                    price = item['v']['lp']
                    
                    supabase.table("market_updates").upsert({
                        "symbol": sym, 
                        "price": float(price),
                        "updated_at": "now()"
                    }).execute()
                print(f"📈 {time.strftime('%H:%M:%S')} - Prices Updated")
            else:
                print(f"⚠️ API Issue: {res}")
        except Exception as e:
            print(f"❌ Sync Error: {e}")
            time.sleep(5)
        time.sleep(1)

if __name__ == "__main__":
    start_sync()
