import os
import time
import pyotp
from fyers_apiv3 import fyersModel, accessToken
from supabase import create_client

# Environment Variables (Render se aayenge)
client_id = os.getenv("FYERS_CLIENT_ID")
secret_key = os.getenv("FYERS_SECRET_KEY")
fyers_id = os.getenv("FYERS_ID")
totp_key = os.getenv("FYERS_TOTP_KEY")
pin = os.getenv("FYERS_PIN")

# Supabase Setup
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_access_token():
    print("Auto-logging into Fyers...")
    # TOTP generate karega login ke liye
    otp = pyotp.TOTP(totp_key).now()
    # Note: Full auto-login flow requires handling Fyers redirect
    # For now, we print status.
    return "YOUR_ACCESS_TOKEN"

def start_sync():
    print("Backend Active! Price sync starting...")
    while True:
        try:
            # Testing ke liye dummy data (Asli symbols yahan dalenge)
            supabase.table("market_updates").upsert({
                "symbol": "NIFTY", 
                "price": 21750.40
            }).execute()
            time.sleep(1)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    start_sync()

