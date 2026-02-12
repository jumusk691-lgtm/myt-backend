import os
import time
import pyotp
from fyers_apiv3 import fyersModel, accessToken
from supabase import create_client

# --- ALL KEYS MIXED HERE ---
# Fyers Details (From your screenshots)
client_id = "BC7D6RF1O7-100"
secret_key = "6MUU574Y06"
fyers_id = "FA141352"
# Dhyan de: TOTP Key aur PIN tujhe khud niche bharna hoga
totp_key = "X5ULXCNYPF3UGA76XTY4CVA5JQQYVINZ"
pin = "8658"

# Supabase Details (From your screenshots)
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
# Screenshot 3 ki 'Secret Key' (sb_secret...) yahan dalo
SUPABASE_KEY = "sb_publishable_pnx6Vb-0H5s9snJocBpVOQ_btd8fQ85"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_access_token():
    """Rozana naya token banane ke liye logic"""
    print(f"Auto-Login start for {fyers_id}...")
    try:
        otp = pyotp.TOTP(totp_key).now()
        # Note: Full automation ke liye Selenium lagta hai, 
        # filhaal ye structure hai token receive karne ka.
        return "YOUR_MANUAL_TOKEN_FOR_TESTING"
    except Exception as e:
        print(f"Login failed: {e}")
        return None

def start_sync():
    token = get_access_token()
    if not token: return

    fyers = fyersModel.FyersModel(client_id=client_id, token=token)
    print("🚀 Backend Live! Syncing to Supabase...")

    # Jo symbols app mein chahiye
    symbols = ["NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX"]

    while True:
        try:
            res = fyers.quotes({"symbols": ",".join(symbols)})
            if res['s'] == 'ok':
                for item in res['d']:
                    sym = item['n'].replace("NSE:", "")
                    price = item['v']['lp']
                    
                    # Supabase mein update bhejna
                    supabase.table("market_updates").upsert({
                        "symbol": sym, 
                        "price": float(price),
                        "updated_at": "now()"
                    }).execute()
                print(f"Prices Updated: {time.strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)
        time.sleep(1) # Har second update

if __name__ == "__main__":
    start_sync()
