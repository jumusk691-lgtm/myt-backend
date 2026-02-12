import time
from fyers_apiv3 import fyersModel
from supabase import create_client

# 1. Credentials (Teri secret key pehle se hai)
client_id = "BC7D6RF1O7-100"
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "EyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDgzOTEzNCwiZXhwIjoyMDg2NDE1MTM0fQ.5BofQbMKiMLGFjqcIGaCwpoO9pLZnuLg7nojP0aGhJw"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_latest_token():
    """Database se latest token uthane ka function"""
    try:
        res = supabase.table("trading_config").select("access_token").order("updated_at", desc=True).limit(1).execute()
        if res.data:
            return res.data[0]['access_token']
    except Exception as e:
        print(f"❌ DB Fetch Error: {e}")
    return None

def start_sync():
    current_token = None
    fyers = None
    print("🚀 Backend Started! Waiting for token from Supabase...")

    while True:
        try:
            # 1. DB se check karo kya naya token aaya hai
            token_from_db = get_latest_token()
            
            if token_from_db and token_from_db != current_token:
                print("🆕 Naya Token mila! Connection refresh kar raha hoon...")
                current_token = token_from_db
                fyers = fyersModel.FyersModel(client_id=client_id, token=current_token, log_path="")

            # 2. Agar connection active hai toh price fetch karo
            if fyers:
                res = fyers.quotes({"symbols": "NSE:NIFTY50-INDEX,NSE:NIFTYBANK-INDEX"})
                if res['s'] == 'ok':
                    for item in res['d']:
                        sym = item['n'].replace("NSE:", "")
                        price = item['v']['lp']
                        # Price Supabase mein save karo
                        supabase.table("market_updates").upsert({"symbol": sym, "price": float(price), "updated_at": "now()"}).execute()
                    print(f"✅ Price Synced: {time.strftime('%H:%M:%S')}")
                else:
                    print(f"⚠️ Fyers Token Expired ya Galat hai. Admin Panel se naya dalo.")
            else:
                print("😴 No Token Found. Please update token in Supabase table 'trading_config'.")

        except Exception as e:
            print(f"❌ Error: {e}")
        
        time.sleep(10) # Har 10 sec mein check karega

if __name__ == "__main__":
    start_sync()
