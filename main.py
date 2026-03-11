import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, firebase_admin, pytz, requests, sqlite3, tempfile, threading, gc
from firebase_admin import credentials, db
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client

# --- 1. CONFIG SETUP ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# Render App URL (Ise Render settings se copy karke yahan paste kar)
RENDER_APP_URL = "https://myt-backend.onrender.com" 
VERCEL_URL = "https://myt-backend-ztm2.vercel.app/api"

# DATABASES
if not firebase_admin._apps:
    cred = credentials.Certificate("trade-f600a-firebase-adminsdk-fbsvc-269ab50c0c.json")
    firebase_admin.initialize_app(cred, {'databaseURL': 'https://trade-f600a-default-rtdb.firebaseio.com/'})

SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
last_price_cache = {} 
subscribed_tokens_set = set() 
last_master_update_date = None 

# --- 2. MAINTENANCE ENGINE (ANTI-SLEEP + RAM CLEANER) ---
def maintenance_engine():
    global last_price_cache
    while True:
        try:
            # A. ANTI-SLEEP: Render ko ping karo taaki wo hibernate na ho
            requests.get(RENDER_APP_URL, timeout=15)
            print("⏰ [Keep-Alive] Render is Awake. Ping successful.")
            
            # B. MEMORY CLEANUP: Har 5 minute mein purana data clear karo
            # Taaki RAM limit hit na ho aur Render crash na kare
            last_price_cache.clear()
            gc.collect() # Force Garbage Collection
            print("🧹 [Cleanup] Cache cleared and RAM optimized.")
            
        except Exception as e:
            print(f"⚠️ Maintenance Alert: {e}")
        
        # Har 5 minute (300 seconds) mein ye loop chalega
        eventlet.sleep(300)

# --- 3. PRICE DISPATCHER (VERCEL PUSH) ---
def send_to_vercel(token, price):
    try:
        # High speed async push to Vercel
        requests.post(VERCEL_URL, json={"token": str(token), "price": str(price)}, timeout=0.3)
    except:
        pass

# --- 4. MASTER DATA AUTO-SYNC (8:30 AM IST) ---
def refresh_supabase_master():
    print(f"🔄 [System] Updating Master DB on Supabase...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            json_data = response.json()
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
                temp_path = tmp.name
            conn = sqlite3.connect(temp_path)
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS symbols")
            cursor.execute('''CREATE TABLE symbols (token TEXT, symbol TEXT, name TEXT, expiry TEXT, 
                             strike TEXT, lotsize TEXT, instrumenttype TEXT, exch_seg TEXT, tick_size TEXT)''')
            data = [(str(i.get('token')), i.get('symbol'), i.get('name'), i.get('expiry'), i.get('strike'), 
                     i.get('lotsize'), i.get('instrumenttype'), i.get('exch_seg'), i.get('tick_size')) 
                    for i in json_data if i.get('token')]
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", data)
            conn.commit()
            conn.close()
            with open(temp_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(path="angel_master.db", file=f.read(), 
                                                         file_options={"x-upsert": "true", "content-type": "application/octet-stream"})
            os.remove(temp_path)
            print("✅ [Success] Supabase Master DB Replaced.")
            return True
    except Exception as e:
        print(f"❌ Master Sync Error: {e}")
        return False

# --- 5. SMART TICK ENGINE ---
def on_data(wsapp, msg):
    global last_price_cache
    if isinstance(msg, dict) and 'token' in msg:
        token = str(msg.get('token'))
        # LTP calculation
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100
        
        if ltp > 0:
            # Sirf tabhi Vercel bhejo jab price change hua ho (Traffic bachane ke liye)
            if last_price_cache.get(token) != ltp:
                last_price_cache[token] = ltp
                # Threading taaki main socket process delay na ho
                threading.Thread(target=send_to_vercel, args=(token, ltp)).start()

# --- 6. BROKER-LEVEL BATCHING (500 TOKENS LIMIT) ---
def subscribe_in_batches(token_list, exchange_type):
    global sws
    if not sws or not token_list: return
    
    # 500-500 ke groups mein subscribe karna (Angel One Limit)
    for i in range(0, len(token_list), 500):
        batch = token_list[i : i + 500]
        try:
            sws.subscribe("myt_batch", 1, [{"exchangeType": exchange_type, "tokens": batch}])
            print(f"📡 Subscribed Batch: {len(batch)} tokens.")
            eventlet.sleep(0.6) # Safe gap between batches
        except Exception as e:
            print(f"❌ Subscription Error: {e}")

# --- 7. CONNECTION & MARKET HOUR MANAGER ---
def manage_connection():
    global sws, is_ws_ready, last_master_update_date
    while True:
        now = datetime.datetime.now(IST)
        
        # A. Subah 8:30 ka Master Sync
        if now.hour == 8 and 30 <= now.minute <= 59 and last_master_update_date != now.date():
            if refresh_supabase_master(): last_master_update_date = now.date()
        
        # B. 24/7 Market Connection (Always On Logic)
        # 8 AM se Raat 12 AM tak chalu rahega
        if 8 <= now.hour < 24:
            if not is_ws_ready:
                try:
                    smart_api = SmartConnect(api_key=API_KEY)
                    totp = pyotp.TOTP(TOTP_STR).now()
                    session = smart_api.generateSession(CLIENT_CODE, PWD, totp)
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('🟢 WebSocket Connected')")
                        sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                        eventlet.spawn(sws.connect)
                except Exception as e:
                    print(f"🔄 Connection Retry in 60s: {e}")
        else:
            # Midnight resetting to keep system fresh
            if is_ws_ready:
                if sws: sws.close()
                is_ws_ready = False
                subscribed_tokens_set.clear()
                print("💤 [Midnight Reset] System refreshed for next day.")
        
        eventlet.sleep(60)

# --- 8. SYNC WATCHLIST (DYNAMIC) ---
def sync_watchlist():
    global subscribed_tokens_set, is_ws_ready
    while True:
        try:
            if is_ws_ready and sws:
                # Firebase se user ki watchlist uthao (Manmarzi wale tokens)
                user_watchlist = db.reference('central_watchlist').get()
                if user_watchlist:
                    all_tokens_from_fb = []
                    for k, v in user_watchlist.items():
                        t = str(v.get('token'))
                        if t and t != "None": all_tokens_from_fb.append(t)
                    
                    # Sirf wo tokens jo pehle se subscribed nahi hain
                    new_tokens = [t for t in all_tokens_from_fb if t not in subscribed_tokens_set]
                    
                    if new_tokens:
                        subscribe_in_batches(new_tokens, 1)
                        for t in new_tokens: subscribed_tokens_set.add(t)
            
            eventlet.sleep(10) # Har 10 sec mein nayi watchlist check karo
        except Exception as e:
            print(f"🔄 Sync Loop Error: {e}")
            eventlet.sleep(5)

# --- 9. STARTUP ---
if __name__ == '__main__':
    # Saare engines ko background mein start karo
    eventlet.spawn(maintenance_engine) # Anti-Sleep & Cleaner
    eventlet.spawn(manage_connection)  # Login & Market Hours
    eventlet.spawn(sync_watchlist)    # Watchlist Subscription
    
    # Render ke Port ko listen karo taaki server chalu dikhe
    from eventlet import wsgi
    print("🚀 [Evergreen Engine] System is LIVE.")
    wsgi.server(eventlet.listen(('0.0.0.0', int(os.environ.get("PORT", 10000)))), lambda e,s: [b"STABLE_LIVE"])
