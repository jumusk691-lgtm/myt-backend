import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, firebase_admin, pytz, requests, sqlite3, tempfile, gc
from firebase_admin import credentials, db
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from pusher import Pusher
from supabase import create_client

# --- 1. CONFIG SETUP ---
API_KEY = os.environ.get("API_KEY", "85HE4VA1")
CLIENT_CODE = os.environ.get("CLIENT_CODE", "S52638556")
PWD = os.environ.get("MPIN", "0000")
TOTP_STR = os.environ.get("TOTP_STR", "XFTXZ2445N4V2UMB7EWUCBDRMU")
IST = pytz.timezone('Asia/Kolkata')

# SOKETI / PUSHER CONFIG (Redis integrated via Soketi)
SOKETI_HOST = "myt-market-socket.onrender.com"
PUSHER_APP_ID = os.environ.get("PUSHER_APP_ID", "1") 
PUSHER_APP_KEY = os.environ.get("PUSHER_APP_KEY", "myt_key")
PUSHER_APP_SECRET = os.environ.get("PUSHER_APP_SECRET", "myt_secret")

# SUPABASE CONFIG
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "your_supabase_key")
BUCKET_NAME = "Myt"

# Pusher Client Initialization
pusher_client = Pusher(
    app_id=PUSHER_APP_ID, 
    key=PUSHER_APP_KEY, 
    secret=PUSHER_APP_SECRET, 
    host=SOKETI_HOST, 
    port=443, 
    ssl=True,
    cluster=None # Soketi doesn't need cluster
)

# Firebase Init
if not firebase_admin._apps:
    try:
        cred = credentials.Certificate("trade-f600a-firebase-adminsdk-fbsvc-269ab50c0c.json")
        firebase_admin.initialize_app(cred, {'databaseURL': 'https://trade-f600a-default-rtdb.firebaseio.com/'})
    except Exception as e:
        print(f"❌ Firebase Init Error: {e}")

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
last_price_cache = {} 
subscribed_tokens_set = set() 
last_master_update_date = None
last_push_time = {} 

# --- 2. MASTER DATA SYNC ---
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
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", 
                    file=f.read(), 
                    file_options={"x-upsert": "true", "content-type": "application/octet-stream"}
                )
            os.remove(temp_path)
            print("✅ [Success] Master DB Uploaded.")
            gc.collect() # Clean memory
            return True
    except Exception as e:
        print(f"❌ Master Sync Error: {e}")
        return False

# --- 3. BROADCASTER (1.5 - 2 Second Throttling for 1 Lakh Users) ---
def broadcast_to_soketi(token, price):
    global last_push_time
    current_time = time.time()
    
    # 1.5 Second ka gap taaki network congest na ho
    if token not in last_push_time or (current_time - last_push_time[token]) >= 1.5:
        try:
            # Using private-market for more security with large user base
            pusher_client.trigger('market-channel', 'price-update', {'t': token, 'p': price})
            last_push_time[token] = current_time
        except Exception as e:
            # Silent fail for triggers to keep the loop fast
            pass

# --- 4. SMART TICK ENGINE ---
def on_data(wsapp, msg):
    global last_price_cache
    if isinstance(msg, dict) and 'token' in msg:
        token = str(msg.get('token'))
        # Handling different Angel API response formats
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp')
        if ltp_raw is not None:
            ltp = float(ltp_raw) / 100
            
            # Sirf tabhi bhejenge jab price change ho aur cache se alag ho
            if ltp > 0 and last_price_cache.get(token) != ltp:
                last_price_cache[token] = ltp
                eventlet.spawn_n(broadcast_to_soketi, token, ltp)

# --- 5. BATCH SUBSCRIPTION (Safe for High Load) ---
def subscribe_in_batches(token_list):
    global sws
    if not sws or not token_list: return
    # 500 tokens per batch is ideal for stability
    for i in range(0, len(token_list), 500):
        batch = token_list[i : i + 500]
        try:
            # ExchangeType 1 = NSE, adjust if BSE/NFO is needed
            sws.subscribe("myt_market_batch", 1, [{"exchangeType": 1, "tokens": batch}])
            print(f"📡 Subscribed Batch: {len(batch)} tokens.")
            eventlet.sleep(0.5)
        except Exception as e:
            print(f"⚠️ Subscription Error: {e}")

# --- 6. CONNECTION MANAGEMENT (Auto-Recovery) ---
def manage_connection():
    global sws, is_ws_ready, last_master_update_date
    while True:
        try:
            now = datetime.datetime.now(IST)
            
            # Master Sync at 8:30 AM
            if now.hour == 8 and 30 <= now.minute <= 45 and last_master_update_date != now.date():
                if refresh_supabase_master(): 
                    last_master_update_date = now.date()
            
            # Connection check
            if 8 <= now.hour < 24:
                if not is_ws_ready or sws is None:
                    print("🚀 Re-establishing Angel Connection...")
                    smart_api = SmartConnect(api_key=API_KEY)
                    token = pyotp.TOTP(TOTP_STR).now()
                    session = smart_api.generateSession(CLIENT_CODE, PWD, token)
                    
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        sws.on_open = lambda ws: setattr(globals(), 'is_ws_ready', True)
                        sws.on_error = lambda ws, err: setattr(globals(), 'is_ws_ready', False)
                        sws.on_close = lambda ws, code, reason: setattr(globals(), 'is_ws_ready', False)
                        eventlet.spawn(sws.connect)
        except Exception as e:
            print(f"⚠️ Connection Manager Error: {e}")
        
        eventlet.sleep(30)

# --- 7. WATCHLIST SYNC (Real-time Firebase listener alternative) ---
def sync_watchlist():
    global subscribed_tokens_set, is_ws_ready
    while True:
        if is_ws_ready:
            try:
                # Central watchlist se tokens fetch karna
                user_watchlist = db.reference('central_watchlist').get()
                if user_watchlist:
                    all_tokens = [str(v.get('token')) for k,v in user_watchlist.items() if v.get('token')]
                    new_tokens = [t for t in all_tokens if t not in subscribed_tokens_set]
                    
                    if new_tokens:
                        subscribe_in_batches(new_tokens)
                        for t in new_tokens: subscribed_tokens_set.add(t)
                        
                # Memory cleanup (cache maintenance)
                if len(last_price_cache) > 5000:
                    last_price_cache.clear()
                    gc.collect()
            except Exception as e:
                print(f"⚠️ Watchlist Sync Error: {e}")
        eventlet.sleep(20)

# --- 8. WEB SERVER FOR RENDER ---
if __name__ == '__main__':
    print("🔥 MYT Market Server Starting Up...")
    eventlet.spawn(manage_connection)
    eventlet.spawn(sync_watchlist)
    
    # Render's requirement to bind to a port
    from eventlet import wsgi
    port = int(os.environ.get("PORT", 10000))
    print(f"🌐 Server Live on Port {port}")
    wsgi.server(eventlet.listen(('0.0.0.0', port)), lambda e,s: [b"SERVER_RUNNING"])
