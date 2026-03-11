import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, firebase_admin, pytz, requests, sqlite3, tempfile, gc
from firebase_admin import credentials, db
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from pusher import Pusher
from supabase import create_client

# --- 1. CONFIG SETUP ---
# Inhe Render Environment Variables mein rakhna behtar hai
API_KEY = os.environ.get("API_KEY", "85HE4VA1")
CLIENT_CODE = os.environ.get("CLIENT_CODE", "S52638556")
PWD = os.environ.get("MPIN", "0000")
TOTP_STR = os.environ.get("TOTP_STR", "XFTXZ2445N4V2UMB7EWUCBDRMU")
IST = pytz.timezone('Asia/Kolkata')

# SOKETI / PUSHER CONFIG
SOKETI_HOST = "myt-market-socket.onrender.com"
# Note: Pusher library expects 'app_id', not 'myt_id'
PUSHER_APP_ID = os.environ.get("PUSHER_APP_ID", "1") 
PUSHER_APP_KEY = os.environ.get("PUSHER_APP_KEY", "myt_key")
PUSHER_APP_SECRET = os.environ.get("PUSHER_APP_SECRET", "myt_secret")

# SUPABASE CONFIG
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRnbHV6c2xqYnhrZG93cWFwamhvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzMwNjI0NDcsImV4cCI6MjA4ODYzODQ0N30.5dvATkqcnVn7FgJcmhcjpJsOANZxrALhKQoFaQTdzHY"
BUCKET_NAME = "Myt"

# Pusher Client Initialization (FIXED)
pusher_client = Pusher(
    app_id=PUSHER_APP_ID, 
    key=PUSHER_APP_KEY, 
    secret=PUSHER_APP_SECRET, 
    host=SOKETI_HOST, 
    port=443, 
    ssl=True
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
            return True
    except Exception as e:
        print(f"❌ Master Sync Error: {e}")
        return False

# --- 3. BROADCASTER (2-Second Throttling) ---
def broadcast_to_soketi(token, price):
    global last_push_time
    current_time = time.time()
    
    if token not in last_push_time or (current_time - last_push_time[token]) >= 2.0:
        try:
            # Chota payload taaki bandwidth bache
            pusher_client.trigger('market-channel', 'price-update', {'t': token, 'p': price})
            last_push_time[token] = current_time
        except:
            pass

# --- 4. SMART TICK ENGINE ---
def on_data(wsapp, msg):
    global last_price_cache
    if isinstance(msg, dict) and 'token' in msg:
        token = str(msg.get('token'))
        ltp = float(msg.get('last_traded_price', 0) or msg.get('ltp', 0)) / 100
        
        if ltp > 0 and last_price_cache.get(token) != ltp:
            last_price_cache[token] = ltp
            # Eventlet spawn is better than threading on Render
            eventlet.spawn(broadcast_to_soketi, token, ltp)

# --- 5. BATCH SUBSCRIPTION ---
def subscribe_in_batches(token_list):
    global sws
    if not sws or not token_list: return
    for i in range(0, len(token_list), 500):
        batch = token_list[i : i + 500]
        # ExchangeType 1 for NSE, 2 for BSE, etc. Adapt as needed.
        sws.subscribe("myt_batch", 1, [{"exchangeType": 1, "tokens": batch}])
        print(f"📡 Subscribed Batch: {len(batch)} tokens.")
        eventlet.sleep(0.8)

# --- 6. CONNECTION MANAGEMENT ---
def manage_connection():
    global sws, is_ws_ready, last_master_update_date
    while True:
        now = datetime.datetime.now(IST)
        
        # 8:30 AM Master Sync
        if now.hour == 8 and 30 <= now.minute <= 45 and last_master_update_date != now.date():
            if refresh_supabase_master(): 
                last_master_update_date = now.date()
        
        # Market Hours Connection (8 AM to Midnight)
        if 8 <= now.hour < 24:
            if not is_ws_ready:
                try:
                    print("🚀 Attempting Angel Broking Connection...")
                    smart_api = SmartConnect(api_key=API_KEY)
                    session = smart_api.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
                    
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        def on_open(ws):
                            global is_ws_ready
                            is_ws_ready = True
                            print("✅ WebSocket Connected!")
                        sws.on_open = on_open
                        eventlet.spawn(sws.connect)
                except Exception as e:
                    print(f"⚠️ Connection Attempt Failed: {e}")
        
        eventlet.sleep(60)

# --- 7. WATCHLIST SYNC ---
def sync_watchlist():
    global subscribed_tokens_set, is_ws_ready
    while True:
        if is_ws_ready:
            try:
                # Firebase se watchlist uthana
                user_watchlist = db.reference('central_watchlist').get()
                if user_watchlist:
                    new_tokens = [str(v.get('token')) for k,v in user_watchlist.items() 
                                 if str(v.get('token')) not in subscribed_tokens_set]
                    if new_tokens:
                        subscribe_in_batches(new_tokens)
                        for t in new_tokens: subscribed_tokens_set.add(t)
            except Exception as e:
                print(f"⚠️ Watchlist Sync Error: {e}")
        eventlet.sleep(15)

# --- 8. SERVER START ---
if __name__ == '__main__':
    print("🔥 Starting MYT Market Server...")
    eventlet.spawn(manage_connection)
    eventlet.spawn(sync_watchlist)
    
    # Render keep-alive server
    from eventlet import wsgi
    port = int(os.environ.get("PORT", 10000))
    wsgi.server(eventlet.listen(('0.0.0.0', port)), lambda e,s: [b"STABLE_LIVE"])
