import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, firebase_admin, pytz, json, requests, io, sqlite3
from firebase_admin import credentials, db
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client

# --- 1. FIREBASE SETUP ---
JSON_FILE = "trade-f600a-firebase-adminsdk-fbsvc-269ab50c0c.json"
if not firebase_admin._apps:
    cred = credentials.Certificate(JSON_FILE)
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://trade-f600a-default-rtdb.firebaseio.com/'
    })

# --- 2. CONFIGURATION ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
token_to_fb_keys = {} 
last_price_cache = {} 

# --- 3. MASTER DATA SYNC ---
def refresh_supabase_master():
    print(f"🔄 [System] Syncing Master Data...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            json_data = response.json()
            temp_db = "angel_master.db"
            if os.path.exists(temp_db): os.remove(temp_db)
            
            db_conn = sqlite3.connect(temp_db)
            cursor = db_conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS symbols 
                             (token TEXT, symbol TEXT, name TEXT, expiry TEXT, 
                              strike TEXT, lotsize TEXT, instrumenttype TEXT, 
                              exch_seg TEXT, tick_size TEXT)''')
            
            data_to_insert = [
                (i.get('token'), i.get('symbol'), i.get('name'), i.get('expiry'), 
                 i.get('strike'), i.get('lotsize'), i.get('instrumenttype'), 
                 i.get('exch_seg'), i.get('tick_size'))
                for i in json_data
            ]
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", data_to_insert)
            db_conn.commit()
            db_conn.close()
            
            with open(temp_db, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", file=f.read(), 
                    file_options={"x-upsert": "true", "content-type": "application/x-sqlite3"}
                )
            if os.path.exists(temp_db): os.remove(temp_db)
            print("✅ [Success] Master DB Updated!")
    except Exception as e:
        print(f"❌ Sync Error: {e}")

# --- 4. TICK ENGINE ---
def on_data(wsapp, msg):
    global last_price_cache
    if isinstance(msg, dict) and 'token' in msg:
        token = msg.get('token')
        ltp = float(msg.get('last_traded_price', 0)) / 100
        
        if ltp > 0 and token in token_to_fb_keys:
            # Sirf tab update karein jab price badle (Bandwidth saving)
            if last_price_cache.get(token) == ltp: return
            
            now_time = datetime.datetime.now(IST).strftime("%H:%M:%S")
            updates = {}
            for fb_key in token_to_fb_keys[token]:
                path = f"central_watchlist/{fb_key}"
                is_mcx = "MCX" in fb_key.upper()
                fmt = "{:.4f}" if is_mcx else "{:.2f}"
                updates[f"{path}/price"] = str(fmt.format(ltp))
                updates[f"{path}/utime"] = now_time

            if updates:
                try: 
                    db.reference().update(updates)
                    last_price_cache[token] = ltp
                except: pass

# --- 5. REAL-TIME WATCHLIST LISTENER (NO DELAY) ---
def start_watchlist_listener():
    """Ye function Firebase mein hone wale har change ko turant pakadta hai"""
    def listener_callback(event):
        global token_to_fb_keys
        if not is_ws_ready: return

        # Jab poora data pehli baar load ho ya koi naya node add ho
        if event.event_type in ['put', 'patch']:
            if event.path == "/":
                data = event.data
            else:
                # Jab sirf ek single stock add ho
                key = event.path.strip("/")
                data = {key: event.data}

            if not data: return

            for fb_key, val in data.items():
                if not isinstance(val, dict): continue
                token = str(val.get('token', ''))
                exch = str(val.get('exch_seg', 'NSE')).upper()
                
                if token and token != "None":
                    # Global map update karein
                    if token not in token_to_fb_keys:
                        token_to_fb_keys[token] = []
                    if fb_key not in token_to_fb_keys[token]:
                        token_to_fb_keys[token].append(fb_key)
                    
                    # Smart Subscribe
                    e_type = 5 if "MCX" in exch else (2 if any(x in exch for x in ["NFO", "FUT", "OPT"]) else 1)
                    sws.subscribe("myt_task", 1, [{"exchangeType": e_type, "tokens": [token]}])
                    print(f"⚡ Instant Subscribed: {fb_key} ({token})")

    print("📡 Monitoring Watchlist for changes...")
    db.reference('central_watchlist').listen(listener_callback)

# --- 6. AUTH & CONNECTION ---
def login_and_connect():
    global sws, is_ws_ready
    while True:
        try:
            smart_api = SmartConnect(api_key=API_KEY)
            session = smart_api.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
            if session.get('status'):
                sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                sws.on_data = on_data
                def on_open(ws):
                    global is_ws_ready
                    is_ws_ready = True
                    print('🟢 Market Engine Live')
                sws.on_open = on_open
                sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                sws.connect()
                break
        except Exception as e:
            print(f"⚠️ Auth/WS Error: {e}")
            eventlet.sleep(10)

def simple_app(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/plain')])
    return [b"ENGINE_STABLE"]

# --- 7. MAIN ---
if __name__ == '__main__':
    # Startup Sync
    refresh_supabase_master()
    
    # Background Processes
    eventlet.spawn(login_and_connect)
    
    # Wait for WebSocket to be ready before starting listener
    while not is_ws_ready:
        eventlet.sleep(1)
        
    eventlet.spawn(start_watchlist_listener)
    
    # Render Port Binding
    from eventlet import wsgi
    port = int(os.environ.get("PORT", 10000))
    wsgi.server(eventlet.listen(('0.0.0.0', port)), simple_app)
