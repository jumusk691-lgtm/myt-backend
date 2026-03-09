import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, firebase_admin, pytz, requests, sqlite3, tempfile
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

# --- CONFIG ---
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
token_to_fb_items = {} 
last_price_cache = {} 
subscribed_tokens_set = set() # Ismein wahi jayega jo real mein subscribe ho chuka hai

# --- 2. MASTER DATA SYNC (UNCHANGED) ---
def refresh_supabase_master():
    print(f"🔄 [System] Master Data Syncing...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            json_data = response.json()
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
                temp_path = tmp.name
            db_conn = sqlite3.connect(temp_path)
            cursor = db_conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS symbols")
            cursor.execute('''CREATE TABLE symbols 
                             (token TEXT, symbol TEXT, name TEXT, expiry TEXT, 
                              strike TEXT, lotsize TEXT, instrumenttype TEXT, 
                              exch_seg TEXT, tick_size TEXT)''')
            data_to_insert = [
                (str(i.get('token')), i.get('symbol'), i.get('name'), i.get('expiry'), 
                 i.get('strike'), i.get('lotsize'), i.get('instrumenttype'), 
                 i.get('exch_seg'), i.get('tick_size'))
                for i in json_data if i.get('token')
            ]
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", data_to_insert)
            db_conn.commit()
            db_conn.close()
            with open(temp_path, "rb") as f:
                raw_data = f.read()
                try: supabase.storage.from_(BUCKET_NAME).remove(["angel_master.db"])
                except: pass
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", file=raw_data, 
                    file_options={"x-upsert": "true", "content-type": "application/octet-stream"}
                )
            os.remove(temp_path)
            print("✅ [Success] Master DB is ready.")
    except Exception as e: print(f"❌ Master Error: {e}")

# --- 3. TICK ENGINE (Fast updates) ---
def on_data(wsapp, msg):
    global last_price_cache
    if isinstance(msg, dict) and 'token' in msg:
        token = msg.get('token')
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100
        
        if ltp > 0 and token in token_to_fb_items:
            # Agar price nahi badla toh update skip karo (Server load bachao)
            if last_price_cache.get(token) == ltp: return
            
            updates = {}
            now_time = datetime.datetime.now(IST).strftime("%H:%M:%S")
            
            for item in token_to_fb_items[token]:
                fb_key = item['key']
                exch = item['exch']
                path = f"central_watchlist/{fb_key}"
                
                # Decimal logic (Indian Segments)
                # MCX aur Currency ke liye 4 decimal, baki sab ke liye 2
                is_4_dec = any(x in exch.upper() for x in ["MCX", "CDS"])
                
                updates[f"{path}/price"] = "{:.4f}".format(ltp) if is_4_dec else "{:.2f}".format(ltp)
                updates[f"{path}/utime"] = now_time
                
                if 'close' in msg and float(msg['close']) > 0:
                    cp = float(msg['close']) / 100
                    p_chg = ((ltp - cp) / cp) * 100
                    updates[f"{path}/pChange"] = "{:.2f}".format(p_chg)

            if updates:
                try: 
                    db.reference().update(updates)
                    last_price_cache[token] = ltp
                except: pass

# --- 4. SMART CONNECTION MANAGER ---
def manage_connection():
    global sws, is_ws_ready, subscribed_tokens_set
    while True:
        now = datetime.datetime.now(IST)
        # Market hours logic
        if 8 <= now.hour < 24:
            if not is_ws_ready:
                try:
                    smart_api = SmartConnect(api_key=API_KEY)
                    otp = pyotp.TOTP(TOTP_STR).now()
                    session = smart_api.generateSession(CLIENT_CODE, PWD, otp)
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('🟢 WebSocket Live')")
                        sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                        eventlet.spawn(sws.connect)
                        refresh_supabase_master()
                except Exception as e: print(f"⚠️ Reconnect Failed: {e}")
        else:
            if is_ws_ready:
                if sws: sws.close()
                is_ws_ready = False
                subscribed_tokens_set.clear() # Raat ko clear karo taaki subah naye siray se ho
        eventlet.sleep(60)

# --- 5. SYNC WATCHLIST (NO DUPLICATE SUBSCRIPTION) ---
def sync_watchlist():
    global token_to_fb_items, subscribed_tokens_set
    while True:
        try:
            if is_ws_ready:
                full_data = db.reference('central_watchlist').get()
                if full_data:
                    new_token_map = {}
                    to_sub_batches = {1: [], 2: [], 5: []} 
                    
                    for fb_key, val in full_data.items():
                        token = str(val.get('token', ''))
                        exch = str(val.get('exch_seg', 'NSE')).upper()
                        if not token or token == "None": continue
                        
                        # Har user ke path ko map karo taaki price update ho sake
                        if token not in new_token_map: new_token_map[token] = []
                        new_token_map[token].append({'key': fb_key, 'exch': exch})
                        
                        # --- SMART FILTER: Baar-baar subscribe nahi karna ---
                        if token not in subscribed_tokens_set:
                            # Exchange Type decide karo
                            etype = 1 # Cash
                            if any(x in exch for x in ["NFO", "BFO"]): etype = 2 # F&O
                            elif "MCX" in exch: etype = 5 # Commodity
                            
                            to_sub_batches[etype].append(token)
                    
                    token_to_fb_items = new_token_map

                    # Sirf unhi tokens ko subscribe karo jo 'set' mein nahi hain
                    for etype, tokens in to_sub_batches.items():
                        if tokens:
                            for i in range(0, len(tokens), 50): # 50 ka batch
                                batch = tokens[i:i+50]
                                sws.subscribe("myt_task", 1, [{"exchangeType": etype, "tokens": batch}])
                                for t in batch:
                                    subscribed_tokens_set.add(t) # Mark as subscribed
                                print(f"📡 New Subscribed: {len(batch)} tokens in Etype {etype}")
            
            eventlet.sleep(5) # Har 5 sec mein check karo koi naya user toh nahi aaya
        except Exception as e:
            print(f"Sync Error: {e}")
            eventlet.sleep(10)

if __name__ == '__main__':
    eventlet.spawn(manage_connection)
    eventlet.spawn(sync_watchlist)
    from eventlet import wsgi
    wsgi.server(eventlet.listen(('0.0.0.0', int(os.environ.get("PORT", 10000)))), lambda e,s: [b"STABLE"])
