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
subscribed_tokens_set = set() 
last_master_update_date = None 

# --- 2. MASTER DATA SYNC (AUTO-OVERWRITE) ---
def refresh_supabase_master():
    print(f"🔄 [System] Master Data Syncing & Overwriting...")
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
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", 
                    file=raw_data, 
                    file_options={"x-upsert": "true", "content-type": "application/octet-stream"}
                )
            os.remove(temp_path)
            print("✅ [Success] Master DB Overwritten in Supabase.")
            return True
    except Exception as e: 
        print(f"❌ Master Error: {e}")
        return False

# --- 3. TICK ENGINE ---
def on_data(wsapp, msg):
    global last_price_cache
    if isinstance(msg, dict) and 'token' in msg:
        token = str(msg.get('token'))
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100
        
        if ltp > 0 and token in token_to_fb_items:
            if last_price_cache.get(token) == ltp: return
            
            updates = {}
            now_time = datetime.datetime.now(IST).strftime("%H:%M:%S")
            
            for item in token_to_fb_items[token]:
                path = f"central_watchlist/{item['key']}"
                exch = item['exch'].upper()
                is_4_dec = any(x in exch for x in ["MCX", "CDS"])
                
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

# --- 4. CONNECTION MANAGER (WITH AUTO-TIMER) ---
def manage_connection():
    global sws, is_ws_ready, subscribed_tokens_set, last_master_update_date
    while True:
        now = datetime.datetime.now(IST)
        today_date = now.date()

        # --- AUTO MASTER UPDATE (8:30 AM TO 9:00 AM IST) ---
        if now.hour == 8 and 30 <= now.minute <= 59:
            if last_master_update_date != today_date:
                if refresh_supabase_master():
                    last_master_update_date = today_date

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
                except Exception as e: print(f"⚠️ Reconnect Failed: {e}")
        else:
            if is_ws_ready:
                if sws: sws.close()
                is_ws_ready = False
                subscribed_tokens_set.clear()
        eventlet.sleep(60)

# --- 5. INSTANT SYNC LISTENER ---
def sync_watchlist():
    global token_to_fb_items, subscribed_tokens_set
    while True:
        try:
            if is_ws_ready and sws:
                full_data = db.reference('central_watchlist').get()
                if full_data:
                    temp_map = {}
                    batches = {1: [], 2: [], 3: [], 4: [], 5: []}
                    
                    for fb_key, val in full_data.items():
                        token = str(val.get('token', ''))
                        exch = str(val.get('exch_seg', 'NSE')).upper()
                        if not token or token == "None": continue
                        
                        if token not in temp_map: temp_map[token] = []
                        temp_map[token].append({'key': fb_key, 'exch': exch})
                        
                        if token not in subscribed_tokens_set:
                            etype = 1 # NSE Cash
                            if "NFO" in exch: etype = 2
                            elif "BSE" in exch: etype = 3
                            elif "BFO" in exch: etype = 4
                            elif "MCX" in exch: etype = 5
                            batches[etype].append(token)
                    
                    token_to_fb_items = temp_map

                    for etype, tokens in batches.items():
                        if tokens:
                            for i in range(0, len(tokens), 50):
                                b = tokens[i:i+50]
                                sws.subscribe("myt_task", 1, [{"exchangeType": etype, "tokens": b}])
                                for t in b: subscribed_tokens_set.add(t)
                                print(f"📡 Live Now: {len(b)} tokens in Etype {etype}")
            
            eventlet.sleep(2) # Instant 2-sec check
        except Exception as e:
            eventlet.sleep(5)

if __name__ == '__main__':
    eventlet.spawn(manage_connection)
    eventlet.spawn(sync_watchlist)
    from eventlet import wsgi
    port = int(os.environ.get("PORT", 10000))
    wsgi.server(eventlet.listen(('0.0.0.0', port)), lambda e,s: [b"STABLE"])
