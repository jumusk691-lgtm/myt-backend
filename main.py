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

# --- 3. MASTER DATA SYNC (The "Never Fail" Route) ---
def refresh_supabase_master():
    print("🔄 [System] Updating Master DB on Supabase...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # GitHub link is the most reliable for Render/Cloud environments
        urls = [
            "https://raw.githubusercontent.com/angel-one/smartapi-python/master/OpenAPIScriptMaster.json",
            "https://margincalculator.angelbroking.com/OpenApiData/smartlights/AllTickers.json"
        ]
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        json_data = None
        
        for url in urls:
            try:
                print(f"📡 Attempting Fetch: {url}")
                res = requests.get(url, headers=headers, timeout=45)
                if res.status_code == 200:
                    json_data = res.json()
                    print(f"✅ Downloaded {len(json_data)} instruments.")
                    break
            except: continue

        if json_data:
            # Memory mein DB banana
            db_conn = sqlite3.connect(':memory:')
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
            
            # DB file ko binary mein convert karna
            buffer = io.BytesIO()
            for line in db_conn.iterdump():
                buffer.write(f'{line}\n'.encode('utf-8'))
            
            # Supabase par Nayi file "Upsert" (Overwrite) karna
            supabase.storage.from_(BUCKET_NAME).upload(
                path="angel_master.db", 
                file=buffer.getvalue(), 
                file_options={"x-upsert": "true", "content-type": "application/octet-stream"}
            )
            db_conn.close()
            print("✅ [Success] Supabase DB file has been updated!")
        else:
            print("⚠️ [Warning] Could not fetch new data. Keeping the old DB file.")

    except Exception as e:
        print(f"❌ DB Update Error: {str(e)}")

# --- 4. TICK ENGINE ---
def on_data(wsapp, msg):
    global last_price_cache
    if isinstance(msg, dict) and 'token' in msg:
        token = msg.get('token')
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100
        
        if ltp > 0 and token in token_to_fb_keys:
            if last_price_cache.get(token) == ltp: return
            
            now_time = datetime.datetime.now(IST).strftime("%H:%M:%S")
            updates = {}
            for fb_key in token_to_fb_keys[token]:
                path = f"central_watchlist/{fb_key}"
                is_mcx = any(x in fb_key.upper() for x in ["MCX", "GOLD", "SILVER"])
                fmt = "{:.4f}" if is_mcx else "{:.2f}"
                updates[f"{path}/price"] = str(fmt.format(ltp))
                updates[f"{path}/utime"] = now_time
                
                if 'close' in msg and msg['close'] > 0:
                    cp = float(msg['close']) / 100
                    updates[f"{path}/pChange"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

            if updates:
                try: 
                    db.reference().update(updates)
                    last_price_cache[token] = ltp
                except: pass

# --- 5. SYSTEM HANDLERS ---
def login_and_connect():
    global sws, is_ws_ready
    while True:
        try:
            smart_api = SmartConnect(api_key=API_KEY)
            session = smart_api.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
            if session.get('status'):
                sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                sws.on_data = on_data
                sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('🟢 WebSocket Connected')")
                sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                eventlet.spawn(sws.connect)
                break
        except: eventlet.sleep(15)

def sync_watchlist():
    global token_to_fb_keys
    while True:
        try:
            if is_ws_ready:
                full_data = db.reference('central_watchlist').get()
                if full_data:
                    new_token_map = {}
                    subscriptions = {1: [], 2: [], 5: []} 
                    for fb_key, val in full_data.items():
                        token = str(val.get('token', ''))
                        exch = str(val.get('exch_seg', 'NSE')).upper()
                        if not token or token == "None": continue
                        
                        if token not in new_token_map: new_token_map[token] = []
                        new_token_map[token].append(fb_key)
                        
                        e_type = 5 if "MCX" in exch else (2 if any(x in exch for x in ["NFO", "FUT", "OPT"]) else 1)
                        subscriptions[e_type].append(token)
                    
                    token_to_fb_keys = new_token_map
                    for etype, tokens in subscriptions.items():
                        if tokens:
                            for i in range(0, len(tokens), 50):
                                sws.subscribe("myt_task", 1, [{"exchangeType": etype, "tokens": tokens[i:i+50]}])
            eventlet.sleep(60)
        except: eventlet.sleep(10)

if __name__ == '__main__':
    # Step 1: DB Update pehle karein
    refresh_supabase_master()
    
    # Step 2: WebSocket aur Sync chalu karein
    eventlet.spawn(login_and_connect)
    eventlet.spawn(sync_watchlist)
    
    # Step 3: Render Dashboard (Health Check)
    from eventlet import wsgi
    wsgi.server(eventlet.listen(('0.0.0.0', int(os.environ.get("PORT", 10000)))), lambda e, r: [b"RUNNING"])
