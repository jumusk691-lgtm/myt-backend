import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, firebase_admin, pytz, requests, sqlite3, tempfile
from firebase_admin import credentials, db
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client

# --- 1. CONFIG & FIREBASE SETUP ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

if not firebase_admin._apps:
    cred = credentials.Certificate("trade-f600a-firebase-adminsdk-fbsvc-269ab50c0c.json")
    firebase_admin.initialize_app(cred, {'databaseURL': 'https://trade-f600a-default-rtdb.firebaseio.com/'})

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
token_to_fb_items = {} 
last_price_cache = {} 
subscribed_tokens_set = set() 
last_master_update_date = None 

# --- 2. MASTER DATA SYNC (AUTO-OVERWRITE AT 8:30 AM) ---
def refresh_supabase_master():
    print(f"🔄 [System] Overwriting Master Data...")
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
            print("✅ [Success] Master DB Overwritten.")
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
                is_4_dec = any(x in item['exch'].upper() for x in ["MCX", "CDS"])
                updates[f"{path}/price"] = "{:.4f}".format(ltp) if is_4_dec else "{:.2f}".format(ltp)
                updates[f"{path}/utime"] = now_time
                if 'close' in msg and float(msg['close']) > 0:
                    cp = float(msg['close']) / 100
                    updates[f"{path}/pChange"] = "{:.2f}".format(((ltp - cp) / cp) * 100)
            if updates:
                try: db.reference().update(updates); last_price_cache[token] = ltp
                except: pass

# --- 4. CONNECTION MANAGER ---
def manage_connection():
    global sws, is_ws_ready, subscribed_tokens_set, last_master_update_date
    while True:
        now = datetime.datetime.now(IST)
        if now.hour == 8 and 30 <= now.minute <= 59 and last_master_update_date != now.date():
            if refresh_supabase_master(): last_master_update_date = now.date()
        if 8 <= now.hour < 24:
            if not is_ws_ready:
                try:
                    smart_api = SmartConnect(api_key=API_KEY)
                    session = smart_api.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('🟢 WebSocket Live')")
                        sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                        eventlet.spawn(sws.connect)
                except: pass
        else:
            if is_ws_ready:
                if sws: sws.close()
                is_ws_ready = False; subscribed_tokens_set.clear()
        eventlet.sleep(60)

# --- 5. SMART SYNC (FIXES SEGMENT ERRORS) ---
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
                        symbol = str(val.get('symbol', '')).upper()
                        exch = str(val.get('exch_seg', 'NSE')).upper()
                        if not token or token == "None": continue
                        if token not in temp_map: temp_map[token] = []
                        temp_map[token].append({'key': fb_key, 'exch': exch})
                        if token not in subscribed_tokens_set:
                            etype = 1 # NSE Cash
                            if "MCX" in exch: etype = 5
                            elif any(x in symbol for x in ["CE", "PE", "FUT"]) or "NFO" in exch:
                                etype = 4 if "SENSEX" in symbol or "BFO" in exch else 2
                            elif "BSE" in exch: etype = 3
                            batches[etype].append(token)
                    token_to_fb_items = temp_map
                    for etype, tokens in batches.items():
                        if tokens:
                            for i in range(0, len(tokens), 50):
                                b = tokens[i:i+50]
                                sws.subscribe("myt", 1, [{"exchangeType": etype, "tokens": b}])
                                for t in b: subscribed_tokens_set.add(t)
                                print(f"📡 Subscribed: {len(b)} tokens in Etype {etype}")
            eventlet.sleep(2)
        except: eventlet.sleep(5)

if __name__ == '__main__':
    eventlet.spawn(manage_connection); eventlet.spawn(sync_watchlist)
    from eventlet import wsgi
    wsgi.server(eventlet.listen(('0.0.0.0', int(os.environ.get("PORT", 10000)))), lambda e,s: [b"STABLE"])
