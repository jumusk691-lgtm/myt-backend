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
token_to_fb_keys = {} 
last_price_cache = {} 
subscribed_tokens_set = set()

# --- 2. MASTER DATA SYNC (Sahi wala Supabase Upload) ---
def refresh_supabase_master():
    print(f"🔄 [System] Syncing Master Data for Android & F&O...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = requests.get(url, timeout=60)
        
        if response.status_code == 200:
            json_data = response.json()
            db_conn = sqlite3.connect(':memory:')
            cursor = db_conn.cursor()
            
            # Wahi table structure jo DatabaseHelper.kt ko chahiye
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
            
            db_dump = io.BytesIO()
            for line in db_conn.iterdump():
                db_dump.write(f'{line}\n'.encode('utf-8'))
            
            raw_data = db_dump.getvalue()
            
            # --- SUPABASE UPLOAD FIX ---
            # Pehle delete karne ki koshish karenge ya upsert use karenge
            try:
                supabase.storage.from_(BUCKET_NAME).remove(["angel_master.db"])
            except: pass
            
            res = supabase.storage.from_(BUCKET_NAME).upload(
                path="angel_master.db", 
                file=raw_data, 
                file_options={"x-upsert": "true", "content-type": "application/octet-stream"}
            )
            
            db_conn.close()
            print("✅ [Success] Supabase Master DB Re-Uploaded!")
    except Exception as e:
        print(f"❌ Supabase Sync Error: {str(e)}")

# --- 3. TICK ENGINE ---
def on_data(wsapp, msg):
    global last_price_cache
    if isinstance(msg, dict) and 'token' in msg:
        token = msg.get('token')
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100
        
        if ltp > 0 and token in token_to_fb_keys:
            if last_price_cache.get(token) == ltp: return
            
            updates = {}
            now_time = datetime.datetime.now(IST).strftime("%H:%M:%S")
            for fb_key in token_to_fb_keys[token]:
                path = f"central_watchlist/{fb_key}"
                # MCX aur Currency ke liye 4 decimal, baki 2
                is_4_decimal = any(x in fb_key.upper() for x in ["MCX", "GOLD", "SILVER", "USDINR"])
                updates[f"{path}/price"] = "{:.4f}".format(ltp) if is_4_decimal else "{:.2f}".format(ltp)
                updates[f"{path}/utime"] = now_time
                
                if 'close' in msg and float(msg['close']) > 0:
                    cp = float(msg['close']) / 100
                    updates[f"{path}/pChange"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

            if updates:
                try: 
                    db.reference().update(updates)
                    last_price_cache[token] = ltp
                except: pass

# --- 4. AUTO-SCHEDULE (8:30 AM - 12:00 AM) ---
def manage_connection():
    global sws, is_ws_ready, subscribed_tokens_set
    while True:
        now = datetime.datetime.now(IST)
        # Market Time: 8:30 se 23:59 tak
        is_market_hours = (now.hour > 8 or (now.hour == 8 and now.minute >= 30)) and (now.hour < 24)
        
        if is_market_hours:
            if not is_ws_ready:
                try:
                    smart_api = SmartConnect(api_key=API_KEY)
                    session = smart_api.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('🟢 Market Live')")
                        sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                        eventlet.spawn(sws.connect)
                        refresh_supabase_master() # Roz subah refresh
                except Exception as e: print(f"⚠️ Login Failed: {e}")
        else:
            if is_ws_ready:
                if sws: sws.close()
                is_ws_ready = False
                subscribed_tokens_set.clear()
        eventlet.sleep(30)

# --- 5. FNO & STOCKS WATCHLIST SYNC ---
def sync_watchlist():
    global token_to_fb_keys, subscribed_tokens_set
    last_refresh = time.time()
    while True:
        try:
            if is_ws_ready:
                # Har 60 sec mein clear karke re-subscribe (Force Refresh)
                if time.time() - last_refresh > 60:
                    subscribed_tokens_set.clear()
                    last_refresh = time.time()

                full_data = db.reference('central_watchlist').get()
                if full_data:
                    new_token_map = {}
                    to_sub = {1: [], 2: [], 5: []} # 1:NSE, 2:FNO, 5:MCX
                    
                    for fb_key, val in full_data.items():
                        token = str(val.get('token', ''))
                        exch = str(val.get('exch_seg', 'NSE')).upper()
                        if not token or token == "None": continue
                        
                        if token not in new_token_map: new_token_map[token] = []
                        new_token_map[token].append(fb_key)
                        
                        if token not in subscribed_tokens_set:
                            # FNO (NFO/BFO) logic
                            if any(x in exch for x in ["NFO", "BFO"]):
                                etype = 2
                            elif "MCX" in exch:
                                etype = 5
                            else:
                                etype = 1
                            to_sub[etype].append(token)
                    
                    token_to_fb_keys = new_token_map
                    for etype, tokens in to_sub.items():
                        if tokens:
                            for i in range(0, len(tokens), 50):
                                batch = tokens[i:i+50]
                                sws.subscribe("myt_task", 1, [{"exchangeType": etype, "tokens": batch}])
                                for t in batch: subscribed_tokens_set.add(t)
            eventlet.sleep(1)
        except: eventlet.sleep(5)

if __name__ == '__main__':
    eventlet.spawn(manage_connection)
    eventlet.spawn(sync_watchlist)
    from eventlet import wsgi
    wsgi.server(eventlet.listen(('0.0.0.0', int(os.environ.get("PORT", 10000)))), lambda e,s: [b"ENGINE_STABLE"])
