import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, threading
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client

# --- 1. CONFIG SETUP ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# SUPABASE CONFIG (Exactly as you provided)
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

# VERCEL CONFIG (Barcelona)
VERCEL_URL = "https://myt-backend-s2q2.vercel.app/api" 

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
last_price_cache = {} 
subscribed_tokens_set = set() 
last_master_update_date = None 

# --- 2. VERCEL PUSH SERVICE ---
def send_to_vercel(token, price):
    try:
        # Har tick ko Vercel par phekne ke liye
        requests.post(VERCEL_URL, json={"token": str(token), "price": str(price)}, timeout=0.5)
    except:
        pass

# --- 3. MASTER DATA SYNC (SUPABASE - NO CHANGES) ---
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

# --- 4. TICK ENGINE ---
def on_data(wsapp, msg):
    global last_price_cache
    if isinstance(msg, dict) and 'token' in msg:
        token = str(msg.get('token'))
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100
        
        if ltp > 0:
            # Check price change to avoid unnecessary hits
            if last_price_cache.get(token) != ltp:
                last_price_cache[token] = ltp
                # Threading use kar rahe hain taaki main process delay na ho
                threading.Thread(target=send_to_vercel, args=(token, ltp)).start()

# --- 5. BATCH SUBSCRIPTION (500 TOKENS) ---
def subscribe_in_batches(token_list, exchange_type):
    global sws
    if not sws or not token_list: return
    
    # 500-500 ke groups mein subscribe karna
    for i in range(0, len(token_list), 500):
        batch = token_list[i : i + 500]
        try:
            sws.subscribe("myt_batch", 1, [{"exchangeType": exchange_type, "tokens": batch}])
            print(f"📡 Subscribed Batch: {len(batch)} tokens (Type: {exchange_type})")
            eventlet.sleep(0.5) # Angel One limit ke liye gap
        except Exception as e:
            print(f"❌ Subscription Error: {e}")

# --- 6. CONNECTION MANAGER ---
def manage_connection():
    global sws, is_ws_ready, last_master_update_date
    while True:
        now = datetime.datetime.now(IST)
        # Master Update at 8:30 AM (Supabase)
        if now.hour == 8 and 30 <= now.minute <= 59 and last_master_update_date != now.date():
            if refresh_supabase_master(): last_master_update_date = now.date()
        
        # 8 AM to Midnight Live
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

# --- 7. SYNC ENGINE (500 BATCHING) ---
def sync_watchlist():
    global subscribed_tokens_set, is_ws_ready
    while True:
        try:
            if is_ws_ready and sws:
                # Yahan aap apne tokens ki list dalen
                # Filhaal example ke liye kuch tokens:
                all_tokens = ["26000", "26009", "3045"] 
                
                new_tokens = [t for t in all_tokens if t not in subscribed_tokens_set]
                
                if new_tokens:
                    # Sirf un-subscribed tokens ko 500 ke batch mein bhejna
                    subscribe_in_batches(new_tokens, 1) # Default NSE Cash
                    for t in new_tokens: subscribed_tokens_set.add(t)
            
            eventlet.sleep(10)
        except:
            eventlet.sleep(5)

if __name__ == '__main__':
    eventlet.spawn(manage_connection)
    eventlet.spawn(sync_watchlist)
    from eventlet import wsgi
    wsgi.server(eventlet.listen(('0.0.0.0', int(os.environ.get("PORT", 10000)))), lambda e,s: [b"STABLE"])
