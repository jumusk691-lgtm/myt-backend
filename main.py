import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json, asyncio, threading
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager
import redis.asyncio as aioredis
import firebase_admin
from firebase_admin import credentials, db

# --- 1. CONFIG & INITIALIZATION ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# Supabase & Redis
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"
REDIS_URL = "redis://default:ATjsAAIncDFiOTVlY2RlNDI1ODk0MDI4YmRiMmE2Yzg0Y2RkM2RkZHAxMTQ1NzI@quick-narwhal-14572.upstash.io:6379"

# Global State (Aapka "Sustum")
redis_client = None
sws = None
is_ws_ready = False
token_to_fb_items = {} 
last_price_cache = {}
subscribed_tokens_set = set()
last_master_update_date = None
loop = None

# --- 2. MASTER DATA SYNC (8:30 AM) ---
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
            print("✅ [Success] Master DB Updated.")
            return True
    except Exception as e:
        print(f"❌ Master Error: {e}")
        return False

# --- 3. TICK ENGINE (REDIS + CACHE) ---
def on_data(wsapp, msg):
    global last_price_cache, token_to_fb_items
    if isinstance(msg, dict) and 'token' in msg:
        token = str(msg.get('token'))
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100
        
        if ltp > 0:
            # Redis Publish (For Android WebSocket)
            tick_data = {"t": token, "p": "{:.2f}".format(ltp)}
            if redis_client and loop:
                asyncio.run_coroutine_threadsafe(redis_client.publish("live_ticks", json.dumps(tick_data)), loop)
            
            # Aapka Purana Firebase Update Logic
            if token in token_to_fb_items:
                if last_price_cache.get(token) == ltp: return
                # Logic to update Firebase if needed...
                last_price_cache[token] = ltp

# --- 4. SMART SYNC (The "Systum") ---
async def sync_watchlist_loop():
    global token_to_fb_items, subscribed_tokens_set, sws, is_ws_ready
    while True:
        try:
            if is_ws_ready and sws:
                # Firebase se watchlist load karein
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
                            # SMART SEGMENT DETECTION
                            etype = 1 # Default: NSE Cash
                            if "MCX" in exch: etype = 5
                            elif any(x in symbol for x in ["CE", "PE", "FUT"]) or "NFO" in exch:
                                etype = 4 if ("SENSEX" in symbol or "BFO" in exch) else 2
                            elif "BSE" in exch: etype = 3
                            
                            batches[etype].append(token)
                    
                    token_to_fb_items = temp_map
                    # Batch Subscribe
                    for etype, tokens in batches.items():
                        if tokens:
                            sws.subscribe(str(int(time.time())), 1, [{"exchangeType": etype, "tokens": tokens}])
                            for t in tokens: subscribed_tokens_set.add(t)
                            print(f"📡 Subscribed {len(tokens)} tokens in Segment {etype}")
        except Exception as e:
            print(f"⚠️ Sync Error: {e}")
        await asyncio.sleep(10) # 10 seconds mein sync check

# --- 5. LIFESPAN & FASTAPI ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, loop
    loop = asyncio.get_running_loop()
    # Firebase Init
    if not firebase_admin._apps:
        cred = credentials.Certificate("firebase_admin.json") # Make sure file exists
        firebase_admin.initialize_app(cred, {'databaseURL': 'YOUR_FIREBASE_URL'})
    
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    asyncio.create_task(manage_connection_loop())
    asyncio.create_task(sync_watchlist_loop())
    yield
    await redis_client.close()

app = FastAPI(lifespan=lifespan)

# --- WebSocket & Connection Manager logic same as before ---
# (manage_connection_loop etc.)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
