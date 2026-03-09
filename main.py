import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json, redis, asyncio
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# --- 1. CONFIG & CONNECTIONS ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# Supabase Config
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

# Redis Connection (Direct Mixed URL)
REDIS_URL = "rediss://default:ATjsAAIncDFiOTVlY2RlNDI1ODk0MDI4YmRiMmE2Yzg0Y2RkM2RkZHAxMTQ1NzI@quick-narwhal-14572.upstash.io:6379"
r = redis.from_url(REDIS_URL, decode_responses=True)

app = FastAPI()

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
last_master_update_date = None

# --- 2. MASTER DATA SYNC (SUPABASE) ---
def refresh_supabase_master():
    print(f"🔄 [System] Overwriting Master Data on Supabase...")
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
            print("✅ [Success] Master DB Overwritten on Supabase.")
            return True
    except Exception as e:
        print(f"❌ Master Error: {e}")
        return False

# --- 3. TICK ENGINE (REDIS PUBLISH) ---
def on_data(wsapp, msg):
    if isinstance(msg, dict) and 'token' in msg:
        token = str(msg.get('token'))
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100
        
        if ltp > 0:
            # Data Packet for Android
            tick_data = {
                "t": token,
                "p": "{:.2f}".format(ltp),
                "c": "{:.2f}".format(float(msg.get('close', 0)) / 100) if 'close' in msg else "0.00"
            }
            # Redis Channel mein phenk do
            r.publish("live_ticks", json.dumps(tick_data))

# --- 4. WEBSOCKET GATEWAY ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    pubsub = r.pubsub()
    pubsub.subscribe("live_ticks")
    try:
        while True:
            message = pubsub.get_message()
            if message and message['type'] == 'message':
                await websocket.send_text(message['data'])
            await asyncio.sleep(0.01) 
    except WebSocketDisconnect:
        pubsub.unsubscribe("live_ticks")

# --- 5. CONNECTION MANAGER ---
def manage_connection():
    global sws, is_ws_ready, last_master_update_date
    while True:
        now = datetime.datetime.now(IST)
        # 8:30 AM Master Data Sync
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
                        sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('🟢 Angel WebSocket Live')")
                        sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                        eventlet.spawn(sws.connect)
                except: pass
        else:
            if is_ws_ready:
                if sws: sws.close()
                is_ws_ready = False
        eventlet.sleep(60)

# Yahan aap permanent tokens subscribe kar sakte hain
def auto_subscribe():
    global is_ws_ready, sws
    while True:
        if is_ws_ready and sws:
            # Aap yahan Nifty/BankNifty ke tokens dal sakte hain jo hamesha chalu rahein
            # sws.subscribe("myt", 1, [{"exchangeType": 1, "tokens": ["26000", "26009"]}])
            pass
        eventlet.sleep(30)

if __name__ == '__main__':
    eventlet.spawn(manage_connection)
    eventlet.spawn(auto_subscribe)
    import uvicorn
    # Render automatically sets the PORT environment variable
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
