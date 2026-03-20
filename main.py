import eventlet
eventlet.monkey_patch(all=True) 

import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json, gc, socket
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from flask import Flask, send_file, request, after_this_request, jsonify
from flask_socketio import SocketIO, join_room, emit

# ==============================================================================
# --- 1. CONFIGURATION ---
# ==============================================================================
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', 
                   ping_timeout=120, ping_interval=40, max_http_buffer_size=50000000)

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
subscribed_tokens_set = set()      
global_market_cache = {}           
last_tick_time = {}                
previous_price = {}                
last_master_update_date = None
user_scores = {}                   

# ==============================================================================
# --- 2. NETWORK STABILITY ---
# ==============================================================================
def check_dns():
    """Render DNS timeout fix"""
    try:
        socket.gethostbyname("apiconnect.angelone.in")
        return True
    except:
        print("⚠️ [Network] Waiting for DNS/Internet...")
        return False

# ==============================================================================
# --- 3. MASTER DATA ENGINE ---
# ==============================================================================
def refresh_supabase_master():
    print(f"🔄 [System] Updating Master Data...")
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
            cursor.execute("CREATE INDEX idx_sym ON symbols(symbol)")
            cursor.execute("CREATE INDEX idx_tok ON symbols(token)")
            conn.commit()
            conn.close()
            with open(temp_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(path="angel_master.db", file=f.read(), file_options={"x-upsert": "true"})
            os.remove(temp_path)
            return True
    except Exception as e:
        print(f"❌ Master Error: {e}")
        return False

# ==============================================================================
# --- 4. TICK & BROADCASTER ENGINE ---
# ==============================================================================
def on_data(wsapp, msg):
    global global_market_cache, previous_price, last_tick_time
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            ltp = float(msg.get('last_traded_price', 0)) / 100
            if ltp <= 0: return
            
            old_p = previous_price.get(token, "{:.2f}".format(ltp))
            payload = {
                "t": token, "p": "{:.2f}".format(ltp), "lp": old_p,
                "h": "{:.2f}".format(float(msg.get('high', 0)) / 100),
                "l": "{:.2f}".format(float(msg.get('low', 0)) / 100),
                "v": msg.get('volume', 0), "o": "{:.2f}".format(float(msg.get('open', 0)) / 100)
            }
            if 'close' in msg and float(msg['close']) > 0:
                cp = float(msg['close']) / 100
                payload["pc"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

            global_market_cache[token] = payload
            previous_price[token] = "{:.2f}".format(ltp)
    except: pass

def broadcaster_engine():
    """Har 0.5 sec mein saare symbols ko ek saath bhejta hai"""
    while True:
        if global_market_cache:
            batch = dict(global_market_cache)
            global_market_cache.clear()
            for token, data in batch.items():
                socketio.emit('live_update', data, to=token)
        eventlet.sleep(0.5)

# ==============================================================================
# --- 5. RECOVERY ENGINE ---
# ==============================================================================
def run_trading_engine():
    global sws, is_ws_ready, subscribed_tokens_set
    refresh_supabase_master()
    while True:
        try:
            if not is_ws_ready:
                if not check_dns():
                    eventlet.sleep(10)
                    continue
                smart_api = SmartConnect(api_key=API_KEY)
                totp = pyotp.TOTP(TOTP_STR).now()
                session = smart_api.generateSession(CLIENT_CODE, MPIN, totp)
                if session and session.get('status'):
                    sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                    sws.on_data = on_data
                    sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('💎 WS Live!')")
                    sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                    sws.connect()
        except: pass
        eventlet.sleep(20)

# ==============================================================================
# --- 6. SMART 500 BATCHING SUBSCRIPTION ---
# ==============================================================================
@socketio.on('subscribe')
def handle_subscribe(json_data):
    global subscribed_tokens_set, sws, is_ws_ready
    watchlist = json_data.get('watchlist', [])
    if not watchlist: return

    # Smart Segment Separation
    batches = {1: [], 2: [], 3: [], 4: [], 5: []}

    for item in watchlist:
        token = str(item.get('token'))
        exch = str(item.get('exch', 'NSE')).upper()
        if not token or token == "None": continue

        join_room(token)
        if token not in subscribed_tokens_set:
            # Auto detect segment
            etype = 1
            if "MCX" in exch: etype = 5
            elif "NFO" in exch: etype = 2
            elif "BSE" in exch: etype = 3
            
            batches[etype].append(token)
            subscribed_tokens_set.add(token)

    # 500 Batching Logic
    if is_ws_ready and sws:
        for etype, tokens in batches.items():
            if tokens:
                # 500 ki batches mein divide karna
                for i in range(0, len(tokens), 500):
                    sub_batch = tokens[i:i+500]
                    try:
                        sws.subscribe(f"batch_{etype}_{i}", 1, [{"exchangeType": etype, "tokens": sub_batch}])
                        print(f"✅ Subscribed Batch: {len(sub_batch)} tokens for Segment {etype}")
                        eventlet.sleep(0.1) # Small gap for stability
                    except Exception as e:
                        print(f"❌ Batch Error: {e}")

# ==============================================================================
# --- 7. ROUTES ---
# ==============================================================================
@app.route('/')
def health():
    return {
        "status": "OPERATIONAL" if is_ws_ready else "RECONNECTING",
        "ws_active": is_ws_ready,
        "total_subscribed": len(subscribed_tokens_set),
        "timestamp": datetime.datetime.now(IST).isoformat()
    }, 200

if __name__ == '__main__':
    socketio.start_background_task(broadcaster_engine)
    socketio.start_background_task(run_trading_engine)
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
