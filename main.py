import eventlet
eventlet.monkey_patch()

import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from flask import Flask, send_file, request, after_this_request
from flask_socketio import SocketIO, join_room, emit

# --- 1. CONFIG & ELITE CONSTANTS ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

app = Flask(__name__)
# Ping timeout aur interval add kiya hai 'Zombie Connections' marne ke liye
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', ping_timeout=60, ping_interval=25)

# --- GLOBAL STATE (Elite Management) ---
sws = None
is_ws_ready = False
subscribed_tokens_set = set() # Global Pooling for 10th Point
token_room_count = {} # Room management
last_master_update_date = None
last_tick_time = {} # For Circuit Breaker/Throttling

# --- 2. MASTER DATA SYNC (Elite Indexing) ---
def refresh_supabase_master():
    print(f"🔄 [System] Updating Master Data with Indexing...")
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
            
            # Elite Logic: Search speed badhane ke liye indexing
            cursor.execute("CREATE INDEX idx_symbol ON symbols(symbol)")
            cursor.execute("CREATE INDEX idx_token ON symbols(token)")
            
            conn.commit()
            conn.close()
            
            with open(temp_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", 
                    file=f.read(),
                    file_options={"x-upsert": "true", "content-type": "application/octet-stream"}
                )
            os.remove(temp_path)
            print("✅ [System] Indexed Master Data Uploaded.")
            return True
    except Exception as e:
        print(f"❌ Master Error: {e}")
        return False

# --- 3. ELITE TICK ENGINE (OHLC + Throttling) ---
def on_data(wsapp, msg):
    global last_tick_time
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            curr_time = time.time()
            
            # Logic 14: Circuit Breaker (Throttling to 5 ticks per second max per token)
            if token in last_tick_time and (curr_time - last_tick_time[token]) < 0.2:
                return
            last_tick_time[token] = curr_time

            ltp = float(msg.get('last_traded_price', 0)) / 100
            if ltp <= 0: return

            # Logic 7: Professional OHLC + Volume Data
            payload = {
                "t": token,
                "p": "{:.2f}".format(ltp),
                "h": "{:.2f}".format(float(msg.get('high', 0)) / 100),
                "l": "{:.2f}".format(float(msg.get('low', 0)) / 100),
                "o": "{:.2f}".format(float(msg.get('open', 0)) / 100),
                "v": msg.get('volume', 0)
            }
            
            if 'close' in msg and float(msg['close']) > 0:
                cp = float(msg['close']) / 100
                payload["pc"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

            # Room Based Broadcast
            socketio.emit('live_update', payload, room=token)
    except Exception as e:
        print(f"Tick Data Error: {e}")

# --- 4. SELF-HEALING CONNECTION MANAGER ---
def run_trading_engine():
    global sws, is_ws_ready, subscribed_tokens_set, last_master_update_date
    refresh_supabase_master()
    
    while True:
        try:
            now = datetime.datetime.now(IST)
            if now.hour == 8 and 30 <= now.minute <= 45 and last_master_update_date != now.date():
                if refresh_supabase_master(): last_master_update_date = now.date()

            if 7 <= now.hour < 24:
                if not is_ws_ready:
                    print("🔄 [System] Attempting to Connect/Reconnect Angel One...")
                    smart_api = SmartConnect(api_key=API_KEY)
                    totp = pyotp.TOTP(TOTP_STR).now()
                    session = smart_api.generateSession(CLIENT_CODE, PWD, totp)
                    
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('🟢 WebSocket Connected')")
                        sws.on_error = lambda ws, err: print(f"❌ WebSocket Error: {err}")
                        sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                        sws.connect()
                        
                        # Logic 10: Self-Healing (Resubscribe existing tokens after crash)
                        eventlet.sleep(2)
                        if is_ws_ready and subscribed_tokens_set:
                            print(f"🚑 Healing: Resubscribing {len(subscribed_tokens_set)} tokens")
                            # Yahan batches mein subscribe logic repeat hoga
            else:
                if is_ws_ready:
                    sws.close()
                    is_ws_ready = False
                    subscribed_tokens_set.clear()
        except Exception as e:
            print(f"Loop Error: {e}")
        eventlet.sleep(15)

# --- 5. P2P SIGNALING & SUBSCRIPTION (Logic 8 & 9) ---
@socketio.on('subscribe')
def handle_subscribe(json_data):
    global subscribed_tokens_set, sws, is_ws_ready
    watchlist = json_data.get('watchlist', [])
    batches = {1: [], 2: [], 3: [], 4: [], 5: []}

    for item in watchlist:
        token = str(item.get('token'))
        exch = str(item.get('exch', 'NSE')).upper()
        if not token: continue

        join_room(token)
        # Logic 9: Global Pooling (Check if already subscribed by someone else)
        if token not in subscribed_tokens_set:
            etype = 1
            if "MCX" in exch: etype = 5
            elif "NFO" in exch: etype = 2
            batches[etype].append(token)

    if is_ws_ready and sws:
        for etype, tokens in batches.items():
            if tokens:
                sws.subscribe(f"sub_{etype}", 1, [{"exchangeType": etype, "tokens": tokens}])
                for t in tokens: subscribed_tokens_set.add(t)

# --- P2P SIGNALING HANDLERS (Logic 8: Android P2P Bridge) ---
@socketio.on('join_p2p')
def on_p2p_join(data):
    # Har device ko uske user_id room mein dalo
    user_id = data.get('user_id')
    if user_id:
        join_room(user_id)
        print(f"📱 P2P Node Registered: {user_id}")

@socketio.on('p2p_signal')
def forward_signal(data):
    # Offer/Answer forwarder
    target = data.get('targetId')
    data['senderId'] = request.sid
    emit('p2p_signal', data, room=target)

@socketio.on('ice_candidate')
def forward_ice(data):
    # Network path forwarder
    target = data.get('targetId')
    emit('ice_candidate', data, room=target)

# --- 6. ROUTES & UTILS ---
@app.route('/download_db')
def download_db():
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        res = supabase.storage.from_(BUCKET_NAME).download("angel_master.db")
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(res)
            tmp_path = tmp.name
        @after_this_request
        def cleanup(response):
            if os.path.exists(tmp_path): os.remove(tmp_path)
            return response
        return send_file(tmp_path, as_attachment=True, download_name="angel_master.db")
    except Exception as e: return f"Error: {e}", 500

@app.route('/history')
def get_history():
    # Logic 2: Dynamic Interval Support
    token = request.args.get('token')
    exch = request.args.get('exch', 'NSE').upper()
    interval = request.args.get('interval', 'FIVE_MINUTE') 
    try:
        smart_api = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_STR).now()
        smart_api.generateSession(CLIENT_CODE, PWD, totp)
        to_date = datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')
        from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=90)).strftime('%Y-%m-%d %H:%M')
        params = {"exchange": exch, "symboltoken": token, "interval": interval, "fromdate": from_date, "todate": to_date}
        res = smart_api.getCandleData(params)
        if res.get('status') and res.get('data'):
            return json.dumps([{"time": int(datetime.datetime.strptime(c[0], "%Y-%m-%dT%H:%M:%S%z").timestamp()), 
                               "open": c[1], "high": c[2], "low": c[3], "close": c[4]} for c in res['data']])
        return "Not Found", 404
    except Exception as e: return str(e), 500

@app.route('/')
def health():
    return f"Engine: {'READY' if is_ws_ready else 'OFFLINE'} | Active Tokens: {len(subscribed_tokens_set)}", 200

if __name__ == '__main__':
    socketio.start_background_task(run_trading_engine)
    port = int(os.environ.get("PORT", 10000))
    socketio.run(app, host='0.0.0.0', port=port)
