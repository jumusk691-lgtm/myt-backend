import eventlet
eventlet.monkey_patch()

import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from flask import Flask, send_file, request, after_this_request
from flask_socketio import SocketIO, join_room, emit

# --- 1. CONFIG & RENDER STABILITY ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

app = Flask(__name__)
# Logic: Buffer size badhaya taaki 500+ symbols ka load handle ho sake
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', 
                   ping_timeout=60, ping_interval=25, 
                   max_http_buffer_size=15000000) 

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
subscribed_tokens_set = set() 
token_masters = {}           
last_tick_time = {}          
previous_price = {}          
last_master_update_date = None

# --- 2. MASTER DATA SYNC ---
def refresh_supabase_master():
    print(f"🔄 [System] Updating Master Data (Indexed)...")
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
    except Exception as e: print(f"❌ Master Error: {e}"); return False

# --- 3. TICK ENGINE (1s Throttle & History) ---
def on_data(wsapp, msg):
    global last_tick_time, previous_price
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            curr_time = time.time()
            if token in last_tick_time and (curr_time - last_tick_time[token]) < 1.0: return
            
            ltp = float(msg.get('last_traded_price', 0)) / 100
            if ltp <= 0: return
            old_p = previous_price.get(token, "{:.2f}".format(ltp))

            payload = {
                "t": token, "p": "{:.2f}".format(ltp), "lp": old_p,
                "h": "{:.2f}".format(float(msg.get('high', 0)) / 100),
                "l": "{:.2f}".format(float(msg.get('low', 0)) / 100),
                "v": msg.get('volume', 0)
            }
            if 'close' in msg and float(msg['close']) > 0:
                cp = float(msg['close']) / 100
                payload["pc"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

            previous_price[token] = "{:.2f}".format(ltp)
            last_tick_time[token] = curr_time
            socketio.emit('live_update', payload, room=token)
            
            if len(last_tick_time) > 2000:
                last_tick_time.clear()
                previous_price.clear()
    except Exception as e: print(f"Tick Error: {e}")

# --- 4. ENGINE & RECOVERY ---
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
                    smart_api = SmartConnect(api_key=API_KEY)
                    session = smart_api.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True")
                        sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                        sws.connect()
            else:
                if is_ws_ready: sws.close(); is_ws_ready = False; subscribed_tokens_set.clear()
        except Exception as e: print(f"Loop: {e}")
        eventlet.sleep(15)

# --- 5. BATCHED P2P & SUBSCRIPTION (MIXED LOGIC) ---
@socketio.on('subscribe')
def handle_subscribe(json_data):
    global subscribed_tokens_set, token_masters, sws, is_ws_ready
    watchlist = json_data.get('watchlist', [])
    
    # Logic: 500 symbols ka batch banaya gaya hai
    BATCH_SIZE = 500
    all_tokens_to_sub = {1: [], 2: [], 3: [], 4: [], 5: []}

    for item in watchlist:
        token = str(item.get('token'))
        exch = str(item.get('exch', 'NSE')).upper()
        if not token or token == "None": continue

        join_room(token)

        # P2P Logic
        if token in token_masters and token_masters[token] != request.sid:
            emit('p2p_connect_to_master', {'token': token, 'masterId': token_masters[token]}, room=request.sid)
        else:
            token_masters[token] = request.sid
            if token not in subscribed_tokens_set:
                etype = 1
                if "MCX" in exch: etype = 5
                elif "NFO" in exch: etype = 2
                elif "BSE" in exch: etype = 3
                all_tokens_to_sub[etype].append(token)
                subscribed_tokens_set.add(token)

    # Angel One Subscription in 500-sized chunks
    if is_ws_ready and sws:
        for etype, tokens in all_tokens_to_sub.items():
            if tokens:
                for i in range(0, len(tokens), BATCH_SIZE):
                    batch = tokens[i:i + BATCH_SIZE]
                    sws.subscribe(f"sub_{etype}_{i}", 1, [{"exchangeType": etype, "tokens": batch}])
                    print(f"📦 [Batch] Subscribed {len(batch)} tokens (Exchange {etype})")
                    eventlet.sleep(0.3) # Stability gap

# --- 6. P2P SIGNALING ---
@socketio.on('join_p2p')
def on_p2p_join(data):
    user_id = data.get('user_id')
    if user_id: join_room(user_id)

@socketio.on('p2p_signal')
def forward_signal(data):
    target = data.get('targetId')
    data['senderId'] = request.sid
    emit('p2p_signal', data, room=target)

@socketio.on('ice_candidate')
def forward_ice(data):
    target = data.get('targetId')
    emit('ice_candidate', data, room=target)

@socketio.on('disconnect')
def on_disconnect():
    global token_masters
    for token, master_id in list(token_masters.items()):
        if master_id == request.sid: del token_masters[token]

# --- 7. ROUTES ---
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
    token = request.args.get('token')
    exch = request.args.get('exch', 'NSE').upper()
    try:
        smart_api = SmartConnect(api_key=API_KEY)
        smart_api.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
        to_date = datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')
        from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=90)).strftime('%Y-%m-%d %H:%M')
        res = smart_api.getCandleData({"exchange": exch, "symboltoken": token, "interval": "FIVE_MINUTE", "fromdate": from_date, "todate": to_date})
        if res.get('status'):
            return json.dumps([{"time": int(datetime.datetime.strptime(c[0], "%Y-%m-%dT%H:%M:%S%z").timestamp()), "open": c[1], "high": c[2], "low": c[3], "close": c[4]} for c in res['data']])
        return "Error", 404
    except Exception as e: return str(e), 500

@app.route('/')
def health():
    return {"engine": "READY" if is_ws_ready else "OFFLINE", "active_tokens": len(subscribed_tokens_set), "p2p_masters": len(token_masters)}, 200

if __name__ == '__main__':
    socketio.start_background_task(run_trading_engine)
    port = int(os.environ.get("PORT", 10000))
    socketio.run(app, host='0.0.0.0', port=port)
