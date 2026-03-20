import eventlet
eventlet.monkey_patch(all=True) 

import os
import pyotp
import time
import datetime
import pytz
import requests
import sqlite3
import tempfile
import json
import gc
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
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU" # Logic: Fixed variable naming
IST = pytz.timezone('Asia/Kolkata')

# Supabase Credentials
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

app = Flask(__name__)

# Logic: High buffer for P2P/WebRTC signaling and large watchlists
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='eventlet', 
    ping_timeout=120, 
    ping_interval=40, 
    max_http_buffer_size=50000000
)

# ==============================================================================
# --- 2. GLOBAL STATE ---
# ==============================================================================
sws = None
is_ws_ready = False
subscribed_tokens_set = set()      
token_masters = {}                # Tracks {token: [list_of_sids]} for P2P
live_data_queue = {}               
last_tick_time = {}                
previous_price = {}                
last_master_update_date = None

# ==============================================================================
# --- 3. MASTER DATA ENGINE (DB SYNC) ---
# ==============================================================================
def refresh_supabase_master():
    print(f"🔄 [System] {datetime.datetime.now(IST)}: Master Data Sync Started...")
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
            cursor.execute('''CREATE TABLE symbols (
                                token TEXT, symbol TEXT, name TEXT, expiry TEXT,
                                strike TEXT, lotsize TEXT, instrumenttype TEXT, 
                                exch_seg TEXT, tick_size TEXT)''')
            
            data = [(str(i.get('token')), i.get('symbol'), i.get('name'), i.get('expiry'), i.get('strike'),
                     i.get('lotsize'), i.get('instrumenttype'), i.get('exch_seg'), i.get('tick_size'))
                    for i in json_data if i.get('token')]
            
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", data)
            cursor.execute("CREATE INDEX idx_sym ON symbols(symbol)")
            cursor.execute("CREATE INDEX idx_tok ON symbols(token)")
            conn.commit()
            conn.close()
            
            with open(temp_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", 
                    file=f.read(),
                    file_options={"x-upsert": "true", "content-type": "application/octet-stream"}
                )
            os.remove(temp_path)
            print("✅ [System] Master DB Synced & Uploaded.")
            return True
    except Exception as e:
        print(f"❌ [Master Error] {e}")
        return False

# ==============================================================================
# --- 4. DATA BROADCASTER (P2P ROUTING) ---
# ==============================================================================
def auto_batch_broadcaster():
    global live_data_queue, token_masters
    while True:
        if live_data_queue:
            all_tokens = list(live_data_queue.keys())
            # Logic: Chunk processing to prevent CPU throttling on Render
            for i in range(0, len(all_tokens), 300):
                batch = all_tokens[i : i + 300]
                for token in batch:
                    payload = live_data_queue.pop(token, None)
                    if payload:
                        # P2P Logic: If Master exists, only send to Master
                        if token in token_masters and token_masters[token]:
                            master_sid = token_masters[token][0]
                            socketio.emit('live_update', payload, room=master_sid)
                        else:
                            socketio.emit('live_update', payload, room=token)
                eventlet.sleep(0.02)
        else:
            eventlet.sleep(0.1)

# ==============================================================================
# --- 5. TICK ENGINE (WS RECEIVER) ---
# ==============================================================================
def on_data(wsapp, msg):
    global live_data_queue, last_tick_time, previous_price
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            curr_time = time.time()
            
            # Logic: 1s Throttling to save server bandwidth
            if token in last_tick_time and (curr_time - last_tick_time[token]) < 1.0:
                return
            
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
            live_data_queue[token] = payload

            # Memory Safety
            if len(last_tick_time) > 3000:
                last_tick_time.clear(); previous_price.clear(); gc.collect()
    except Exception as e:
        print(f"⚠️ [WS Data Error] {e}")

# ==============================================================================
# --- 6. SELF-HEALING ENGINE ---
# ==============================================================================
def run_trading_engine():
    global sws, is_ws_ready, subscribed_tokens_set, last_master_update_date
    refresh_supabase_master()
    
    while True:
        try:
            now = datetime.datetime.now(IST)
            if now.hour == 8 and 35 <= now.minute <= 45 and last_master_update_date != now.date():
                if refresh_supabase_master(): last_master_update_date = now.date()

            if 7 <= now.hour < 24:
                if not is_ws_ready:
                    smart_api = SmartConnect(api_key=API_KEY)
                    totp = pyotp.TOTP(TOTP_STR).now()
                    session = smart_api.generateSession(CLIENT_CODE, MPIN, totp)
                    
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('🟢 WebSocket Connected')")
                        sws.on_error = lambda ws, err: print(f"❌ WS Error: {err}")
                        sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                        sws.connect()
            else:
                if is_ws_ready:
                    sws.close(); is_ws_ready = False; subscribed_tokens_set.clear()
        except Exception as e:
            print(f"🔴 [Loop Error] {e}")
            is_ws_ready = False
        eventlet.sleep(20)

# ==============================================================================
# --- 7. SUBSCRIPTION & P2P ---
# ==============================================================================
@socketio.on('subscribe')
def handle_subscribe(json_data):
    global subscribed_tokens_set, sws, is_ws_ready, token_masters
    watchlist = json_data.get('watchlist', [])
    if not watchlist: return

    batches = {1: [], 2: [], 3: [], 4: [], 5: []}
    for item in watchlist:
        token = str(item.get('token'))
        exch = str(item.get('exch', 'NSE')).upper()
        symbol = str(item.get('symbol', '')).upper()
        if not token or token in ["None", ""]: continue

        join_room(token)
        if token not in token_masters: token_masters[token] = []
        if request.sid not in token_masters[token]: token_masters[token].append(request.sid)

        # Logic: Nominate P2P Master
        if len(token_masters[token]) > 1:
            emit('p2p_assign', {'token': token, 'master_sid': token_masters[token][0]}, room=request.sid)

        if token not in subscribed_tokens_set:
            if "MCX" in exch: etype = 5
            elif "NFO" in exch or any(x in symbol for x in ["CE", "PE", "FUT"]): etype = 2
            elif "BFO" in exch: etype = 4
            elif "BSE" in exch: etype = 3
            else: etype = 1 
            batches[etype].append(token)

    if is_ws_ready and sws:
        for etype, tokens in batches.items():
            for i in range(0, len(tokens), 50):
                chunk = tokens[i : i + 50]
                sws.subscribe(f"myt_{etype}_{time.time()}", 1, [{"exchangeType": etype, "tokens": chunk}])
                for t in chunk: subscribed_tokens_set.add(t)
                eventlet.sleep(0.3)

# ==============================================================================
# --- 8. P2P SIGNALING ---
# ==============================================================================
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
    for token in list(token_masters.keys()):
        if request.sid in token_masters[token]:
            token_masters[token].remove(request.sid)
            if not token_masters[token]: del token_masters[token]

# ==============================================================================
# --- 9. API & BOOTSTRAP ---
# ==============================================================================
@app.route('/history')
def get_history():
    token = request.args.get('token')
    exch = request.args.get('exch', 'NSE').upper()
    try:
        smart_api = SmartConnect(api_key=API_KEY)
        smart_api.generateSession(CLIENT_CODE, MPIN, pyotp.TOTP(TOTP_STR).now())
        res = smart_api.getCandleData({
            "exchange": exch, "symboltoken": token, "interval": "FIVE_MINUTE", 
            "fromdate": (datetime.datetime.now(IST) - datetime.timedelta(days=90)).strftime('%Y-%m-%d %H:%M'),
            "todate": datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')
        })
        if res.get('status'):
            return jsonify([{"time": int(datetime.datetime.strptime(c[0], "%Y-%m-%dT%H:%M:%S%z").timestamp()), 
                             "open": c[1], "high": c[2], "low": c[3], "close": c[4]} for c in res['data']])
        return jsonify({"error": "No data"}), 404
    except Exception as e: return str(e), 500

@app.route('/')
def health():
    return {"engine": "READY" if is_ws_ready else "OFFLINE", "tokens": len(subscribed_tokens_set), "rooms": len(token_masters)}, 200

if __name__ == '__main__':
    socketio.start_background_task(auto_batch_broadcaster)
    socketio.start_background_task(run_trading_engine)
    port = int(os.environ.get("PORT", 10000))
    socketio.run(app, host='0.0.0.0', port=port)
