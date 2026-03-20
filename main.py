import eventlet
eventlet.monkey_patch(all=True) 

import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from flask import Flask, request
from flask_socketio import SocketIO, join_room, emit

# --- 1. CONFIG ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRnbHV6c2xqYnhrZG93cWFwamhvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzMwNjI0NDcsImV4cCI6MjA4ODYzODQ0N30.5dvATkqcnVn7FgJcmhcjpJsOANZxrALhKQoFaQTdzHY"
BUCKET_NAME = "Myt"

app = Flask(__name__)
# High Buffer for 6000 symbols
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', max_http_buffer_size=50000000)

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
subscribed_tokens_set = set() 
last_master_update_date = None
# NEW: For P2P & Grouping
token_masters = {} 
live_data_queue = {} 

# --- 2. MASTER DATA SYNC (UNCHANGED) ---
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
            conn.commit()
            conn.close()
            with open(temp_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(path="angel_master.db", file=f.read(),
                                                         file_options={"x-upsert": "true", "content-type": "application/octet-stream"})
            os.remove(temp_path)
            return True
    except Exception as e:
        print(f"❌ Master Error: {e}")
        return False

# --- 3. DYNAMIC BATCHING & P2P WORKER (THE POLICE) ---
def auto_batch_broadcaster():
    global live_data_queue, token_masters
    while True:
        if live_data_queue:
            all_tokens = list(live_data_queue.keys())
            # Batching: 300 symbols at a time
            for i in range(0, len(all_tokens), 300):
                batch = all_tokens[i : i + 300]
                for token in batch:
                    payload = live_data_queue.pop(token, None)
                    if payload:
                        # P2P Logic: Check for Master User
                        if token in token_masters and token_masters[token]:
                            # Emit only to the "Master" of this token
                            # Master will then broadcast to others via P2P
                            master_sid = token_masters[token][0]
                            socketio.emit('live_update', payload, room=master_sid)
                        else:
                            # Fallback to room broadcast
                            socketio.emit('live_update', payload, room=token)
                
                # Small gap to prevent network congestion
                eventlet.sleep(0.05)
        else:
            eventlet.sleep(0.1)

# --- 4. TICK ENGINE (RECEIVER) ---
def on_data(wsapp, msg):
    global live_data_queue
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            ltp = float(msg.get('last_traded_price', 0)) / 100
            
            if ltp <= 0: return

            payload = {
                "t": token,
                "p": "{:.2f}".format(ltp)
            }
            
            if 'close' in msg and float(msg['close']) > 0:
                cp = float(msg['close']) / 100
                payload["pc"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

            # INSTEAD OF DIRECT EMIT, PUSH TO QUEUE
            live_data_queue[token] = payload
    except Exception as e:
        print(f"Tick Data Error: {e}")

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("🟢 Engine Live: Connected to Angel WebSocket")

# --- 5. CONNECTION MANAGER ---
def run_trading_engine():
    global sws, is_ws_ready, subscribed_tokens_set, last_master_update_date
    while True:
        try:
            now = datetime.datetime.now(IST)
            if now.hour == 8 and 30 <= now.minute <= 45 and last_master_update_date != now.date():
                if refresh_supabase_master(): last_master_update_date = now.date()

            if 7 <= now.hour < 24:
                if not is_ws_ready:
                    smart_api = SmartConnect(api_key=API_KEY)
                    totp = pyotp.TOTP(TOTP_STR).now()
                    session = smart_api.generateSession(CLIENT_CODE, PWD, totp)
                    
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        sws.on_open = on_open
                        sws.on_error = lambda ws, err: print(f"❌ WebSocket Error: {err}")
                        sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                        sws.connect()
            else:
                if is_ws_ready:
                    sws.close()
                    is_ws_ready = False
                    subscribed_tokens_set.clear()
        except Exception as e:
            print(f"Loop Error: {e}")
        eventlet.sleep(15)

# --- 6. SMART SUBSCRIBE WITH P2P SIGNALING ---
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
        
        if not token or token == "None" or token == "": continue

        join_room(token)

        # P2P LOGIC: Add user to potential master list
        if token not in token_masters: token_masters[token] = []
        if request.sid not in token_masters[token]:
            token_masters[token].append(request.sid)

        # Signaling: If there's already a master, tell this user to listen for P2P
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
            if tokens:
                for i in range(0, len(tokens), 50):
                    chunk = tokens[i:i+50]
                    sws.subscribe(f"myt_sub_{etype}", 1, [{"exchangeType": etype, "tokens": chunk}])
                    for t in chunk: subscribed_tokens_set.add(t)
                    eventlet.sleep(0.3)

@socketio.on('disconnect')
def on_disconnect():
    global token_masters
    for token in token_masters:
        if request.sid in token_masters[token]:
            token_masters[token].remove(request.sid)

@app.route('/')
def health():
    status = "READY" if is_ws_ready else "OFFLINE"
    return f"Engine: {status} | Tokens: {len(subscribed_tokens_set)} | Queue: {len(live_data_queue)}", 200

if __name__ == '__main__':
    socketio.start_background_task(auto_batch_broadcaster)
    socketio.start_background_task(run_trading_engine)
    port = int(os.environ.get("PORT", 10000))
    socketio.run(app, host='0.0.0.0', port=port)
