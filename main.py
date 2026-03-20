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

# ==========================================
# --- 1. CONFIGURATION & RENDER STABILITY ---
# ==========================================
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# Supabase Credentials
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

app = Flask(__name__)

# CRITICAL: 50MB Buffer for handling 10,000+ tokens and stable WebRTC/P2P signaling
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='eventlet', 
    ping_timeout=120, 
    ping_interval=40, 
    max_http_buffer_size=50000000
)

# ==========================================
# --- 2. GLOBAL STATE & MEMORY MANAGEMENT ---
# ==========================================
sws = None
is_ws_ready = False
subscribed_tokens_set = set()      # Global Pooling to avoid duplicate subs
token_masters = {}                # P2P Master-Slave Tracking {token: [list_of_sids]}
live_data_queue = {}               # High-speed buffer queue
last_tick_time = {}                # 1s Throttling tracker
previous_price = {}                # History tracker for Red/Green APK logic
last_master_update_date = None

# ==========================================
# --- 3. MASTER DATA ENGINE (INDEXED DB) ---
# ==========================================
def refresh_supabase_master():
    """
    Logic: Fetches 60,000+ symbols, creates an indexed SQLite DB, 
    and uploads to Supabase for APK search performance.
    """
    print(f"🔄 [System] {datetime.datetime.now(IST)}: Starting Master Data Sync...")
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
            
            # Creating Table Structure
            cursor.execute("DROP TABLE IF EXISTS symbols")
            cursor.execute('''CREATE TABLE symbols (
                                token TEXT, symbol TEXT, name TEXT, expiry TEXT,
                                strike TEXT, lotsize TEXT, instrumenttype TEXT, 
                                exch_seg TEXT, tick_size TEXT)''')
            
            # Heavy Data Insertion
            data = [(str(i.get('token')), i.get('symbol'), i.get('name'), i.get('expiry'), i.get('strike'),
                     i.get('lotsize'), i.get('instrumenttype'), i.get('exch_seg'), i.get('tick_size'))
                    for i in json_data if i.get('token')]
            
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", data)
            
            # Optimization: Indexing for APK search speed
            cursor.execute("CREATE INDEX idx_sym ON symbols(symbol)")
            cursor.execute("CREATE INDEX idx_tok ON symbols(token)")
            
            conn.commit()
            conn.close()
            
            # Uploading to Supabase Storage
            with open(temp_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", 
                    file=f.read(),
                    file_options={"x-upsert": "true", "content-type": "application/octet-stream"}
                )
            
            os.remove(temp_path)
            print("✅ [System] Indexed Master DB Uploaded successfully.")
            return True
        else:
            print(f"⚠️ [System] Angel One Master API returned status: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ [Critical] Master DB Error: {str(e)}")
        return False

# ==========================================
# --- 4. DATA BROADCASTER (P2P ROUTING) ---
# ==========================================
def auto_batch_broadcaster():
    """
    Logic: This is the 'Police' thread. It takes data from the queue,
    checks if a 'Master' exists for the token, and routes data accordingly.
    """
    global live_data_queue, token_masters
    print("🚀 [Broadcaster] P2P & Queue Worker Started.")
    
    while True:
        if live_data_queue:
            all_tokens = list(live_data_queue.keys())
            
            # Logic: Processing in chunks of 300 to prevent CPU spikes on Render
            for i in range(0, len(all_tokens), 300):
                batch = all_tokens[i : i + 300]
                for token in batch:
                    payload = live_data_queue.pop(token, None)
                    if payload:
                        # P2P LOGIC: If multiple users want the same token, 
                        # send ONLY to Master to save server bandwidth.
                        if token in token_masters and token_masters[token]:
                            master_sid = token_masters[token][0]
                            socketio.emit('live_update', payload, room=master_sid)
                        else:
                            # Fallback: Broadcast to all in the room
                            socketio.emit('live_update', payload, room=token)
                
                # Small micro-sleep to keep network buffers clean
                eventlet.sleep(0.02)
        else:
            eventlet.sleep(0.1)

# ==========================================
# --- 5. TICK ENGINE (ANGEL ONE RECEIVER) ---
# ==========================================
def on_data(wsapp, msg):
    """
    Logic: Receives binary/dict data from Angel One, throttles it to 1s,
    calculates percentage change, and pushes to queue.
    """
    global live_data_queue, last_tick_time, previous_price
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            curr_time = time.time()
            
            # Logic: Strictly 1 tick per second per token (Render Free Tier Stability)
            if token in last_tick_time and (curr_time - last_tick_time[token]) < 1.0:
                return
            
            ltp = float(msg.get('last_traded_price', 0)) / 100
            if ltp <= 0: return

            # Logic: Track 1-sec previous price for APK Red/Green color logic
            old_p = previous_price.get(token, "{:.2f}".format(ltp))

            payload = {
                "t": token,
                "p": "{:.2f}".format(ltp),      # Current LTP
                "lp": old_p,                    # Last Price (History)
                "h": "{:.2f}".format(float(msg.get('high', 0)) / 100),
                "l": "{:.2f}".format(float(msg.get('low', 0)) / 100),
                "v": msg.get('volume', 0)
            }
            
            # Logic: Percentage Change calculation
            if 'close' in msg and float(msg['close']) > 0:
                cp = float(msg['close']) / 100
                payload["pc"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

            # Update State
            previous_price[token] = "{:.2f}".format(ltp)
            last_tick_time[token] = curr_time
            
            # Push to processing queue
            live_data_queue[token] = payload

            # Memory Safety Logic: Flush history if it exceeds 2000 tokens
            if len(last_tick_time) > 2000:
                last_tick_time.clear()
                previous_price.clear()
                gc.collect()

    except Exception as e:
        print(f"⚠️ [Tick Error] {e}")

# ==========================================
# --- 6. SELF-HEALING ENGINE LOOP ---
# ==========================================
def run_trading_engine():
    """
    Logic: Maintains WebSocket connection. Handles market hours.
    Auto-reconnects if Render drops the connection.
    """
    global sws, is_ws_ready, subscribed_tokens_set, last_master_update_date
    
    # Initial DB Refresh
    refresh_supabase_master()
    
    while True:
        try:
            now = datetime.datetime.now(IST)
            
            # Logic: Scheduled Master Update (8:35 AM IST)
            if now.hour == 8 and 35 <= now.minute <= 45 and last_master_update_date != now.date():
                if refresh_supabase_master():
                    last_master_update_date = now.date()

            # Logic: Connectivity during Market Hours (including pre/post)
            if 7 <= now.hour < 24:
                if not is_ws_ready:
                    print("🔄 [System] Initializing SmartConnect Session...")
                    smart_api = SmartConnect(api_key=API_KEY)
                    totp = pyotp.TOTP(TOTP_STR).now()
                    
                    # Logic: Fixed MPIN implementation
                    session = smart_api.generateSession(CLIENT_CODE, MPIN, totp)
                    
                    if session.get('status'):
                        print("🟢 [System] Session Generated. Opening WebSocket...")
                        sws = SmartWebSocketV2(
                            session['data']['jwtToken'], 
                            API_KEY, CLIENT_CODE, 
                            session['data']['feedToken']
                        )
                        
                        sws.on_data = on_data
                        sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('💎 WebSocket Connected!')")
                        sws.on_error = lambda ws, err: print(f"❌ [WS Error] {err}")
                        sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                        
                        sws.connect()
                    else:
                        print(f"❌ [Session Error] {session.get('message')}")
            else:
                # Logic: Outside market hours, close to save resources
                if is_ws_ready:
                    print("😴 [System] Market Closed. Sleeping...")
                    sws.close()
                    is_ws_ready = False
                    subscribed_tokens_set.clear()
        
        except Exception as e:
            print(f"🔴 [Critical Loop Error] {e}")
            is_ws_ready = False
        
        eventlet.sleep(20)

# ==========================================
# --- 7. SMART SUBSCRIPTION & P2P LOGIC ---
# ==========================================
@socketio.on('subscribe')
def handle_subscribe(json_data):
    """
    Logic: Handles incoming watchlists. 
    Implements P2P Master-Slave logic and Batched Angel One Subscriptions.
    """
    global subscribed_tokens_set, sws, is_ws_ready, token_masters
    
    watchlist = json_data.get('watchlist', [])
    if not watchlist: return

    # Batching buckets for different exchange types
    batches = {1: [], 2: [], 3: [], 4: [], 5: []}

    for item in watchlist:
        token = str(item.get('token'))
        exch = str(item.get('exch', 'NSE')).upper()
        symbol = str(item.get('symbol', '')).upper()
        
        if not token or token in ["None", ""]: continue

        # Logic: User enters the 'token room'
        join_room(token)

        # --- P2P MASTER LOGIC ---
        if token not in token_masters:
            token_masters[token] = []
        
        if request.sid not in token_masters[token]:
            token_masters[token].append(request.sid)

        # Logic: If a master already exists, tell this client to be a slave
        if len(token_masters[token]) > 1:
            emit('p2p_assign', {
                'token': token, 
                'master_sid': token_masters[token][0]
            }, room=request.sid)

        # --- GLOBAL POOLING LOGIC ---
        if token not in subscribed_tokens_set:
            # Determining Exchange Type for Angel One
            if "MCX" in exch: etype = 5
            elif "NFO" in exch or any(x in symbol for x in ["CE", "PE", "FUT"]): etype = 2
            elif "BFO" in exch: etype = 4
            elif "BSE" in exch: etype = 3
            else: etype = 1 
            
            batches[etype].append(token)

    # Logic: Subscription Execution in chunks of 50 (Angel One Limit)
    if is_ws_ready and sws:
        for etype, tokens in batches.items():
            if tokens:
                for i in range(0, len(tokens), 50):
                    chunk = tokens[i : i + 50]
                    try:
                        sws.subscribe(f"myt_sub_{etype}_{time.time()}", 1, [{"exchangeType": etype, "tokens": chunk}])
                        for t in chunk: subscribed_tokens_set.add(t)
                        print(f"📦 Subscribed {len(chunk)} tokens (Exch: {etype})")
                        eventlet.sleep(0.3)
                    except Exception as e:
                        print(f"Sub Error: {e}")

# ==========================================
# --- 8. P2P SIGNALING (WEBRTC BRIDGE) ---
# ==========================================
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
    # Cleanup token masters to allow other users to take the lead
    for token in list(token_masters.keys()):
        if request.sid in token_masters[token]:
            token_masters[token].remove(request.sid)
            if not token_masters[token]:
                del token_masters[token]

# ==========================================
# --- 9. API ROUTES (HISTORY & HEALTH) ---
# ==========================================
@app.route('/history')
def get_history():
    """
    Logic: Fetch 90-day candle data for APK charts.
    """
    token = request.args.get('token')
    exch = request.args.get('exch', 'NSE').upper()
    if not token: return "Missing Token", 400
    
    try:
        smart_api = SmartConnect(api_key=API_KEY)
        smart_api.generateSession(CLIENT_CODE, MPIN, pyotp.TOTP(TOTP_STR).now())
        
        to_date = datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')
        from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=90)).strftime('%Y-%m-%d %H:%M')
        
        res = smart_api.getCandleData({
            "exchange": exch, 
            "symboltoken": token, 
            "interval": "FIVE_MINUTE", 
            "fromdate": from_date, 
            "todate": to_date
        })
        
        if res.get('status'):
            formatted_data = [
                {
                    "time": int(datetime.datetime.strptime(c[0], "%Y-%m-%dT%H:%M:%S%z").timestamp()), 
                    "open": c[1], "high": c[2], "low": c[3], "close": c[4]
                } for c in res['data']
            ]
            return jsonify(formatted_data)
        return jsonify({"error": "No data found"}), 404
    except Exception as e:
        return str(e), 500

@app.route('/')
def health():
    return {
        "engine": "READY" if is_ws_ready else "OFFLINE",
        "active_tokens": len(subscribed_tokens_set),
        "connected_clients": len(token_masters),
        "queue_size": len(live_data_queue),
        "timestamp": datetime.datetime.now(IST).isoformat()
    }, 200

# ==========================================
# --- 10. MAIN EXECUTION ---
# ==========================================
if __name__ == '__main__':
    # Start Background Tasks
    socketio.start_background_task(auto_batch_broadcaster)
    socketio.start_background_task(run_trading_engine)
    
    # Render Port Binding
    port = int(os.environ.get("PORT", 10000))
    print(f"🔥 [Server] TradeND Engine Starting on Port {port}...")
    socketio.run(app, host='0.0.0.0', port=port)
