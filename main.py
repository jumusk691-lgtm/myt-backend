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
import sys
import socket
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from flask import Flask, send_file, request, after_this_request, jsonify
from flask_socketio import SocketIO, join_room, leave_room, emit

# ==============================================================================
# --- 1. CONFIGURATION (PRO LEVEL) ---
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

# WebSocket optimization for high-speed packet delivery
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='eventlet', 
    ping_timeout=120, 
    ping_interval=40, 
    max_http_buffer_size=50000000 
)

# ==============================================================================
# --- 2. GLOBAL ENGINE STATE (BATCHING, ROOMS & P2P) ---
# ==============================================================================
sws = None
is_ws_ready = False
subscribed_tokens_set = set()      
global_market_cache = {}           # High-speed cache for all symbols
last_tick_time = {}                
previous_price = {}                
active_peers = {}                  # For P2P Relay tracking
last_master_update_date = None
room_members = {}                  # Tracking members in each token room
user_scores = {}                   # Track score as requested

# ==============================================================================
# --- 3. NETWORK & DNS STABILITY (FIX FOR RENDER ERROR) ---
# ==============================================================================
def check_dns(host="apiconnect.angelone.in"):
    """Bhai, yeh function Render ke DNS timeout error ko handle karega"""
    try:
        socket.gethostbyname(host)
        return True
    except socket.gaierror:
        print(f"⚠️ [Network] DNS Lookup failed for {host}. Retrying...")
        return False

# ==============================================================================
# --- 4. MASTER DATA ENGINE (BROKER GRADE) ---
# ==============================================================================
def refresh_supabase_master():
    print(f"🔄 [System] {datetime.datetime.now(IST)}: Full Master Data Sync...")
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
            print("✅ [System] Master DB Synced and Indexed.")
            return True
    except Exception as e:
        print(f"❌ [Master Error] {e}")
        return False

# ==============================================================================
# --- 5. HYBRID BROADCASTER ENGINE ---
# ==============================================================================
def mass_broadcaster_engine():
    global global_market_cache, active_peers
    print("🚀 [Engine] Hybrid Broadcaster Engine Started...")
    
    while True:
        try:
            if global_market_cache:
                current_cache = dict(global_market_cache)
                global_market_cache.clear()
                
                # Room-based emission: Sirf unhe jayega jo us token ko subscribe kiye hain
                for token, payload in current_cache.items():
                    socketio.emit('live_update', payload, to=token)
                
                # Cleanup logic
                if len(previous_price) > 3000:
                    previous_price.clear()
                    last_tick_time.clear()
                    gc.collect()

            eventlet.sleep(0.4) # 400ms broadcast pulse
        except Exception as e:
            print(f"⚠️ [Broadcaster Error] {e}")
            eventlet.sleep(1)

# ==============================================================================
# --- 6. TICK ENGINE (ON_DATA PROCESSING) ---
# ==============================================================================
def on_data(wsapp, msg):
    global global_market_cache, previous_price, last_tick_time
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            curr_time = time.time()
            
            # Anti-flood check (0.2s throttle)
            if token in last_tick_time and (curr_time - last_tick_time[token]) < 0.2:
                return
            
            ltp = float(msg.get('last_traded_price', 0)) / 100
            if ltp <= 0: return
            
            old_p = previous_price.get(token, "{:.2f}".format(ltp))

            payload = {
                "t": token, 
                "p": "{:.2f}".format(ltp), 
                "lp": old_p,
                "h": "{:.2f}".format(float(msg.get('high', 0)) / 100),
                "l": "{:.2f}".format(float(msg.get('low', 0)) / 100),
                "v": msg.get('volume', 0),
                "o": "{:.2f}".format(float(msg.get('open', 0)) / 100)
            }
            
            if 'close' in msg and float(msg['close']) > 0:
                cp = float(msg['close']) / 100
                payload["pc"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

            global_market_cache[token] = payload
            previous_price[token] = "{:.2f}".format(ltp)
            last_tick_time[token] = curr_time
    except:
        pass

# ==============================================================================
# --- 7. HEALING TRADING ENGINE (AUTO-RECOVERY LOGIC) ---
# ==============================================================================
def run_trading_engine():
    global sws, is_ws_ready, subscribed_tokens_set, last_master_update_date
    
    eventlet.sleep(2)
    refresh_supabase_master()
    
    while True:
        try:
            if not is_ws_ready:
                # DNS Check: Render network stabilize hone ka wait karega
                if not check_dns():
                    eventlet.sleep(5)
                    continue

                print("🔄 [Engine] Connecting to Angel One...")
                smart_api = SmartConnect(api_key=API_KEY)
                smart_api.local_ip = "127.0.0.1" 
                smart_api.public_ip = "1.1.1.1"
                
                totp = pyotp.TOTP(TOTP_STR).now()
                session = smart_api.generateSession(CLIENT_CODE, MPIN, totp)
                
                if session and session.get('status'):
                    sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                    
                    def on_open_wrapper(ws):
                        global is_ws_ready
                        is_ws_ready = True
                        print('💎 [Engine] WebSocket Connected & Authorized!')
                        # Re-subscribe tokens on reconnect
                        if subscribed_tokens_set:
                            tokens = list(subscribed_tokens_set)
                            for i in range(0, len(tokens), 50):
                                batch = tokens[i:i+50]
                                sws.subscribe(f"restore_{i}", 1, [{"exchangeType": 1, "tokens": batch}])
                                eventlet.sleep(0.2)

                    def on_close_wrapper(ws, code, reason):
                        global is_ws_ready
                        is_ws_ready = False
                        print(f'⚠️ [Engine] WS Closed: {reason}')

                    sws.on_data = on_data
                    sws.on_open = on_open_wrapper
                    sws.on_close = on_close_wrapper
                    sws.connect()
                else:
                    print(f"❌ [Login] Failed: {session.get('message')}")
            
        except Exception as e:
            print(f"⚙️ [Engine Error] {e}")
            is_ws_ready = False
        eventlet.sleep(15)

# ==============================================================================
# --- 8. SOCKET HANDLERS (ROOMS & SCORE) ---
# ==============================================================================
@socketio.on('connect')
def handle_connect():
    active_peers[request.sid] = time.time()
    print(f"📡 New Connection: {request.sid}")

@socketio.on('disconnect')
def handle_disconnect():
    if request.sid in active_peers:
        del active_peers[request.sid]
    print(f"🔌 Disconnected: {request.sid}")

@socketio.on('subscribe')
def handle_subscribe(json_data):
    global subscribed_tokens_set, sws, is_ws_ready, user_scores
    watchlist = json_data.get('watchlist', [])
    if not watchlist: return

    # Score Logic: Bhai, yahan score update hoga subscription par
    sid = request.sid
    user_scores[sid] = user_scores.get(sid, 0) + len(watchlist)

    for item in watchlist:
        token = str(item.get('token'))
        exch = str(item.get('exch', 'NSE')).upper()
        if not token or token == "None": continue

        join_room(token)
        
        if token not in subscribed_tokens_set:
            subscribed_tokens_set.add(token)
            if is_ws_ready and sws:
                etype = 5 if "MCX" in exch else (2 if "NFO" in exch else 1)
                try:
                    sws.subscribe(f"sub_{token}", 1, [{"exchangeType": etype, "tokens": [token]}])
                except: pass

@socketio.on('unsubscribe')
def handle_unsubscribe(json_data):
    token = str(json_data.get('token'))
    if token:
        leave_room(token)
        print(f"🚪 Left Room: {token}")

# ==============================================================================
# --- 9. REST API ROUTES ---
# ==============================================================================
@app.route('/')
def health():
    return {
        "status": "OPERATIONAL",
        "ws_active": is_ws_ready,
        "subscribed_count": len(subscribed_tokens_set),
        "peers": len(active_peers),
        "timestamp": datetime.datetime.now(IST).isoformat()
    }, 200

@app.route('/score')
def get_user_score():
    return jsonify(user_scores), 200

@app.route('/history')
def get_candle_data():
    token = request.args.get('token')
    exch = request.args.get('exch', 'NSE').upper()
    try:
        smart_api = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_STR).now()
        smart_api.generateSession(CLIENT_CODE, MPIN, totp)
        
        to_date = datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')
        from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=7)).strftime('%Y-%m-%d %H:%M')
        
        res = smart_api.getCandleData({
            "exchange": exch, "symboltoken": token, "interval": "ONE_MINUTE",
            "fromdate": from_date, "todate": to_date
        })
        return jsonify(res)
    except Exception as e:
        return {"error": str(e)}, 500

# ==============================================================================
# --- 10. MAIN EXECUTION ---
# ==============================================================================
if __name__ == '__main__':
    socketio.start_background_task(mass_broadcaster_engine)
    socketio.start_background_task(run_trading_engine)
    
    port = int(os.environ.get("PORT", 10000))
    import eventlet.wsgi
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
