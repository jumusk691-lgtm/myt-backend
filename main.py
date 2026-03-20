import eventlet
eventlet.monkey_patch() 

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
from flask_socketio import SocketIO, join_room, emit

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

# WebSocket optimize kiya taaki packet drop na ho
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='eventlet', 
    ping_timeout=120, 
    ping_interval=40, 
    max_http_buffer_size=50000000 
)

# ==============================================================================
# --- 2. GLOBAL ENGINE STATE (BATCHING & P2P MIXED) ---
# ==============================================================================
sws = None
is_ws_ready = False
subscribed_tokens_set = set()      
global_market_cache = {}           # Saare 500+ symbols ka data yahan jama hota hai
last_tick_time = {}                
previous_price = {}                
active_peers = {}                  # P2P users tracking
last_master_update_date = None

# ==============================================================================
# --- 3. NETWORK & DNS STABILITY ---
# ==============================================================================
def check_dns(host="apiconnect.angelone.in"):
    try:
        socket.gethostbyname(host)
        return True
    except socket.gaierror:
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
# --- 5. ULTRA BROADCASTER (BATCHING + P2P LOGIC) ---
# ==============================================================================
def mass_broadcaster_engine():
    """Ye loop har 1 second mein 500 symbols ka MEGA PACKET sabko bhejta hai"""
    global global_market_cache, active_peers
    print("🚀 [Engine] Mass Broadcaster Engine Started...")
    
    while True:
        try:
            if global_market_cache:
                # Mega Packet taiyar karo (Batching)
                mega_packet = list(global_market_cache.values())
                
                # P2P Hybrid Logic: Agar users 100 se zyada hain toh relay signal on karo
                if len(active_peers) > 100:
                    # Sirf first 20 'Super Nodes' ko direct data (Seeders)
                    seeders = list(active_peers.values())[:20]
                    for sid in seeders:
                        socketio.emit('market_snapshot', mega_packet, to=sid)
                    
                    # Baaki sab ko P2P ke liye signal bhejo
                    socketio.emit('p2p_relay_on', {"status": True})
                else:
                    # Sabko direct broadcast (Render Error Fixed: removed 'broadcast=True')
                    socketio.emit('market_snapshot', mega_packet)
                
                # Memory Maintenance
                if len(global_market_cache) > 2500:
                    global_market_cache.clear()
                    gc.collect()

            eventlet.sleep(1.0) # 1 second pulse
        except Exception as e:
            print(f"⚠️ [Broadcaster Error] {e}")
            eventlet.sleep(2)

# ==============================================================================
# --- 6. TICK ENGINE (BROKER LOGIC) ---
# ==============================================================================
def on_data(wsapp, msg):
    global global_market_cache, previous_price, last_tick_time
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            curr_time = time.time()
            
            # Throttling
            if token in last_tick_time and (curr_time - last_tick_time[token]) < 0.3:
                return
            
            ltp = float(msg.get('last_traded_price', 0)) / 100
            if ltp <= 0: return
            
            old_p = previous_price.get(token, "{:.2f}".format(ltp))

            # Batching cache mein update
            global_market_cache[token] = {
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
                global_market_cache[token]["pc"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

            previous_price[token] = "{:.2f}".format(ltp)
            last_tick_time[token] = curr_time
    except:
        pass

# ==============================================================================
# --- 7. HEALING TRADING ENGINE ---
# ==============================================================================
def run_trading_engine():
    global sws, is_ws_ready, subscribed_tokens_set, last_master_update_date
    
    eventlet.sleep(2)
    refresh_supabase_master()
    last_master_update_date = datetime.datetime.now(IST).date()
    
    while True:
        try:
            if not is_ws_ready:
                if not check_dns():
                    eventlet.sleep(10)
                    continue

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
                        print('💎 [Engine] WebSocket Connected!')
                        if subscribed_tokens_set:
                            t_list = list(subscribed_tokens_set)
                            for i in range(0, len(t_list), 50):
                                batch = t_list[i:i+50]
                                sws.subscribe(f"sub_{i}", 1, [{"exchangeType": 1, "tokens": batch}])
                                eventlet.sleep(0.2)

                    def on_close_wrapper(ws, code, reason):
                        global is_ws_ready
                        is_ws_ready = False

                    sws.on_data = on_data
                    sws.on_open = on_open_wrapper
                    sws.on_close = on_close_wrapper
                    sws.connect()
                else:
                    print(f"❌ Login Failed: {session.get('message', 'Credentials')}")
            
        except:
            is_ws_ready = False
        eventlet.sleep(20)

# ==============================================================================
# --- 8. SMART API & PEER TRACKING ---
# ==============================================================================
@socketio.on('connect')
def handle_connect():
    active_peers[request.sid] = request.sid
    print(f"📡 Peer Connected: {request.sid}")

@socketio.on('disconnect')
def handle_disconnect():
    if request.sid in active_peers:
        del active_peers[request.sid]

@socketio.on('subscribe')
def handle_subscribe(json_data):
    global subscribed_tokens_set, sws, is_ws_ready
    watchlist = json_data.get('watchlist', [])
    if not watchlist: return

    for item in watchlist:
        token = str(item.get('token'))
        if not token or token == "None": continue

        join_room(token)
        if token not in subscribed_tokens_set:
            subscribed_tokens_set.add(token)
            if is_ws_ready and sws:
                # Exchange logic for 500+ symbols
                exch = str(item.get('exch', 'NSE')).upper()
                etype = 5 if "MCX" in exch else 1
                sws.subscribe(f"s_{token}", 1, [{"exchangeType": etype, "tokens": [token]}])

@app.route('/')
def health():
    return {
        "status": "OPERATIONAL",
        "ws": is_ws_ready,
        "symbols": len(global_market_cache),
        "peers": len(active_peers)
    }, 200

@app.route('/history')
def get_candle_data():
    token = request.args.get('token')
    exch = request.args.get('exch', 'NSE').upper()
    if not token: return {"error": "Token required"}, 400
    try:
        smart_api = SmartConnect(api_key=API_KEY)
        smart_api.local_ip = "127.0.0.1"
        smart_api.public_ip = "1.1.1.1"
        totp = pyotp.TOTP(TOTP_STR).now()
        smart_api.generateSession(CLIENT_CODE, MPIN, totp)
        to_date = datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')
        from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=7)).strftime('%Y-%m-%d %H:%M')
        res = smart_api.getCandleData({"exchange": exch, "symboltoken": token, "interval": "ONE_MINUTE", "fromdate": from_date, "todate": to_date})
        return jsonify(res)
    except Exception as e:
        return {"error": str(e)}, 500

if __name__ == '__main__':
    # Start both background engines
    socketio.start_background_task(mass_broadcaster_engine)
    socketio.start_background_task(run_trading_engine)
    
    port = int(os.environ.get("PORT", 10000))
    import eventlet.wsgi
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
