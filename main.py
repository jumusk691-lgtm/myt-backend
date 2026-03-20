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
token_masters = {}                
live_data_queue = {}               
last_tick_time = {}                
previous_price = {}                
last_master_update_date = None

# ==============================================================================
# --- 3. DNS & IP HELPERS (STABILITY FIX) ---
# ==============================================================================
def check_dns(host="apiconnect.angelone.in"):
    try:
        socket.gethostbyname(host)
        return True
    except socket.gaierror:
        return False

# ==============================================================================
# --- 4. MASTER DATA ENGINE ---
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
            print("✅ [System] Master DB Synced Successfully.")
            return True
    except Exception as e:
        print(f"❌ [Master Error] {e}")
        return False

# ==============================================================================
# --- 5. DATA BROADCASTER ---
# ==============================================================================
def auto_batch_broadcaster():
    global live_data_queue, last_tick_time, previous_price
    last_clean_time = time.time()
    
    while True:
        try:
            if live_data_queue:
                current_items = list(live_data_queue.items())
                live_data_queue.clear() 

                for token, payload in current_items:
                    socketio.emit('live_update', payload, to=token)
            
            if time.time() - last_clean_time > 5:
                if len(last_tick_time) > 1500:
                    last_tick_time.clear()
                    previous_price.clear()
                gc.collect() 
                last_clean_time = time.time()
                
            eventlet.sleep(0.1) 
        except:
            eventlet.sleep(1)

# ==============================================================================
# --- 6. TICK ENGINE ---
# ==============================================================================
def on_data(wsapp, msg):
    global live_data_queue, last_tick_time, previous_price
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            curr_time = time.time()
            
            if token in last_tick_time and (curr_time - last_tick_time[token]) < 0.5:
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
    except:
        pass

# ==============================================================================
# --- 7. SELF-HEALING ENGINE (IP TIMEOUT FIX) ---
# ==============================================================================
def run_trading_engine():
    global sws, is_ws_ready, subscribed_tokens_set, last_master_update_date
    
    eventlet.sleep(5)
    refresh_supabase_master()
    last_master_update_date = datetime.datetime.now(IST).date()
    
    while True:
        try:
            if not is_ws_ready:
                if not check_dns():
                    eventlet.sleep(10)
                    continue

                # SmartConnect logic with forced timeout handling
                smart_api = SmartConnect(api_key=API_KEY)
                try:
                    # Manually setting a placeholder local IP to bypass ipify check failures
                    smart_api.local_ip = "127.0.0.1" 
                    smart_api.public_ip = "1.1.1.1"
                    
                    totp = pyotp.TOTP(TOTP_STR).now()
                    session = smart_api.generateSession(CLIENT_CODE, MPIN, totp)
                    
                    if session and session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        
                        def on_open_wrapper(ws):
                            global is_ws_ready
                            is_ws_ready = True
                            print('🟢 [Engine] WebSocket Live!')

                        def on_close_wrapper(ws, code, reason):
                            global is_ws_ready
                            is_ws_ready = False
                            print('🔴 [Engine] WS Disconnected.')

                        sws.on_data = on_data
                        sws.on_open = on_open_wrapper
                        sws.on_close = on_close_wrapper
                        sws.connect()
                    else:
                        print(f"❌ Login Error: {session.get('message', 'Timeout')}")
                except Exception as e:
                    print(f"⚠️ Session Fail: {str(e)[:50]}")
            
        except:
            is_ws_ready = False
        eventlet.sleep(20)

# ==============================================================================
# --- 8. EVENTS & API ---
# ==============================================================================
@socketio.on('subscribe')
def handle_subscribe(json_data):
    global subscribed_tokens_set, sws, is_ws_ready
    watchlist = json_data.get('watchlist', [])
    if not watchlist: return

    batches = {1: [], 2: [], 3: [], 4: [], 5: []}
    for item in watchlist:
        token = str(item.get('token'))
        exch = str(item.get('exch', 'NSE')).upper()
        symbol = str(item.get('symbol', '')).upper()
        if not token: continue

        join_room(token)
        if token not in subscribed_tokens_set:
            if "MCX" in exch: etype = 5
            elif "BFO" in exch or "SENSEX" in symbol: etype = 4
            elif "BSE" in exch: etype = 3
            elif "NFO" in exch or any(x in symbol for x in ["CE", "PE", "FUT"]): etype = 2
            else: etype = 1 
            batches[etype].append(token)

    if is_ws_ready and sws:
        for etype, tokens in batches.items():
            if tokens:
                try:
                    sws.subscribe(f"sub_{time.time()}", 1, [{"exchangeType": etype, "tokens": tokens}])
                    for t in tokens: subscribed_tokens_set.add(t)
                    eventlet.sleep(0.1)
                except: pass

@app.route('/')
def health():
    return {"status": "LIVE", "ws": is_ws_ready, "tokens": len(subscribed_tokens_set)}, 200

if __name__ == '__main__':
    socketio.start_background_task(auto_batch_broadcaster)
    socketio.start_background_task(run_trading_engine)
    
    port = int(os.environ.get("PORT", 10000))
    import eventlet.wsgi
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
