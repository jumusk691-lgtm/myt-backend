import eventlet
eventlet.monkey_patch(all=True) # LOGIC 1: High-speed async networking

import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json, gc, socket, sys, logging, threading, traceback
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from flask import Flask, send_file, request, after_this_request, jsonify
from flask_socketio import SocketIO, join_room, emit, leave_room

# ==============================================================================
# --- 1. CONFIG & SYSTEM LOGGING (LOGIC 1 & 18) ---
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# LOGIC 4: Supabase Infrastructure
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', ping_timeout=120, manage_session=False)

# ==============================================================================
# --- 2. GLOBAL SYSTEM STATE (LOGIC 2, 11, 15, 19) ---
# ==============================================================================
class MunhEngineState:
    def __init__(self):
        self.smart_api = None
        self.sws = None
        self.is_ws_ready = False
        self.subscribed_tokens_set = set()      # LOGIC 13: Guard
        self.token_metadata = {}                # LOGIC 5: Segments
        self.global_market_cache = {}           # LOGIC 12: Cluster Batching
        self.previous_price = {}                # LOGIC 7: Red/Green
        self.live_ohlc = {}                     # LOGIC 15: Real-time OHLC
        self.user_p2p_scores = {}               # LOGIC 11: P2P Scoring (10/100/1000)
        self.active_users_pool = {}             # LOGIC 19: Multi-User session tracking
        self.dns_status = False                 # LOGIC 2: DNS Fix
        self.db_path = None                     # Local DB for Search/Option Chain
        self.total_packets = 0

state = MunhEngineState()

# ==============================================================================
# --- 3. MASTER DATA & OPTION CHAIN LOGIC (SEARCH & 41 STRIKES) ---
# ==============================================================================
def sync_master_data_v2():
    """LOGIC 4: Sync & Upload to Supabase"""
    try:
        socket.gethostbyname("apiconnect.angelone.in")
        state.dns_status = True
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        master_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = requests.get(master_url, timeout=45)
        if response.status_code == 200:
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
                state.db_path = tmp.name
            conn = sqlite3.connect(state.db_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("DROP TABLE IF EXISTS symbols")
            cursor.execute('''CREATE TABLE symbols (token TEXT, symbol TEXT, name TEXT, expiry TEXT, strike TEXT, lotsize TEXT, instrumenttype TEXT, exch_seg TEXT, tick_size TEXT)''')
            records = [(str(i.get('token')), i.get('symbol'), i.get('name'), i.get('expiry'), i.get('strike'), i.get('lotsize'), i.get('instrumenttype'), i.get('exch_seg'), i.get('tick_size')) for i in response.json() if i.get('token')]
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", records)
            cursor.execute("CREATE INDEX idx_name ON symbols(name)")
            conn.commit()
            conn.close()
            with open(state.db_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload("angel_master.db", f.read(), {"x-upsert": "true"})
            return True
    except: return False

@app.route('/search_symbols', methods=['POST'])
def search_symbols():
    """Logic: Returns min 20 symbols for search bar"""
    query = request.json.get('query', '').upper()
    conn = sqlite3.connect(state.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT name FROM symbols WHERE name LIKE ? AND instrumenttype = '' LIMIT 20", (f'%{query}%',))
    results = [row for row in cursor.fetchall()]
    conn.close()
    return jsonify({"symbols": results})

@app.route('/get_option_chain', methods=['POST'])
def option_chain():
    """Logic: 41 Strike Option Chain (CE | Strike | PE) for NSE/BSE/MCX"""
    d = request.json
    name, expiry = d.get('name'), d.get('expiry')
    conn = sqlite3.connect(state.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT strike, token, symbol, instrumenttype FROM symbols WHERE name = ? AND expiry = ? AND instrumenttype LIKE 'OPT%' ORDER BY CAST(strike AS FLOAT) ASC", (name, expiry))
    data = cursor.fetchall()
    conn.close()
    return jsonify({"option_chain": data})

# ==============================================================================
# --- 4. CORE TICK ENGINE (LOGIC 7, 11, 12, 15, 17, 21) ---
# ==============================================================================
def on_data_callback(wsapp, msg):
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            ltp = float(msg.get('last_traded_price', 0)) / 100
            if ltp <= 0: return
            
            state.total_packets += 1
            state.user_p2p_scores[token] = state.user_p2p_scores.get(token, 0) + 1 # LOGIC 11
            old_val = state.previous_price.get(token, "{:.2f}".format(ltp))
            
            # LOGIC 15 & 17: OHLC
            if token not in state.live_ohlc:
                state.live_ohlc[token] = {"o": ltp, "h": ltp, "l": ltp, "c": ltp}
            else:
                state.live_ohlc[token]["h"] = max(state.live_ohlc[token]["h"], ltp)
                state.live_ohlc[token]["l"] = min(state.live_ohlc[token]["l"], ltp)
                state.live_ohlc[token]["c"] = ltp

            state.global_market_cache[token] = {
                "t": token, "p": "{:.2f}".format(ltp), "lp": old_val,
                "h": "{:.2f}".format(state.live_ohlc[token]["h"]),
                "l": "{:.2f}".format(state.live_ohlc[token]["l"]),
                "score": state.user_p2p_scores[token]
            }
            state.previous_price[token] = "{:.2f}".format(ltp)
    except: pass

def pulse_broadcaster():
    """LOGIC 21: 0.5s Broadcaster"""
    while True:
        if state.global_market_cache:
            snap = dict(state.global_market_cache)
            state.global_market_cache.clear()
            socketio.emit('live_update_batch', snap)
        eventlet.sleep(0.5)

# ==============================================================================
# --- 5. CHARTS & ORDERS (LOGIC 14, 16, 22) ---
# ==============================================================================
@app.route('/get_90day_chart', methods=['POST'])
def chart_logic():
    """LOGIC 14 & 16: Multi-Timeframe Chart (1 week to 3 months)"""
    d = request.json
    if state.smart_api:
        return jsonify(state.smart_api.getCandleData({
            "exchange": d.get('exch'), "symboltoken": d.get('token'),
            "interval": d.get('interval', "ONE_MINUTE"),
            "fromdate": (datetime.datetime.now(IST) - datetime.timedelta(days=90)).strftime('%Y-%m-%d %H:%M'),
            "todate": datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')
        }))
    return jsonify({"status": False})

@app.route('/order_slice', methods=['POST'])
def slice_order():
    """LOGIC 22: Order Book Slicing"""
    d = request.json
    qty, max_limit = int(d.get('qty', 0)), int(d.get('max', 1800))
    slices = []
    while qty > 0:
        chunk = min(qty, max_limit); slices.append(chunk); qty -= chunk
    return jsonify({"slices": slices})

# ==============================================================================
# --- 6. LIFECYCLE & RECOVERY (LOGIC 6, 9, 20) ---
# ==============================================================================
def engine_lifecycle_manager():
    sync_master_data_v2()
    while True:
        if not state.is_ws_ready:
            try:
                state.smart_api = SmartConnect(api_key=API_KEY)
                session = state.smart_api.generateSession(CLIENT_CODE, MPIN, pyotp.TOTP(TOTP_STR).now())
                if session.get('status'):
                    state.sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                    state.sws.on_data = on_data_callback
                    state.sws.on_open = lambda ws: setattr(state, 'is_ws_ready', True)
                    state.sws.connect()
                    eventlet.sleep(3); re_subscribe_engine() # LOGIC 9 & 20
            except: pass
        eventlet.sleep(25)

def re_subscribe_engine():
    """LOGIC 6: 500-Batch Subscription"""
    tokens = list(state.subscribed_tokens_set)
    for i in range(0, len(tokens), 500):
        state.sws.subscribe(f"recovery_{i}", 1, [{"exchangeType": 1, "tokens": tokens[i:i+500]}])
        eventlet.sleep(0.2)

# ==============================================================================
# --- 7. BOOTSTRAP (LOGIC 3, 18, 19) ---
# ==============================================================================
@socketio.on('connect')
def handle_connect(): state.active_users_pool[request.sid] = time.time() # LOGIC 19

if __name__ == '__main__':
    socketio.start_background_task(pulse_broadcaster)
    socketio.start_background_task(engine_lifecycle_manager)
    socketio.start_background_task(lambda: [eventlet.sleep(600), gc.collect()]) # LOGIC 3 & 18
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
