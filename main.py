import eventlet
eventlet.monkey_patch(all=True)

import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json, gc, socket, sys, logging, threading, traceback
from flask import Flask, send_file, request, after_this_request, jsonify
from flask_socketio import SocketIO, join_room, emit, leave_room
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client

# ==============================================================================
# --- 1. PRO-LEVEL LOGGING & CONFIGURATION ---
# ==============================================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(message)s', 
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

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
    manage_session=False, 
    max_http_buffer_size=100000000
)

# ==============================================================================
# --- 2. GLOBAL SYSTEM STATE ---
# ==============================================================================
class MunhEngineState:
    def __init__(self):
        self.smart_api = None
        self.sws = None
        self.is_ws_ready = False
        self.reconnect_count = 0
        self.subscribed_tokens_set = set()      
        self.token_metadata = {}                
        self.global_market_cache = {}           
        self.previous_price = {}                
        self.live_ohlc = {}                     
        self.user_p2p_scores = {}               
        self.active_users_pool = {}             
        self.dns_status = False                 
        self.last_master_update = None
        self.total_packets = 0
        self.start_time = datetime.datetime.now(IST)
        self.db_path = None
        self.heartbeat_gap = 0.5 

state = MunhEngineState()

# ==============================================================================
# --- 3. SYSTEM RESILIENCE & MASTER DATA ---
# ==============================================================================
def verify_dns_resilience():
    try:
        host = "apiconnect.angelone.in"
        socket.gethostbyname(host)
        state.dns_status = True
        return True
    except:
        state.dns_status = False
        return False

def system_memory_protector():
    while True:
        eventlet.sleep(600)
        try:
            gc.collect()
            if len(state.previous_price) > 10000:
                state.previous_price.clear()
                state.live_ohlc.clear()
                state.global_market_cache.clear()
            logger.info(f"🧹 [System] RAM Optimized. Packets: {state.total_packets}")
        except: pass

def sync_master_data_v2():
    logger.info("🔄 [Master] Syncing Scrip Master...")
    try:
        if not verify_dns_resilience(): return False
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        master_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = requests.get(master_url, timeout=45)
        if response.status_code == 200:
            json_payload = response.json()
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
                state.db_path = tmp.name
            conn = sqlite3.connect(state.db_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("DROP TABLE IF EXISTS symbols")
            cursor.execute('''CREATE TABLE symbols (token TEXT, symbol TEXT, name TEXT, expiry TEXT, strike TEXT, lotsize TEXT, instrumenttype TEXT, exch_seg TEXT, tick_size TEXT)''')
            records = [(str(i.get('token')), i.get('symbol'), i.get('name'), i.get('expiry'), i.get('strike'), i.get('lotsize'), i.get('instrumenttype'), i.get('exch_seg'), i.get('tick_size')) for i in json_payload if i.get('token')]
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", records)
            conn.commit()
            conn.close()
            with open(state.db_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(path="angel_master.db", file=f.read(), file_options={"x-upsert": "true", "content-type": "application/octet-stream"})
            state.last_master_update = datetime.datetime.now(IST)
            logger.info("✅ [Master] Sync Complete.")
            return True
    except Exception as e:
        logger.error(f"❌ [Master Error]: {e}")
        return False

# ==============================================================================
# --- 4. API ROUTES (SEARCH, OPTION CHAIN, OHLC) ---
# ==============================================================================
@app.route('/')
def health_check():
    return jsonify({"status": "Titan V3 Live", "ws": state.is_ws_ready, "packets": state.total_packets}), 200

@app.route('/api/search', methods=['POST'])
def handle_search():
    try:
        query = request.json.get('query', '').upper()
        conn = sqlite3.connect(state.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT name FROM symbols WHERE name LIKE ? LIMIT 25", (f'%{query}%',))
        results = [row[0] for row in cursor.fetchall()]
        conn.close()
        return jsonify({"symbols": results})
    except: return jsonify([])

@app.route('/api/option_chain', methods=['POST'])
def handle_option_chain():
    try:
        d = request.json
        conn = sqlite3.connect(state.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT strike, token, symbol, lotsize, exch_seg FROM symbols WHERE name = ? AND expiry = ? AND instrumenttype LIKE 'OPT%' ORDER BY CAST(strike AS FLOAT)", (d.get('name'), d.get('expiry')))
        rows = cursor.fetchall()
        conn.close()
        return jsonify({"chain": rows})
    except: return jsonify({"status": False})

@app.route('/get_90day_chart', methods=['POST'])
def chart_logic():
    try:
        d = request.json
        if state.smart_api:
            payload = {"exchange": d.get('exch', 'NSE'), "symboltoken": d.get('token'), "interval": d.get('interval', "ONE_MINUTE"), "fromdate": (datetime.datetime.now(IST) - datetime.timedelta(days=90)).strftime('%Y-%m-%d %H:%M'), "todate": datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')}
            return jsonify(state.smart_api.getCandleData(payload))
    except: pass
    return jsonify({"status": False})

# ==============================================================================
# --- 5. TICK ENGINE (ON_DATA & PULSE) ---
# ==============================================================================
def on_data_callback(wsapp, msg):
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            ltp = float(msg.get('last_traded_price', 0)) / 100
            if ltp <= 0: return
            state.total_packets += 1
            state.user_p2p_scores[token] = state.user_p2p_scores.get(token, 0) + 1
            if token not in state.live_ohlc: state.live_ohlc[token] = {"o": ltp, "h": ltp, "l": ltp, "c": ltp}
            else:
                state.live_ohlc[token]["h"] = max(state.live_ohlc[token]["h"], ltp)
                state.live_ohlc[token]["l"] = min(state.live_ohlc[token]["l"], ltp)
                state.live_ohlc[token]["c"] = ltp
            data_packet = {"t": token, "p": "{:.2f}".format(ltp), "lp": state.previous_price.get(token, "{:.2f}".format(ltp)), "h": "{:.2f}".format(state.live_ohlc[token]["h"]), "l": "{:.2f}".format(state.live_ohlc[token]["l"]), "o": "{:.2f}".format(state.live_ohlc[token]["o"]), "score": state.user_p2p_scores[token]}
            if 'close' in msg and float(msg['close']) > 0:
                cp = float(msg['close']) / 100
                data_packet["pc"] = "{:.2f}".format(((ltp - cp) / cp) * 100)
            state.global_market_cache[token] = data_packet
            state.previous_price[token] = "{:.2f}".format(ltp)
    except: pass

def pulse_broadcaster():
    while True:
        try:
            if state.global_market_cache:
                snap = dict(state.global_market_cache)
                state.global_market_cache.clear()
                socketio.emit('live_update_batch', snap)
            eventlet.sleep(state.heartbeat_gap)
        except: eventlet.sleep(1)

# ==============================================================================
# --- 6. SMART SUBSCRIBE LOGIC (THE HEART) ---
# ==============================================================================
@socketio.on('subscribe')
def handle_incoming_subscription(data):
    watchlist = data.get('watchlist', [])
    if not watchlist: return
    batch_registry = {1: [], 2: [], 3: [], 4: [], 5: []}
    for inst in watchlist:
        token = str(inst.get('token'))
        symbol = str(inst.get('symbol', '')).upper()
        exch = str(inst.get('exch', inst.get('exch_seg', 'NSE'))).upper()
        if not token or token == "None": continue
        join_room(token)
        
        # --- FULL LOGIC: AUTO-DETECTION ENGINE ---
        etype = 1 # NSE Cash
        if "MCX" in exch: etype = 5
        elif "BFO" in exch or ("BSE" in exch and any(x in symbol for x in ["CE", "PE", "FUT"])): etype = 4
        elif "NFO" in exch or any(x in symbol for x in ["CE", "PE", "FUT"]): etype = 2
        elif "BSE" in exch: etype = 3
        
        state.token_metadata[token] = etype
        if token not in batch_registry[etype]:
            batch_registry[etype].append(token)
            state.subscribed_tokens_set.add(token)

    if state.is_ws_ready and state.sws:
        for etype, tokens in batch_registry.items():
            if not tokens: continue
            for i in range(0, len(tokens), 500):
                b = tokens[i:i+500]
                state.sws.subscribe(f"sub_{etype}_{i}", 1, [{"exchangeType": etype, "tokens": b}])
                logger.info(f"📡 Subscribed: {len(b)} tokens in Segment {etype}")
                eventlet.sleep(0.05)

# ==============================================================================
# --- 7. LIFECYCLE & AUTO-HEALING ---
# ==============================================================================
def engine_lifecycle_manager():
    sync_master_data_v2()
    while True:
        try:
            if not state.is_ws_ready:
                if not verify_dns_resilience(): eventlet.sleep(10); continue
                state.smart_api = SmartConnect(api_key=API_KEY)
                session = state.smart_api.generateSession(CLIENT_CODE, MPIN, pyotp.TOTP(TOTP_STR).now())
                if session and session.get('status'):
                    state.sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                    state.sws.on_data = on_data_callback
                    state.sws.on_open = lambda ws: [setattr(state, 'is_ws_ready', True), re_subscribe_all_tokens()]
                    state.sws.on_close = lambda ws, c, r: setattr(state, 'is_ws_ready', False)
                    state.sws.connect()
                    eventlet.sleep(5)
        except Exception as e: logger.error(f"⚠️ Lifecycle Error: {e}"); state.is_ws_ready = False
        eventlet.sleep(20)

def re_subscribe_all_tokens():
    if not state.sws or not state.is_ws_ready: return
    seg_map = {1: [], 2: [], 3: [], 4: [], 5: []}
    for t in state.subscribed_tokens_set: seg_map[state.token_metadata.get(t, 1)].append(t)
    for s, tokens in seg_map.items():
        if not tokens: continue
        for i in range(0, len(tokens), 500):
            state.sws.subscribe(f"recon_{s}_{i}", 1, [{"exchangeType": s, "tokens": tokens[i:i+500]}])
            eventlet.sleep(0.1)

if __name__ == '__main__':
    socketio.start_background_task(pulse_broadcaster)
    socketio.start_background_task(engine_lifecycle_manager)
    socketio.start_background_task(system_memory_protector)
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
