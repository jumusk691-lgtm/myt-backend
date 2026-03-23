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

# Credentials
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# Supabase Infrastructure
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2V_cm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
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
# --- 2. GLOBAL SYSTEM STATE (ULTRA LOGIC) ---
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
# --- 3. SYSTEM RESILIENCE & MEMORY MANAGEMENT ---
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
            logger.info(f"🧹 [System] RAM Optimized. Total Packets: {state.total_packets}")
        except: pass

# ==============================================================================
# --- 4. MASTER DATA SYNCHRONIZATION ---
# ==============================================================================
def sync_master_data_v2():
    logger.info("🔄 [Master] Initializing Scrip Master Sync...")
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
            cursor.execute('''CREATE TABLE symbols (
                token TEXT, symbol TEXT, name TEXT, expiry TEXT, 
                strike TEXT, lotsize TEXT, instrumenttype TEXT, 
                exch_seg TEXT, tick_size TEXT)''')
            
            records = [(str(i.get('token')), i.get('symbol'), i.get('name'), i.get('expiry'),
                        i.get('strike'), i.get('lotsize'), i.get('instrumenttype'),
                        i.get('exch_seg'), i.get('tick_size'))
                       for i in json_payload if i.get('token')]
            
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", records)
            cursor.execute("CREATE INDEX idx_token_fast ON symbols(token)")
            conn.commit()
            conn.close()
            
            with open(state.db_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", 
                    file=f.read(),
                    file_options={"x-upsert": "true", "content-type": "application/octet-stream"}
                )
            state.last_master_update = datetime.datetime.now(IST)
            logger.info("✅ [Master] Cloud DB Sync Complete.")
            return True
    except Exception as e:
        logger.error(f"❌ [Master] Fatal Error: {e}")
        return False

# ==============================================================================
# --- 5. SEARCH & OPTION CHAIN API ROUTES ---
# ==============================================================================
@app.route('/')
def health_check():
    return jsonify({
        "status": "Titan V3 Backend Live",
        "uptime": str(datetime.datetime.now(IST) - state.start_time),
        "ws_active": state.is_ws_ready
    }), 200

@app.route('/api/search', methods=['POST'])
def handle_search():
    try:
        query = request.json.get('query', '').upper()
        conn = sqlite3.connect(state.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT name FROM symbols WHERE name LIKE ? AND instrumenttype = '' LIMIT 25", (f'%{query}%',))
        results = [row[0] for row in cursor.fetchall()]
        conn.close()
        return jsonify({"symbols": results})
    except: return jsonify([])

# ==============================================================================
# --- 6. TICK ENGINE (LTP, OHLC & BROADCASTING) ---
# ==============================================================================
def on_data_callback(wsapp, msg):
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            ltp_raw = msg.get('last_traded_price', 0)
            if ltp_raw <= 0: return
            
            ltp = float(ltp_raw) / 100
            state.total_packets += 1
            
            if token not in state.live_ohlc:
                state.live_ohlc[token] = {"o": ltp, "h": ltp, "l": ltp, "c": ltp}
            else:
                state.live_ohlc[token]["h"] = max(state.live_ohlc[token]["h"], ltp)
                state.live_ohlc[token]["l"] = min(state.live_ohlc[token]["l"], ltp)
                state.live_ohlc[token]["c"] = ltp

            data_packet = {
                "t": token,
                "p": "{:.2f}".format(ltp),
                "h": "{:.2f}".format(state.live_ohlc[token]["h"]),
                "l": "{:.2f}".format(state.live_ohlc[token]["l"]),
                "o": "{:.2f}".format(state.live_ohlc[token]["o"])
            }
            
            if 'close' in msg and float(msg['close']) > 0:
                cp = float(msg['close']) / 100
                data_packet["pc"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

            state.global_market_cache[token] = data_packet
    except: pass

def pulse_broadcaster():
    while True:
        try:
            if state.global_market_cache:
                snap = dict(state.global_market_cache)
                state.global_market_cache.clear()
                socketio.emit('live_update_batch', snap)
                for token, data in snap.items():
                    socketio.emit('live_update', data, to=token)
            eventlet.sleep(state.heartbeat_gap)
        except: eventlet.sleep(1)

# ==============================================================================
# --- 8. SMART FULL-SEGMENT SUBSCRIPTION (NSE/BSE/NFO/MCX - FUT & OPT) ---
# ==============================================================================
@socketio.on('subscribe')
def handle_incoming_subscription(data):
    """
    Logic 13: UPDATED Full Multi-Segment Sync
    Handles NSE/BSE Cash, Futures, Options and MCX Futures/Options.
    """
    watchlist = data.get('watchlist', [])
    if not watchlist: return

    # Segment Map: 1=NSE, 2=NFO, 3=BSE, 4=BFO, 5=MCX, 7=MCX_OPT
    batch_registry = {1: [], 2: [], 3: [], 4: [], 5: [], 7: []}

    for instrument in watchlist:
        token = str(instrument.get('token'))
        exch = str(instrument.get('exch', 'NSE')).upper()
        
        if not token or token == "None" or token == "": continue

        join_room(token)
        
        # --- PRECISE SEGMENT DETECTION ENGINE ---
        etype = 1 # NSE Cash Default
        
        if "MCX" in exch:
            # MCX Options vs Futures
            etype = 7 if any(x in exch for x in ["OPT", "CE", "PE"]) else 5
        elif "BSE" in exch:
            # BSE Options vs Cash
            etype = 4 if any(x in exch for x in ["OPT", "FUT", "NFO"]) else 3
        elif any(x in exch for x in ["NFO", "FUT", "OPT", "NI"]):
            # NSE Derivatives
            etype = 2
        else:
            # NSE Cash
            etype = 1
        
        state.token_metadata[token] = etype
        
        if token not in batch_registry[etype]:
            batch_registry[etype].append(token)
            state.subscribed_tokens_set.add(token)

    # 500-Batch Execution
    if state.is_ws_ready and state.sws:
        for seg, tokens in batch_registry.items():
            if not tokens: continue
            for i in range(0, len(tokens), 500):
                batch = tokens[i:i+500]
                try:
                    state.sws.subscribe(f"sync_{seg}_{i}", 1, [{"exchangeType": seg, "tokens": batch}])
                    logger.info(f"📈 [Sync] Segment {seg} | Tokens: {len(batch)}")
                    eventlet.sleep(0.05) 
                except Exception as e:
                    logger.error(f"❌ [Sub Error]: {e}")
    else:
        logger.warning("🔌 [Sub Pending] Socket not ready. Tokens queued.")

# ==============================================================================
# --- 9. LIFECYCLE & AUTO-RECOVERY ---
# ==============================================================================
def engine_lifecycle_manager():
    sync_master_data_v2()
    while True:
        try:
            if not state.is_ws_ready:
                if not verify_dns_resilience():
                    eventlet.sleep(15); continue
                
                state.smart_api = SmartConnect(api_key=API_KEY)
                totp = pyotp.TOTP(TOTP_STR).now()
                session = state.smart_api.generateSession(CLIENT_CODE, MPIN, totp)
                
                if session and session.get('status'):
                    state.sws = SmartWebSocketV2(
                        session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken']
                    )
                    
                    def handle_open(ws):
                        state.is_ws_ready = True
                        logger.info("💎 [WS] Connected!")
                        re_subscribe_all_tokens()

                    state.sws.on_data = on_data_callback
                    state.sws.on_open = handle_open
                    state.sws.on_close = lambda ws,c,r: setattr(state, 'is_ws_ready', False)
                    state.sws.connect()
                    eventlet.sleep(5)
        except: state.is_ws_ready = False
        eventlet.sleep(25)

def re_subscribe_all_tokens():
    if not state.sws or not state.is_ws_ready: return
    
    seg_map = {1: [], 2: [], 3: [], 4: [], 5: [], 7: []}
    for t in state.subscribed_tokens_set:
        s = state.token_metadata.get(t, 1)
        seg_map[s].append(t)
        
    for seg, tokens in seg_map.items():
        for i in range(0, len(tokens), 500):
            batch = tokens[i:i+500]
            if batch:
                state.sws.subscribe(f"recon_{seg}", 1, [{"exchangeType": seg, "tokens": batch}])
                eventlet.sleep(0.1)

# ==============================================================================
# --- 10. BOOTSTRAP ---
# ==============================================================================
if __name__ == '__main__':
    socketio.start_background_task(pulse_broadcaster)
    socketio.start_background_task(engine_lifecycle_manager)
    socketio.start_background_task(system_memory_protector)
    
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
