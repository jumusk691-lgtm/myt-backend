import eventlet
eventlet.monkey_patch(all=True) 

import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json, gc, socket, sys, logging, threading, traceback
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from flask import Flask, send_file, request, after_this_request, jsonify
from flask_socketio import SocketIO, join_room, emit, leave_room

# ==============================================================================
# --- 1. PRO-LEVEL LOGGING & SECURITY CONFIG ---
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

# Supabase Infrastructure
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

app = Flask(__name__)
# Engine Optimization for High-Volume Data
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='eventlet', 
    ping_timeout=120, 
    ping_interval=40, 
    max_http_buffer_size=100000000,
    manage_session=False
)

# ==============================================================================
# --- 2. GLOBAL SYSTEM STATE (ULTRA LOGIC) ---
# ==============================================================================
class MunhEngineState:
    def __init__(self):
        self.sws = None
        self.is_ws_ready = False
        self.subscribed_tokens_set = set()      
        self.token_metadata = {}           
        self.global_market_cache = {}      
        self.previous_price = {}           
        self.last_tick_time = {}           
        self.last_master_update = None
        self.user_scores = {} # Score tracking
        self.reconnect_count = 0
        self.dns_status = False
        self.start_time = datetime.datetime.now(IST)
        self.total_packets_received = 0

state = MunhEngineState()

# ==============================================================================
# --- 3. NETWORK RESILIENCE & DNS FIX (SCREENSHOT FIX) ---
# ==============================================================================
def check_network_stability():
    """Fixes 'Lookup Timed Out' by verifying DNS before login"""
    test_hosts = ["apiconnect.angelone.in", "google.com", "8.8.8.8"]
    for host in test_hosts:
        try:
            socket.gethostbyname(host)
            state.dns_status = True
            return True
        except (socket.gaierror, socket.timeout):
            continue
    state.dns_status = False
    return False

def memory_protector_logic():
    """Prevents RAM crashes on Render.com"""
    while True:
        eventlet.sleep(300) 
        try:
            curr_size = len(state.previous_price)
            if curr_size > 5000:
                state.previous_price.clear()
                state.global_market_cache.clear()
            gc.collect()
            logger.info(f"🧹 [System] RAM Cleared. Active Tokens: {len(state.subscribed_tokens_set)}")
        except: pass

# ==============================================================================
# --- 4. MASTER DATA SYNCHRONIZATION (SUPABASE) ---
# ==============================================================================
def sync_master_data_v2():
    """Pure Python Logic for Master Data Sync"""
    logger.info("🔄 [Master] Fetching Global Scrip Master...")
    try:
        if not check_network_stability(): return False
        
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        master_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        
        response = requests.get(master_url, timeout=45)
        if response.status_code == 200:
            json_payload = response.json()
            
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
                db_path = tmp.name
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL") # High-speed DB logic
            cursor.execute("DROP TABLE IF EXISTS symbols")
            cursor.execute('''CREATE TABLE symbols (
                token TEXT, symbol TEXT, name TEXT, expiry TEXT,
                strike TEXT, lotsize TEXT, instrumenttype TEXT, 
                exch_seg TEXT, tick_size TEXT)''')
            
            # Efficient batch insertion
            records = [(str(i.get('token')), i.get('symbol'), i.get('name'), i.get('expiry'),
                        i.get('strike'), i.get('lotsize'), i.get('instrumenttype'),
                        i.get('exch_seg'), i.get('tick_size'))
                       for i in json_payload if i.get('token')]
            
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", records)
            cursor.execute("CREATE INDEX idx_token_fast ON symbols(token)")
            conn.commit()
            conn.close()
            
            # Cloud Upload
            with open(db_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", 
                    file=f.read(),
                    file_options={"x-upsert": "true", "content-type": "application/octet-stream"}
                )
            os.remove(db_path)
            state.last_master_update = datetime.datetime.now(IST)
            logger.info("✅ [Master] Cloud DB Updated Successfully.")
            return True
    except Exception:
        logger.error(f"❌ [Master] Fatal Error: {traceback.format_exc()}")
        return False

# ==============================================================================
# --- 5. ULTRA-FAST TICK ENGINE (THE CORE) ---
# ==============================================================================
def on_data_callback(wsapp, msg):
    """Handles incoming live market packets"""
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            ltp_raw = msg.get('last_traded_price', 0)
            if ltp_raw <= 0: return
            
            state.total_packets_received += 1
            ltp = float(ltp_raw) / 100
            
            # Price Movement Logic
            old_val = state.previous_price.get(token, "{:.2f}".format(ltp))
            
            data_packet = {
                "t": token,
                "p": "{:.2f}".format(ltp),
                "lp": old_val,
                "h": "{:.2f}".format(float(msg.get('high', 0)) / 100),
                "l": "{:.2f}".format(float(msg.get('low', 0)) / 100),
                "v": msg.get('volume', 0),
                "o": "{:.2f}".format(float(msg.get('open', 0)) / 100)
            }
            
            # Percent Change Calculation
            if 'close' in msg and float(msg['close']) > 0:
                cp = float(msg['close']) / 100
                data_packet["pc"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

            state.global_market_cache[token] = data_packet
            state.previous_price[token] = "{:.2f}".format(ltp)
    except: pass

def pulse_broadcaster():
    """Broadcasts updates every 0.5s to reduce network congestion"""
    while True:
        try:
            if state.global_market_cache:
                snap = dict(state.global_market_cache)
                state.global_market_cache.clear()
                
                for token, data in snap.items():
                    socketio.emit('live_update', data, to=token)
            
            eventlet.sleep(0.5) 
        except Exception:
            eventlet.sleep(1)

# ==============================================================================
# --- 6. AUTO-HEALING RECOVERY ENGINE ---
# ==============================================================================
def engine_lifecycle_manager():
    """Main loop to keep the connection alive forever"""
    sync_master_data_v2()
    
    while True:
        try:
            if not state.is_ws_ready:
                if not check_network_stability():
                    logger.warning("📡 [Engine] Internet Offline. Waiting...")
                    eventlet.sleep(15)
                    continue
                
                logger.info("🔐 [Engine] Establishing Secure Session...")
                smart_api = SmartConnect(api_key=API_KEY)
                token_otp = pyotp.TOTP(TOTP_STR).now()
                session = smart_api.generateSession(CLIENT_CODE, MPIN, token_otp)
                
                if session and session.get('status'):
                    state.sws = SmartWebSocketV2(
                        session['data']['jwtToken'], 
                        API_KEY, 
                        CLIENT_CODE, 
                        session['data']['feedToken']
                    )
                    
                    def handle_open(ws):
                        state.is_ws_ready = True
                        state.reconnect_count += 1
                        logger.info(f"💎 [WS] Connected! Session: {state.reconnect_count}")
                        # Auto-Recovery Logic for Subscriptions
                        re_subscribe_all_tokens()

                    def handle_close(ws, code, reason):
                        state.is_ws_ready = False
                        logger.error(f"🔌 [WS] Disconnected: {reason}")

                    state.sws.on_data = on_data_callback
                    state.sws.on_open = handle_open
                    state.sws.on_close = handle_close
                    state.sws.connect()
                else:
                    logger.error(f"❌ [Auth] Error: {session.get('message')}")
            
        except Exception as e:
            logger.error(f"⚠️ [Critical Engine Failure]: {e}")
            state.is_ws_ready = False
            
        eventlet.sleep(25)

def re_subscribe_all_tokens():
    """Resubscribes all tokens in 500 batches after reconnection"""
    if not state.sws or not state.is_ws_ready: return
    
    seg_map = {1: [], 2: [], 3: [], 5: []}
    for t in state.subscribed_tokens_set:
        s = state.token_metadata.get(t, 1)
        seg_map[s].append(t)
        
    for seg, tokens in seg_map.items():
        for i in range(0, len(tokens), 500):
            batch = tokens[i:i+500]
            if batch:
                state.sws.subscribe(f"recon_{seg}_{i}", 1, [{"exchangeType": seg, "tokens": batch}])
                eventlet.sleep(0.2)

# ==============================================================================
# --- 7. SMART 500 BATCHING & SOCKET LOGIC ---
# ==============================================================================
@socketio.on('subscribe')
def handle_incoming_subscription(data):
    """Logic to open next sign page and handle watchlist"""
    watchlist = data.get('watchlist', [])
    if not watchlist: return

    batch_registry = {1: [], 2: [], 3: [], 5: []}

    for instrument in watchlist:
        token = str(instrument.get('token'))
        exch = str(instrument.get('exch', 'NSE')).upper()
        if not token or token == "None": continue

        join_room(token)
        
        # Segment Identification Logic
        etype = 1
        if "MCX" in exch: etype = 5
        elif "NFO" in exch: etype = 2
        elif "BSE" in exch: etype = 3
        
        state.token_metadata[token] = etype
        
        if token not in state.subscribed_tokens_set:
            batch_registry[etype].append(token)
            state.subscribed_tokens_set.add(token)

    # 500-Batch Execution
    if state.is_ws_ready and state.sws:
        for etype, tokens in batch_registry.items():
            for i in range(0, len(tokens), 500):
                final_batch = tokens[i:i+500]
                if final_batch:
                    try:
                        state.sws.subscribe(f"live_batch_{i}", 1, [{"exchangeType": etype, "tokens": final_batch}])
                        logger.info(f"📈 [Sub] Activated {len(final_batch)} tokens for Seg {etype}")
                        eventlet.sleep(0.1)
                    except Exception as e:
                        logger.error(f"❌ [Sub Error]: {e}")

# ==============================================================================
# --- 8. API ROUTES (BLACK/BLUE THEME COMPATIBLE) ---
# ==============================================================================
@app.route('/')
def system_status():
    """Returns engine health for the APK"""
    return jsonify({
        "engine": "Munh_V3_Titan",
        "status": "OPERATIONAL" if state.is_ws_ready else "RECOVERING",
        "ws_active": state.is_ws_ready,
        "dns_ok": state.dns_status,
        "uptime": str(datetime.datetime.now(IST) - state.start_time),
        "total_subscriptions": len(state.subscribed_tokens_set),
        "packets_processed": state.total_packets_received
    }), 200

# ==============================================================================
# --- 9. BOOTSTRAP ---
# ==============================================================================
if __name__ == '__main__':
    # Initializing Background Threads
    socketio.start_background_task(pulse_broadcaster)
    socketio.start_background_task(engine_lifecycle_manager)
    socketio.start_background_task(memory_protector_logic)
    
    # Server configuration for Render.com
    server_port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 [Munh Engine] Deploying on port {server_port}...")
    
    try:
        eventlet.wsgi.server(eventlet.listen(('0.0.0.0', server_port)), app)
    except Exception as fatal:
        logger.critical(f"💀 [Server Crash]: {fatal}")
