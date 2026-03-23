import eventlet
eventlet.monkey_patch(all=True)

import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json, gc, socket, sys, logging, threading, traceback
from flask import Flask, send_file, request, jsonify
from flask_socketio import SocketIO, join_room, emit
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client

# ==============================================================================
# --- CONFIGURATION & PRO LOGGING ---
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# Supabase
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', ping_timeout=120)

# ==============================================================================
# --- ENGINE STATE (THE BRAIN) ---
# ==============================================================================
class SmartTitanEngine:
    def __init__(self):
        self.smart_api = None
        self.sws = None
        self.is_ws_ready = False
        self.subscribed_tokens = set()      # Logic 20: Recovery Tracking
        self.token_meta = {}                # Logic 10: Segment Memory
        self.price_cache = {}               # Logic 17: Global Cache
        self.ohlc_data = {}                 # Logic 14: Real-time OHLC
        self.db_path = None
        self.total_packets = 0              # Logic 13: Health Score
        self.start_time = datetime.datetime.now(IST)

state = SmartTitanEngine()

# ==============================================================================
# --- SMART SEGMENT DETECTOR (LOGIC 8 & 13) ---
# ==============================================================================
def get_smart_segment(exch, symbol, token):
    """
    Logic 8: Intelligent Segment Identifier
    Detects if it's NSE, NFO, MCX, BSE or BSE_FO automatically.
    """
    exch = str(exch).upper()
    sym = str(symbol).upper()
    
    if "MCX" in exch: return 5
    if "BSE" in exch:
        if any(x in sym for x in ["CE", "PE", "FUT"]): return 4 # BSE F&O
        return 3 # BSE Cash
    if any(x in sym for x in ["CE", "PE", "FUT"]) or "NFO" in exch:
        return 2 # NSE F&O
    return 1 # NSE Cash

# ==============================================================================
# --- MASTER CORE (LOGIC 4, 5, 25) ---
# ==============================================================================
def sync_master_data():
    try:
        master_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        res = requests.get(master_url, timeout=30)
        if res.status_code == 200:
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
                state.db_path = tmp.name
            conn = sqlite3.connect(state.db_path)
            c = conn.cursor()
            c.execute("PRAGMA journal_mode=WAL") # Logic 5: Fast DB
            c.execute("CREATE TABLE symbols (token TEXT, symbol TEXT, name TEXT, exch_seg TEXT)")
            
            data = [(i['token'], i['symbol'], i['name'], i['exch_seg']) for i in res.json() if i.get('token')]
            c.executemany("INSERT INTO symbols VALUES (?,?,?,?)", data)
            conn.commit()
            conn.close()
            logger.info("✅ [Master] SQLite Ready & Indexed.")
    except Exception as e: logger.error(f"Master Sync Fail: {e}")

# ==============================================================================
# --- THE SMART SUBSCRIPTION ENGINE (LOGIC 9, 10, 20) ---
# ==============================================================================
def execute_smart_subscription(watchlist):
    """
    Core Logic: Batching 500 tokens and auto-detecting segments.
    """
    if not state.is_ws_ready or not state.sws:
        return False

    batches = {1:[], 2:[], 3:[], 4:[], 5:[]}
    
    for item in watchlist:
        token = str(item.get('token'))
        if not token or token == "None": continue
        
        # Smart Logic: Segment detect karo bina user se puche
        etype = get_smart_segment(item.get('exch', 'NSE'), item.get('symbol', ''), token)
        state.token_meta[token] = etype
        
        if token not in state.subscribed_tokens:
            batches[etype].append(token)
            state.subscribed_tokens.add(token)

    for etype, tokens in batches.items():
        if not tokens: continue
        # Logic 9: 500-Batch Slicing
        for i in range(0, len(tokens), 500):
            chunk = tokens[i:i+500]
            try:
                state.sws.subscribe(f"smart_{etype}_{i}", 1, [{"exchangeType": etype, "tokens": chunk}])
                eventlet.sleep(0.05)
            except: pass
    return True

# ==============================================================================
# --- SMART API ROUTES (URL HIT LOGIC) ---
# ==============================================================================
@app.route('/api/smart_subscribe', methods=['POST'])
def url_subscribe():
    """
    Logic: APK or URL Manager can hit this to force subscribe tokens.
    URL ke zariye bhi price activation hoga.
    """
    data = request.json
    watchlist = data.get('watchlist', [])
    success = execute_smart_subscription(watchlist)
    return jsonify({"status": success, "active_count": len(state.subscribed_tokens)})

@socketio.on('subscribe')
def handle_socket_sub(data):
    watchlist = data.get('watchlist', [])
    for item in watchlist: join_room(str(item.get('token')))
    execute_smart_subscription(watchlist)

# ==============================================================================
# --- TICK & BROADCAST ENGINE (LOGIC 7, 11, 14, 15, 16, 21) ---
# ==============================================================================
def on_data_callback(wsapp, msg):
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg['token'])
            ltp = float(msg.get('last_traded_price', 0)) / 100 # Logic 11
            if ltp <= 0: return

            state.total_packets += 1 # Logic 13: Score
            
            # Logic 14: Live OHLC Calculation
            if token not in state.ohlc_data:
                state.ohlc_data[token] = {"o": ltp, "h": ltp, "l": ltp, "c": ltp}
            else:
                state.ohlc_data[token]["h"] = max(state.ohlc_data[token]["h"], ltp)
                state.ohlc_data[token]["l"] = min(state.ohlc_data[token]["l"], ltp)

            packet = {
                "t": token, "p": "{:.2f}".format(ltp),
                "h": "{:.2f}".format(state.ohlc_data[token]["h"]),
                "l": "{:.2f}".format(state.ohlc_data[token]["l"]),
                "o": "{:.2f}".format(state.ohlc_data[token]["o"]),
                "s": state.total_packets
            }
            state.price_cache[token] = packet
    except: pass

def pulse_broadcaster():
    """Logic 16: 0.5s Pulse Distribution"""
    while True:
        if state.price_cache:
            snap = dict(state.price_cache)
            state.price_cache.clear()
            socketio.emit('live_update_batch', snap)
            for t, d in snap.items(): socketio.emit('live_update', d, to=t)
        eventlet.sleep(0.5)

# ==============================================================================
# --- LIFECYCLE & RECOVERY (LOGIC 2, 3, 6, 20) ---
# ==============================================================================
def engine_lifecycle():
    sync_master_data()
    while True:
        try:
            if not state.is_ws_ready:
                # Logic 2: DNS Resilience
                socket.gethostbyname("apiconnect.angelone.in")
                
                state.smart_api = SmartConnect(api_key=API_KEY)
                session = state.smart_api.generateSession(CLIENT_CODE, MPIN, pyotp.TOTP(TOTP_STR).now())
                
                if session.get('status'):
                    state.sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                    
                    def on_open(ws):
                        state.is_ws_ready = True
                        logger.info("💎 [WS] Connected & Smart Recovery Started")
                        # Logic 20: Auto Resubscribe
                        if state.subscribed_tokens:
                            execute_smart_subscription([{"token": t, "exch": "NSE"} for t in state.subscribed_tokens])

                    state.sws.on_data = on_data_callback
                    state.sws.on_open = on_open
                    state.sws.connect()
            eventlet.sleep(20)
        except: eventlet.sleep(10)

def ram_cleaner():
    """Logic 3: Memory Protector"""
    while True:
        eventlet.sleep(600)
        gc.collect()

# ==============================================================================
# --- BOOTSTRAP ---
# ==============================================================================
if __name__ == '__main__':
    socketio.start_background_task(pulse_broadcaster)
    socketio.start_background_task(engine_lifecycle)
    socketio.start_background_task(ram_cleaner)
    
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 [Munh Smart V3] Port: {port}")
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
