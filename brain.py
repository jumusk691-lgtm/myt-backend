import eventlet
eventlet.monkey_patch(all=True)

import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json, gc, socket, sys, logging, threading, traceback
from flask import Flask, send_file, request, after_this_request, jsonify
from flask_socketio import SocketIO, join_room, emit, leave_room
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client

# ==============================================================================
# --- PRO-LEVEL LOGGING & CONFIGURATION ---
# ==============================================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(message)s', 
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MunhTitan_V3")

# Credentials
API_KEY = "Z80WG5Sg"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# Supabase
SUPABASE_URL = "https://fnfynhgkdevxytxtfzrk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZuZnluaGdrZGV2eHl0eHRmenJrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc0OTAwMjgsImV4cCI6MjA5MzA2NjAyOH0.Tgr8kB6KGeAsAbXzH8a2wlLStqMFS3fnFPcowbL4Di8"
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==============================================================================
# --- FLASK & SOCKETIO (FIXED) ---
# ==============================================================================
app = Flask(__name__)
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='eventlet', 
    ping_timeout=120, 
    ping_interval=25,
    manage_session=False, 
    max_decode_packets=5000, 
    max_http_buffer_size=5242880 
)

# ==============================================================================
# --- GLOBAL SYSTEM STATE (FIXED MISSING ATTRIBUTES) ---
# ==============================================================================
class MunhEngineState:
    def __init__(self):
        self.smart_api = None
        self.sws = None
        self.is_ws_ready = False
        self.reconnect_count = 0
        
        # --- THE BYPASS CORE ---
        self.subscribed_tokens_set = set()      
        self.token_metadata = {}                
        self.token_ref_count = {}               
        self.user_subscriptions = {}            
        
        # --- FIXED: Added Missing Attributes causing the error ---
        self.active_users_pool = {}             # Iske bina error aa raha tha
        self.user_levels = {}                   # Safe side ke liye isse bhi add kiya
        
        # User & Score Management
        self.score = 0                          
        self.user_p2p_scores = {}               
        
        self.start_time = datetime.datetime.now(IST)
        self.db_path = None

state = MunhEngineState()

# ==============================================================================
# --- SMART API & WEBSOCKET ---
# ==============================================================================
def login_to_angel():
    try:
        totp = pyotp.TOTP(TOTP_STR).now()
        state.smart_api = SmartConnect(api_key=API_KEY)
        data = state.smart_api.generateSession(CLIENT_CODE, MPIN, totp)
        if data['status']:
            logger.info("✅ AngelOne Login Successful")
            init_websocket(data['data']['jwtToken'])
            return True
        return False
    except Exception as e:
        logger.error(f"💥 Login Exception: {str(e)}")
        return False

def on_data(wsapp, msg):
    try:
        if 'token' in msg and 'last_traded_price' in msg:
            token = msg['token']
            socketio.emit(f'tick_{token}', msg, room=f'token_{token}')
    except: pass

def init_websocket(jwt_token):
    state.sws = SmartWebSocketV2(jwt_token, API_KEY, CLIENT_CODE, "FEED_TOKEN_NOT_NEEDED")
    state.sws.on_data = on_data
    state.sws.on_open = lambda ws: setattr(state, 'is_ws_ready', True)
    threading.Thread(target=state.sws.connect, daemon=True).start()

# ==============================================================================
# --- SOCKETIO EVENTS ---
# ==============================================================================
@socketio.on('connect')
def handle_connect():
    sid = request.sid
    state.user_subscriptions[sid] = set()
    # Logic to register user in pool
    state.active_users_pool[sid] = {"connected_at": time.time()}
    logger.info(f"Client Connected: {sid}")

@socketio.on('subscribe_token')
def handle_subscription(data):
    sid = request.sid
    token = data.get('token')
    if token:
        join_room(f'token_{token}')
        state.user_subscriptions[sid].add(token)
        if token not in state.subscribed_tokens_set:
            state.subscribed_tokens_set.add(token)
            if state.sws and state.is_ws_ready:
                state.sws.subscribe({"correlationId": "sub", "action": 1, "mode": 3, "tokenList": [{"exchangeType": 1, "tokens": [token]}]})
        emit('subscription_success', {'status': 'success', 'token': token})

@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    if sid in state.active_users_pool: del state.active_users_pool[sid]
    if sid in state.user_subscriptions: del state.user_subscriptions[sid]

@app.route('/')
def health():
    return jsonify({"status": "running", "active_users": len(state.active_users_pool)})

if __name__ == '__main__':
    login_to_angel()
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host='0.0.0.0', port=port, debug=False)
