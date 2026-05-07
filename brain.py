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

# Credentials & Timezone
API_KEY = "Z80WG5Sg"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# Supabase Infrastructure
SUPABASE_URL = "https://fnfynhgkdevxytxtfzrk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZuZnluaGdrZGV2eHl0eHRmenJrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc0OTAwMjgsImV4cCI6MjA5MzA2NjAyOH0.Tgr8kB6KGeAsAbXzH8a2wlLStqMFS3fnFPcowbL4Di8"
BUCKET_NAME = "myt"

# Fix for ImportError: Ensuring supabase is initialized here
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==============================================================================
# --- FLASK & SOCKETIO (FIXED FOR RENDER PACKET ERROR) ---
# ==============================================================================
app = Flask(__name__)

# UPDATED: Added max_decode_packets and increased buffer to stop Render crashes
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='eventlet', 
    ping_timeout=120, 
    ping_interval=25,
    manage_session=False, 
    max_decode_packets=5000,      # FIXED: 'Too many packets in payload' error
    max_http_buffer_size=5242880   # Increased to 5MB for stability
)

# ==============================================================================
# --- GLOBAL SYSTEM STATE (ZERO-RAM ENGINE) ---
# ==============================================================================
class MunhEngineState:
    def __init__(self):
        # API & WebSocket Management
        self.smart_api = None
        self.sws = None
        self.is_ws_ready = False
        self.reconnect_count = 0
        
        # --- THE BYPASS CORE ---
        self.subscribed_tokens_set = set()      
        self.token_metadata = {}                
        self.token_ref_count = {}               
        self.user_subscriptions = {}            
        
        # User & Level Management
        self.user_levels = {}                   
        self.active_users_pool = {}             
        
        # Score System
        self.score = 0                          
        self.user_p2p_scores = {}               
        
        # System Health
        self.dns_status = False                 
        self.last_master_update = None
        self.start_time = datetime.datetime.now(IST)
        self.db_path = None

# Shared Global State Instance
state = MunhEngineState()

# ==============================================================================
# --- ANGELONE CONNECTION LOGIC ---
# ==============================================================================
def login_to_angel():
    try:
        totp = pyotp.TOTP(TOTP_STR).now()
        state.smart_api = SmartConnect(api_key=API_KEY)
        data = state.smart_api.generateSession(CLIENT_CODE, MPIN, totp)
        if data['status']:
            logger.info("✅ AngelOne Session Created Successfully.")
            init_websocket(data['data']['jwtToken'])
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Login Error: {str(e)}")
        return False

def on_data(wsapp, msg):
    """Directly emits data to rooms without storing in RAM"""
    try:
        if 'token' in msg and 'last_traded_price' in msg:
            token = msg['token']
            # Emit tick only to users in that token's room
            socketio.emit(f'tick_{token}', msg, room=f'token_{token}')
    except:
        pass

def init_websocket(jwt_token):
    state.sws = SmartWebSocketV2(jwt_token, API_KEY, CLIENT_CODE, "FEED_TOKEN_NOT_NEEDED")
    state.sws.on_data = on_data
    state.sws.on_open = lambda ws: setattr(state, 'is_ws_ready', True)
    threading.Thread(target=state.sws.connect, daemon=True).start()

# ==============================================================================
# --- SOCKETIO EVENTS & LOGIC ---
# ==============================================================================
@socketio.on('connect')
def handle_connect():
    sid = request.sid
    state.active_users_pool[sid] = {"connected_at": time.time()}
    state.user_subscriptions[sid] = set()
    logger.info(f"🔌 Connected: {sid}")

@socketio.on('subscribe_token')
def handle_subscription(data):
    """Logic for registering and opening next page/data"""
    sid = request.sid
    token = data.get('token')
    exchange = data.get('exchange', 1)

    if token:
        join_room(f'token_{token}')
        state.user_subscriptions[sid].add(token)
        
        if token not in state.subscribed_tokens_set:
            state.subscribed_tokens_set.add(token)
            if state.sws and state.is_ws_ready:
                state.sws.subscribe({
                    "correlationId": "myt_v3",
                    "action": 1,
                    "mode": 3,
                    "tokenList": [{"exchangeType": exchange, "tokens": [token]}]
                })
        emit('subscription_success', {'token': token})

@socketio.on('update_score')
def handle_score(data):
    """Logic to track and add to the global score"""
    points = data.get('points', 0)
    state.score += points
    emit('score_updated', {'total_score': state.score}, broadcast=True)

@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    if sid in state.active_users_pool: del state.active_users_pool[sid]
    if sid in state.user_subscriptions: del state.user_subscriptions[sid]

# ==============================================================================
# --- STARTUP ---
# ==============================================================================
@app.route('/')
def health():
    return jsonify({"status": "live", "users": len(state.active_users_pool), "bucket": BUCKET_NAME})

if __name__ == '__main__':
    login_to_angel()
    # Port selection for Render
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🧠 [Brain] System Ready on Port {port}")
    socketio.run(app, host='0.0.0.0', port=port, debug=False)
