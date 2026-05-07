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

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==============================================================================
# --- FLASK & SOCKETIO (FIXED PACKET ERROR) ---
# ==============================================================================
app = Flask(__name__)

# max_decode_packets=5000: Render/SocketIO ka 'Too many packets' error fix karne ke liye
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='eventlet', 
    ping_timeout=120, 
    ping_interval=25,
    manage_session=False, 
    max_decode_packets=5000, 
    max_http_buffer_size=5242880  # 5MB Buffer for stability
)

# ==============================================================================
# --- GLOBAL SYSTEM STATE (ZERO-RAM ENGINE) ---
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
        
        # User & Score Management
        self.score = 0                          
        self.user_p2p_scores = {}               
        
        self.start_time = datetime.datetime.now(IST)
        self.db_path = None

state = MunhEngineState()

# ==============================================================================
# --- SMART API LOGIN LOGIC ---
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
        else:
            logger.error(f"❌ Login Failed: {data['message']}")
            return False
    except Exception as e:
        logger.error(f"💥 Login Exception: {str(e)}")
        return False

# ==============================================================================
# --- WEBSOCKET ENGINE (TICK BYPASS) ---
# ==============================================================================
def on_data(wsapp, msg):
    """
    Directly emits tick data to the room associated with the token.
    Zero-RAM: Data is not stored in any list or global variable.
    """
    try:
        if 'token' in msg and 'last_traded_price' in msg:
            token = msg['token']
            # Room based broadcasting: Only users subscribed to this token get it
            socketio.emit(f'tick_{token}', msg, room=f'token_{token}')
    except Exception as e:
        pass # Low latency silence

def on_open(wsapp):
    state.is_ws_ready = True
    logger.info("🌐 WebSocket Connected & Ready")

def on_error(wsapp, error):
    logger.error(f"🌐 WebSocket Error: {error}")

def init_websocket(jwt_token):
    state.sws = SmartWebSocketV2(jwt_token, API_KEY, CLIENT_CODE, "FEED_TOKEN_NOT_NEEDED")
    state.sws.on_data = on_data
    state.sws.on_open = on_open
    state.sws.on_error = on_error
    
    # Run in background thread
    threading.Thread(target=state.sws.connect, daemon=True).start()

# ==============================================================================
# --- SOCKETIO EVENTS (SIGN-UP / LOGIC FLOW) ---
# ==============================================================================
@socketio.on('connect')
def handle_connect():
    sid = request.sid
    logger.info(f"Client Connected: {sid}")
    state.user_subscriptions[sid] = set()

@socketio.on('subscribe_token')
def handle_subscription(data):
    """
    Logic: If user registers/subscribes, they join a specific room for that token.
    This ensures the next 'page' or data flow opens seamlessly.
    """
    sid = request.sid
    token = data.get('token')
    exchange = data.get('exchange', 1) # Default NSE

    if token:
        room_name = f'token_{token}'
        join_room(room_name)
        state.user_subscriptions[sid].add(token)
        
        # Add to global subscription set for Angel WebSocket
        if token not in state.subscribed_tokens_set:
            state.subscribed_tokens_set.add(token)
            if state.sws and state.is_ws_ready:
                correlation_id = f"sub_{token}"
                action = 1 # Subscribe
                params = {
                    "correlationId": correlation_id,
                    "action": action,
                    "mode": 3, # Full Mode
                    "tokenList": [{"exchangeType": exchange, "tokens": [token]}]
                }
                state.sws.subscribe(params)
        
        # Logic for 'Next Page' transition
        emit('subscription_success', {'status': 'success', 'token': token})

@socketio.on('update_score')
def handle_score(data):
    """Logic to track and add to the global score."""
    points = data.get('points', 0)
    state.score += points
    emit('score_updated', {'total_score': state.score}, broadcast=True)

@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    if sid in state.user_subscriptions:
        for token in state.user_subscriptions[sid]:
            leave_room(f'token_{token}')
        del state.user_subscriptions[sid]
    logger.info(f"Client Disconnected: {sid}")

# ==============================================================================
# --- ROUTES ---
# ==============================================================================
@app.route('/')
def health_check():
    return jsonify({
        "status": "running",
        "engine": "MunhTitan_V3",
        "score": state.score,
        "uptime": str(datetime.datetime.now(IST) - state.start_time)
    })

# ==============================================================================
# --- EXECUTION ---
# ==============================================================================
if __name__ == '__main__':
    # Initial Login
    login_to_angel()
    
    # Port configuration for Render
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"🚀 Launching Server on port {port}...")
    socketio.run(app, host='0.0.0.0', port=port, debug=False)
