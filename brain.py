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
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# Supabase Infrastructure
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

# ==============================================================================
# --- FLASK & SOCKETIO (BYPASS OPTIMIZED) ---
# ==============================================================================
app = Flask(__name__)
# Max buffer ko 1MB rakha hai taaki RAM leak na ho
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='eventlet', 
    ping_timeout=60, 
    ping_interval=25,
    manage_session=False, 
    max_http_buffer_size=1048576 
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
        # Sirf metadata aur count rakh rahe hain, tick data NAHI
        self.subscribed_tokens_set = set()      
        self.token_metadata = {}                # mapping: {token: etype}
        self.token_ref_count = {}               # mapping: {token: user_count}
        self.user_subscriptions = {}            # mapping: {sid: set(tokens)}
        
        # User & Level Management
        self.user_levels = {}                   
        self.active_users_pool = {}             
        
        # Score System (Track & Add logic ready)
        self.score = 0                          
        self.user_p2p_scores = {}               
        
        # System Health
        self.dns_status = False                 
        self.last_master_update = None
        self.start_time = datetime.datetime.now(IST)
        self.db_path = None
        
        # REMOVED: global_market_cache, previous_price, live_ohlc
        # Inhe remove karne se Render ki RAM hamesha khali rahegi.

# Shared Global State Instance
state = MunhEngineState()

logger.info("🧠 [Brain] System State Initialized in Zero-RAM Bypass Mode.")
