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

# Flask & SocketIO Instance Creation
app = Flask(__name__)
# Bypass mode ke liye buffer size ko chota rakha hai taaki RAM reserve na ho
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='eventlet', 
    ping_timeout=60, 
    ping_interval=25,
    manage_session=False, 
    max_http_buffer_size=1000000 # Reduced to 1MB for Zero-RAM
)

# ==============================================================================
# --- GLOBAL SYSTEM STATE (THE ZERO-RAM BYPASS ENGINE) ---
# ==============================================================================
class MunhEngineState:
    def __init__(self):
        # API & WebSocket
        self.smart_api = None
        self.sws = None
        self.is_ws_ready = False
        self.reconnect_count = 0
        
        # --- BYPASS MODE: NO CACHE, NO OHLC ---
        # Sirf wahi cheezein rakhi hain jo connections manage karne ke liye zaruri hain
        self.subscribed_tokens_set = set()      # AngelOne subscription manage karne ke liye
        self.token_metadata = {}                # Token to Etype mapping (Zaruri hai pipe ke liye)
        
        # Tracker for Room Management (Direct Pipe Logic)
        self.token_ref_count = {}               # Kitne log ek token room mein hain
        
        # User & Score Management
        self.user_levels = {}                   
        self.user_p2p_scores = {}               
        self.active_users_pool = {}             
        self.score = 0                          # Global score tracker
        
        # System Monitoring
        self.dns_status = False                 
        self.last_master_update = None
        self.start_time = datetime.datetime.now(IST)
        self.db_path = None
        
        # Removed: global_market_cache, previous_price, live_ohlc (RAM Bachers!)

# Shared Global State Instance
state = MunhEngineState()

logger.info("🧠 [Brain] File 1 Updated for ZERO-RAM BYPASS Mode.")
