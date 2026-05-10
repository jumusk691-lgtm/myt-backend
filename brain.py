import os, pyotp, datetime, pytz, sqlite3, tempfile, json, gc, socket, sys, logging, asyncio
from fastapi import FastAPI, Request, jsonify
from fastapi_socketio import SocketManager
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

# ==============================================================================
# --- FASTAPI & SOCKET ENGINE (CLOUDFLARE BYPASS) ---
# ==============================================================================
app = FastAPI()
# SocketManager Cloudflare ke environment mein optimized chalta hai
sm = SocketManager(app=app, cors_allowed_origins="*")

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
        
        # --- BATCHING BUFFER (The Magic Pipe) ---
        # Data yahan jama hoga aur pipeline.py ise broadcast karega
        self.batch_buffer = {} 
        
        # User & Level Management
        self.user_levels = {}                   
        self.active_users_pool = {}             
        
        # Health & Persistence
        self.dns_status = False                 
        self.last_master_update = None
        self.start_time = datetime.datetime.now(IST)
        self.db_path = None
        
        logger.info("🧠 [Brain] State Initialized for Cloudflare (Python Worker).")

# Shared Global State Instance
state = MunhEngineState()

# ==============================================================================
# --- MEMORY GUARDIAN ---
# ==============================================================================
def cleanup_memory():
    """Manual trigger to stay under 12MB"""
    gc.collect()

# ==============================================================================
# --- FASTAPI ADAPTERS (For Search & Option Chain) ---
# ==============================================================================
@app.post("/api/search")
async def api_search(request: Request):
    from master_db import handle_search
    return await handle_search()

@app.post("/api/option_chain")
async def api_option_chain(request: Request):
    from master_db import get_chain
    return await get_chain()
