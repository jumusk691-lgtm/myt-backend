import eventlet
eventlet.monkey_patch(all=True)

import os
import pyotp
import socketio
import redis
import time
from datetime import datetime
from supabase import create_client
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- GLOBAL STATE ---
sws_instance = None
is_ws_ready = False 
r = None
supabase = None
active_subscriptions = set()

def initialize_clients():
    global r, supabase
    # Redis configuration
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        print("❌ ERROR: REDIS_URL not found in Render Environment")
    r = redis.from_url(redis_url, decode_responses=True)
    
    # Supabase configuration
    supabase_url = "https://rcosgmsyisybusmuxzei.supabase.co"
    # Service Role Key (Corrected for cleanup/delete access)
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDgzOTEzNCwiZXhwIjoyMDg2NDE1MTM0fQ.5BofQbMKiMLGFjqcIGaCwpoO9pLZnuLg7nojP0aGhJw"
    
    supabase = create_client(supabase_url, supabase_key)

initialize_clients()

sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
socketio_app = socketio.WSGIApp(sio)

# --- DATABASE CLEANUP ---
def cleanup_expired_contracts():
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        print(f"🧹 Expired contracts cleanup for: {today}")
        try:
            # Watchlist items delete logic
            supabase.table("watchlist_items").delete().lt("expiry", today).execute()
            print("✅ Supabase Cleanup Success")
        except Exception as table_e:
            print(f"⚠️ Cleanup failed: Table 'watchlist_items' check failed.")
    except Exception as e:
        print(f"❌ Cleanup Error: {e}")

# --- HEARTBEAT TO KEEP WS ALIVE ---
def start_heartbeat():
    global is_ws_ready, sws_instance
    while True:
        if is_ws_ready and sws_instance:
            try:
                # Nifty Spot (26000) check as a pulse to keep connection alive
                sws_instance.subscribe("hb_pulse", 1, [{"exchangeType": 1, "tokens": ["26000"]}])
                # print("💓 Connection Pulse Sent")
            except:
                pass
        eventlet.sleep(30)

# --- CALLBACKS ---
def on_data(wsapp, msg):
    if isinstance(msg, dict):
        # Handle both LTP and last_traded_price keys
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp')
        token = msg.get('token')
        
        if ltp_raw is not None and token:
            try:
                # Angel price is multiplied by 100
                lp = str(float(ltp_raw) / 100)
                token_str = str(token).strip()
                
                # 1. Update Redis
                r.set(f"price:{token_str}", lp)
                
                # 2. Broadcast to Socket.io users
                sio.emit('livePrice', {"tk": token_str, "lp": lp})
            except Exception as e:
                print(f"❌ Data processing error: {e}")

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("✅ Angel WebSocket Connected Successfully")

def on_error(wsapp, error):
    global is_ws_ready
    is_ws_ready = False
    print(f"❌ WS Error: {error}")

def on_close(wsapp):
    global is_ws_ready, active_subscriptions
    is_ws_ready = False
    active_subscriptions.clear()
    print("🔌 WS Connection Closed. Retrying login in 10s...")
    eventlet.sleep(10)
    login_to_angel()

def login_to_angel():
    global sws_instance, is_ws_ready
    try:
        API_KEY = "85HE4VA1"
        CLIENT_ID = "S52638556"
        PIN = "0000" 
        TOTP_KEY = "XFTXZ2445N4V2UMB7EWUCBDRMU"
        
        obj = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_KEY).now()
        session = obj.generateSession(CLIENT_ID, PIN, totp)
        
        if session.get('status'):
            sws_instance = SmartWebSocketV2(
                session['data']['jwtToken'], API_KEY, CLIENT_ID, session['data']['feedToken']
            )
            sws_instance.on_data = on_data
            sws_instance.on_open = on_open
            sws_instance.on_error = on_error
            sws_instance.on_close = on_close
            
            eventlet.spawn(sws_instance.connect)
            print("🚀 Angel Login Successful")
            cleanup_expired_contracts()
    except Exception as e:
        print(f"❌ Angel Login Failed: {e}")

# --- EXCHANGE LOGIC ---
def get_exchange_type(token):
    try:
        t = int(token)
        if 30000 <= t <= 999999: return 2 # NFO
        if t >= 1000000: return 5         # MCX
        return 1                          # NSE
    except: return 2

# --- SOCKET EVENTS ---
@sio.event
def connect(sid, environ):
    print(f"📱 User Connected: {sid}")
    if not is_ws_ready:
        login_to_angel()

@sio.event
def subscribe(sid, data):
    global sws_instance, is_ws_ready
    if data and sws_instance and is_ws_ready:
        def do_subscribe():
            tokens = data if isinstance(data, list) else [data]
            new_tokens = [str(t).strip() for t in tokens if str(t).strip()]
            
            if not new_tokens: return

            grouped = {}
            for t in new_tokens:
                etype = get_exchange_type(t)
                if etype not in grouped: grouped[etype] = []
                grouped[etype].append(t)

            for etype, t_list in grouped.items():
                correlation_id = f"sub_{sid}_{etype}"
                try:
                    # Subscribe mode 1 (LTP)
                    sws_instance.subscribe(correlation_id, 1, [{"exchangeType": etype, "tokens": t_list}])
                    print(f"📡 Subscribed: {t_list} (Exchange {etype})")
                    eventlet.sleep(0.5) 
                except Exception as e:
                    print(f"❌ Sub Error: {e}")
        eventlet.spawn(do_subscribe)

def app(environ, start_response):
    if environ.get('PATH_INFO') == '/':
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b"MYT PRO BACKEND - PULSE MODE ACTIVE"]
    return socketio_app(environ, start_response)

if __name__ == '__main__':
    # Start Heartbeat Thread
    eventlet.spawn(start_heartbeat)
    # Initial login
    login_to_angel()
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
