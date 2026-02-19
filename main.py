import eventlet
eventlet.monkey_patch(all=True)

import os
import pyotp
import socketio
import redis
import json
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
    r = redis.from_url(redis_url, decode_responses=True)
    
    # Supabase configuration
    supabase_url = "https://rcosgmsyisybusmuxzei.supabase.co"
    # User provided Service Role Key for full access
    supabase_key = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDgzOTEzNCwiZXhwIjoyMDg2NDE1MTM0fQ.5BofQbMKiMLGFjqcIGaCwpoO9pLZnuLg7nojP0aGhJw") 
    supabase = create_client(supabase_url, supabase_key)

initialize_clients()

sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
socketio_app = socketio.WSGIApp(sio)

# --- DATABASE CLEANUP ---
def cleanup_expired_contracts():
    """Purana expired data Supabase aur Redis se hatana"""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        print(f"🧹 Cleaning expired contracts before: {today}")
        
        # Supabase cleanup (Table: watchlist_items में expiry column के आधार पर)
        # Auth error fix: valid service_role key use ho rahi hai
        try:
            supabase.table("watchlist_items").delete().lt("expiry", today).execute()
            print("✅ Supabase Cleanup Success")
        except Exception as db_e:
            print(f"⚠️ Supabase Delete Error: {db_e}")
            
    except Exception as e:
        print(f"❌ Cleanup Global Error: {e}")

# --- EXCHANGE LOGIC ---
def get_exchange_type(token):
    try:
        t = int(token)
        if 35000 <= t <= 999999: return 2  # NFO (Options/Futures)
        if t >= 1000000: return 5          # MCX (Commodity)
        return 1                           # NSE (Stocks)
    except:
        return 2

# --- CALLBACKS ---
def on_data(wsapp, msg):
    if isinstance(msg, dict):
        token = msg.get('token')
        # LTP detection for V2
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp')
        
        if ltp_raw is not None and token:
            lp = str(float(ltp_raw) / 100)
            token_str = str(token).strip()
            
            # Redis update
            r.set(f"price:{token_str}", lp)
            
            # Frontend broadcast
            sio.emit('livePrice', {"tk": token_str, "lp": lp})

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("✅ Angel WebSocket Connected")

def on_error(wsapp, error):
    global is_ws_ready
    is_ws_ready = False
    print(f"❌ WS Error: {error}")

def on_close(wsapp):
    global is_ws_ready, active_subscriptions
    is_ws_ready = False
    active_subscriptions.clear()
    print("🔌 Connection Closed. Reconnecting in 5s...")
    eventlet.sleep(5)
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
        print(f"❌ Login Failed: {e}")

# --- SOCKET EVENTS ---
@sio.event
def connect(sid, environ):
    print(f"📱 User Connected: {sid}")
    if not is_ws_ready:
        login_to_angel()

@sio.event
def subscribe(sid, data):
    global sws_instance, is_ws_ready, active_subscriptions
    
    if data and sws_instance and is_ws_ready:
        def do_subscribe():
            tokens_to_sub = data if isinstance(data, list) else [data]
            new_tokens = [str(t) for t in tokens_to_sub]
            
            grouped = {}
            for t in new_tokens:
                etype = get_exchange_type(t)
                if etype not in grouped: grouped[etype] = []
                grouped[etype].append(t)

            for etype, tokens in grouped.items():
                correlation_id = f"sub_{sid}_{etype}"
                try:
                    # Rate limiting fix: sleep added to prevent socket close
                    sws_instance.subscribe(correlation_id, 1, [{"exchangeType": etype, "tokens": tokens}])
                    for t in tokens: active_subscriptions.add(t)
                    print(f"📡 Subscribed: {len(tokens)} tokens on Exch {etype}")
                    eventlet.sleep(0.3) 
                except Exception as e:
                    print(f"❌ Subscription Failed: {e}")

        eventlet.spawn(do_subscribe)

def app(environ, start_response):
    if environ.get('PATH_INFO') == '/':
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b"MYT PRO BACKEND - MCX/NFO/CASH READY"]
    return socketio_app(environ, start_response)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
