import eventlet
eventlet.monkey_patch(all=True)

import os
import pyotp
import socketio
import redis
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
        print("❌ ERROR: REDIS_URL environment variable-e nei!")
    r = redis.from_url(redis_url, decode_responses=True)
    
    # Supabase configuration
    supabase_url = "https://rcosgmsyisybusmuxzei.supabase.co"
    # Service Role Key
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDgzOTEzNCwiZXhwIjoyMDg2NDE1MTM0fQ.5BofQbMKiMLGFjqcIGaCwpoO9pLZnuLg7nojP0aGhJw"
    
    supabase = create_client(supabase_url, supabase_key)

initialize_clients()

sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
socketio_app = socketio.WSGIApp(sio)

# --- DATABASE CLEANUP ---
def cleanup_expired_contracts():
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        print(f"🧹 Expired contracts delete kora hocche: {today}")
        try:
            supabase.table("watchlist_items").delete().lt("expiry", today).execute()
            print("✅ Supabase Cleanup Success")
        except Exception as table_e:
            print(f"⚠️ Cleanup failed: Table pawa jayni ba Auth error.")
    except Exception as e:
        print(f"❌ Cleanup Error: {e}")

# --- EXCHANGE LOGIC ---
def get_exchange_type(token):
    try:
        t = int(token)
        # NFO (Options) usually 30000+ hoy
        if 30000 <= t <= 999999: return 2  
        # MCX
        if t >= 1000000: return 5          
        # NSE Cash
        return 1                           
    except: return 2

# --- CALLBACKS ---
def on_data(wsapp, msg):
    if isinstance(msg, dict):
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp')
        token = msg.get('token')
        
        if ltp_raw is not None and token:
            try:
                # Angel One price 100 diye gile pathay, tai divide kora hoyeche
                lp = str(float(ltp_raw) / 100)
                token_str = str(token).strip()
                
                # Redis-e update
                r.set(f"price:{token_str}", lp)
                
                # Live Socket Broadcast (Sudhu active user-der jonno)
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
    print("🔌 WS Connection Closed. Retrying in 5s...")
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
        print(f"❌ Angel Login Failed: {e}")

# --- SOCKET EVENTS ---
@sio.event
def connect(sid, environ):
    print(f"📱 User Connected: {sid}")
    # User connect holei Angel login check korbe
    if not is_ws_ready:
        login_to_angel()

@sio.event
def subscribe(sid, data):
    """Ekhane sudhu user-er pathano specific tokens subscribe hoy"""
    global sws_instance, is_ws_ready, active_subscriptions
    if data and sws_instance and is_ws_ready:
        def do_subscribe():
            tokens = data if isinstance(data, list) else [data]
            # Token list clean kora hochche (extra space bad diye)
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
                    # Sudhu user-er watchlisted tokens-i subscribe hobe
                    sws_instance.subscribe(correlation_id, 1, [{"exchangeType": etype, "tokens": t_list}])
                    print(f"📡 Subscribed: {t_list} (Exchange {etype})")
                    eventlet.sleep(0.5) 
                except Exception as e:
                    print(f"❌ Sub Error: {e}")

        eventlet.spawn(do_subscribe)

def app(environ, start_response):
    if environ.get('PATH_INFO') == '/':
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b"MYT PRO BACKEND RUNNING - CLEAN MODE"]
    return socketio_app(environ, start_response)

if __name__ == '__main__':
    login_to_angel() # Initial login
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
