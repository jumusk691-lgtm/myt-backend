import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, socketio, redis, time
from supabase import create_client
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- CONFIGURATION ---
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDgzOTEzNCwiZXhwIjoyMDg2NDE1MTM0fQ.5BofQbMKiMLGFjqcIGaCwpoO9pLZnuLg7nojP0aGhJw"
REDIS_URL = os.environ.get("REDIS_URL")

mgr = socketio.RedisManager(REDIS_URL)
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet', client_manager=mgr)
socketio_app = socketio.WSGIApp(sio)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Global State
sws_instance = None
is_ws_ready = False 
subscribed_tokens = set()
MAX_LIMIT = 3000  # Aapki batayi hui limit

def on_data(wsapp, msg):
    if isinstance(msg, dict):
        token = msg.get('token')
        ltp = msg.get('last_traded_price') or msg.get('ltp')
        if ltp and token:
            lp = "{:.2f}".format(float(ltp) / 100)
            # Saare users ko unique price broadcast karna
            sio.emit('livePrice', {"tk": str(token).strip(), "lp": lp})

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("✅ WebSocket Connected!")

def login_to_angel():
    global sws_instance
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
            sws_instance.on_error = lambda ws, err: print(f"❌ WS Error: {err}")
            sws_instance.on_close = lambda ws: login_to_angel()
            eventlet.spawn(sws_instance.connect)
    except Exception as e:
        print(f"❌ Login Error: {e}")

# 🔥 UNIQUE TOKEN SYNC LOGIC
def sync_unique_watchlist():
    global subscribed_tokens, sws_instance, is_ws_ready
    while True:
        try:
            if sws_instance and is_ws_ready:
                # 1. Sabhi users ki watchlist se tokens uthao
                res = supabase.table("watchlist_items").select("token").execute()
                
                # 2. Python Set se duplicates hatana (Unique IDs)
                db_unique_tokens = {str(item['token']) for item in res.data if item.get('token')}
                
                # 3. Limit lagana (Max 3000 tokens)
                limited_tokens = set(list(db_unique_tokens)[:MAX_LIMIT])

                # 4. Sirf naye unique tokens ko subscribe karna
                new_tokens = limited_tokens - subscribed_tokens
                
                if new_tokens:
                    print(f"📡 Subscribing to {len(new_tokens)} New Unique Tokens")
                    token_list = []
                    for t in new_tokens:
                        ex_type = 2 if len(t) > 5 else 1 
                        token_list.append({"exchangeType": ex_type, "tokens": [t]})
                    
                    sws_instance.subscribe("bhai_unique_sync", 1, token_list)
                    subscribed_tokens.update(new_tokens)
                    
        except Exception as e:
            print(f"🔄 Sync Error: {e}")
        eventlet.sleep(30) # Har 30 sec mein naye user additions check karega

if __name__ == '__main__':
    eventlet.spawn(login_to_angel)
    eventlet.spawn(sync_unique_watchlist)
    port = int(os.environ.get("PORT", 10000))
    print(f"🚀 Backend Live on Port {port} with Unique Sync Logic")
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), socketio_app)
