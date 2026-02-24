import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, socketio, redis, time, datetime
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

sws_instance = None
is_ws_ready = False 
subscribed_tokens = set()

def on_data(wsapp, msg):
    if isinstance(msg, dict):
        token = msg.get('token')
        ltp = msg.get('last_traded_price') or msg.get('ltp')
        if ltp and token:
            lp = "{:.2f}".format(float(ltp) / 100)
            sio.emit('livePrice', {"tk": str(token).strip(), "lp": lp})

def login_to_angel():
    global sws_instance, is_ws_ready
    try:
        obj = SmartConnect(api_key="85HE4VA1")
        session = obj.generateSession("S52638556", "0000", pyotp.TOTP("XFTXZ2445N4V2UMB7EWUCBDRMU").now())
        if session.get('status'):
            sws_instance = SmartWebSocketV2(session['data']['jwtToken'], "85HE4VA1", "S52638556", session['data']['feedToken'])
            sws_instance.on_data = on_data
            sws_instance.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('✅ WS Ready')")
            eventlet.spawn(sws_instance.connect)
    except Exception as e: print(f"❌ Login Error: {e}")

# 🚀 Tunnel 1: Morning 9 AM & Night 9 PM Cleanup Logic
def market_data_tunnel():
    global subscribed_tokens
    while True:
        try:
            now = datetime.datetime.now().strftime("%H:%M")
            if now in ["09:00", "21:00"]:
                print(f"⏰ Scheduled Cleanup at {now}: Refreshing Tokens...")
                subscribed_tokens.clear() # Sari memory clear taaki naye expiry/tokens load ho sakein
                eventlet.sleep(65) # Ek minute wait taaki loop repeat na ho
            eventlet.sleep(30)
        except Exception as e: print(f"Tunnel 1 Error: {e}"); eventlet.sleep(60)

# 🚀 Tunnel 2: 0.1s Fast Sync with Unique Set Logic
def watchlist_live_tunnel():
    global subscribed_tokens
    while True:
        try:
            if sws_instance and is_ws_ready:
                res = supabase.table("watchlist_items").select("token, exch_seg").execute()
                # Set comprehension se duplicates database level par hi hat jayenge
                active_db_data = { (str(item['token']), item['exch_seg']) for item in res.data if item.get('token') }
                
                for token, exch in active_db_data:
                    if token not in subscribed_tokens:
                        if exch == "MCX": ex_code = 5
                        elif exch == "NFO": ex_code = 2
                        elif exch == "BSE": ex_code = 3
                        elif exch == "BFO": ex_code = 4
                        elif exch == "CDS": ex_code = 7
                        else: ex_code = 1
                        
                        sws_instance.subscribe("bhai_task", 1, [{"exchangeType": ex_code, "tokens": [token]}])
                        subscribed_tokens.add(token)
                        print(f"✅ Live: {token} ({exch})")
        except Exception as e: print(f"Tunnel 2 Error: {e}")
        eventlet.sleep(0.1) # 🔥 Super Fast Update

if __name__ == '__main__':
    eventlet.spawn(login_to_angel)
    eventlet.spawn(market_data_tunnel)
    eventlet.spawn(watchlist_live_tunnel)
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', int(os.environ.get("PORT", 10000)))), socketio_app)
