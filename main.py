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

def market_data_tunnel():
    global subscribed_tokens
    while True:
        try:
            now = datetime.datetime.now().strftime("%H:%M")
            if now in ["09:00", "21:00"]:
                print(f"⏰ Scheduled Cleanup at {now}...")
                subscribed_tokens.clear() 
                eventlet.sleep(65)
            eventlet.sleep(30)
        except Exception as e: print(f"Tunnel 1 Error: {e}"); eventlet.sleep(60)

# 🚀 Tunnel 2: Smart Batching (500 tokens) & Unique Logic
def watchlist_live_tunnel():
    global subscribed_tokens
    while True:
        try:
            if sws_instance and is_ws_ready:
                res = supabase.table("watchlist_items").select("token, exch_seg").execute()
                # Unique Set: 10k users same token dekhenge tab bhi 1 hi count hoga
                all_unique_data = { (str(item['token']), item['exch_seg']) for item in res.data if item.get('token') }
                
                # Filter naye tokens
                new_to_sub = [t for t in all_unique_data if t[0] not in subscribed_tokens]

                if new_to_sub:
                    # 500-500 ke batch mein divide karna
                    for i in range(0, len(new_to_sub), 500):
                        batch = new_to_sub[i:i+500]
                        for ex_name, ex_code in [("MCX", 5), ("NFO", 2), ("BSE", 3), ("BFO", 4), ("NSE", 1)]:
                            tokens = [t[0] for t in batch if t[1] == ex_name]
                            if tokens:
                                sws_instance.subscribe("bhai_task", 1, [{"exchangeType": ex_code, "tokens": tokens}])
                                for t in tokens: subscribed_tokens.add(t)
                                print(f"✅ Batched {len(tokens)} tokens for {ex_name}")
        except Exception as e: print(f"Tunnel 2 Error: {e}")
        eventlet.sleep(0.5)

if __name__ == '__main__':
    eventlet.spawn(login_to_angel)
    eventlet.spawn(market_data_tunnel)
    eventlet.spawn(watchlist_live_tunnel)
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', int(os.environ.get("PORT", 10000)))), socketio_app)
