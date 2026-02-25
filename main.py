import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, socketio, redis, time, datetime, requests
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- CONFIGURATION ---
REDIS_URL = os.environ.get("REDIS_URL")
# Google Drive Direct Link (Aapki JSON file ka link yahan aayega)
MASTER_JSON_URL = "https://your_google_drive_direct_link.json" 

mgr = socketio.RedisManager(REDIS_URL)
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet', client_manager=mgr)
socketio_app = socketio.WSGIApp(sio)
r_client = redis.from_url(REDIS_URL, decode_responses=True)

sws_instance = None
is_ws_ready = False 

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

def watchlist_live_tunnel():
    while True:
        try:
            if sws_instance and is_ws_ready:
                # Ab data Supabase se nahi, Redis ya Local memory se aayega
                # Jo tokens users ne watchlist mein add kiye hain unhe subscribe karo
                all_tokens = r_client.smembers("active_user_tokens") 
                
                new_to_sub = [t for t in all_tokens if not r_client.sismember("global_subscribed_tokens", t)]

                if new_to_sub:
                    # Yahan exchange-wise subscribe logic (NSE, NFO, etc.)
                    # Ex: sws_instance.subscribe("bhai_task", 1, [{"exchangeType": 1, "tokens": new_to_sub}])
                    r_client.sadd("global_subscribed_tokens", *new_to_sub)
                    print(f"🚀 Redis Sync: {len(new_to_sub)} new tokens")
        except Exception as e: print(f"Tunnel 2 Error: {e}")
        eventlet.sleep(0.5)

if __name__ == '__main__':
    eventlet.spawn(login_to_angel)
    eventlet.spawn(watchlist_live_tunnel)
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', int(os.environ.get("PORT", 10000)))), socketio_app)
