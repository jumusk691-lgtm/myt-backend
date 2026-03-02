import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, firebase_admin
from firebase_admin import credentials, db
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- 1. FIREBASE SETUP ---
JSON_FILE = "trade-f600a-firebase-adminsdk-fbsvc-269ab50c0c.json"
if not firebase_admin._apps:
    cred = credentials.Certificate(JSON_FILE)
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://trade-f600a-default-rtdb.firebaseio.com/'
    })
print("✅ Firebase Connected! Master Hybrid Engine Ready.")

# --- CONFIGURATION ---
API_KEY, CLIENT_CODE, PWD, TOTP_STR = "85HE4VA1", "S52638556", "0000", "XFTXZ2445N4V2UMB7EWUCBDRMU"

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
active_subscriptions = set()
token_to_fb_keys = {}
last_price_cache = {} 
last_tick_time = time.time() 

# --- 2. MARKET HOURS TIMER ---
def is_market_open():
    now = datetime.datetime.now()
    if now.weekday() >= 5: return False 
    return now.replace(hour=9, minute=0) <= now <= now.replace(hour=23, minute=30)

# --- 3. PRO TICK ENGINE ---
def on_data(wsapp, msg):
    global last_price_cache, last_tick_time
    if isinstance(msg, dict):
        token = msg.get('token')
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100
        
        if ltp > 0 and token in token_to_fb_keys:
            last_tick_time = time.time() 
            
            # 🔥 Delta Logic
            if last_price_cache.get(token) == ltp:
                return 

            # 🔥 Precision Check
            is_cds = any("CDS" in k.upper() for k in token_to_fb_keys[token])
            formatted_lp = "{:.4f}".format(ltp) if is_cds else "{:.2f}".format(ltp)
            
            now_time = datetime.datetime.now().strftime("%H:%M:%S")
            updates = {}
            for fb_key in token_to_fb_keys[token]:
                updates[f"central_watchlist/{fb_key}/price"] = formatted_lp
                updates[f"central_watchlist/{fb_key}/utime"] = now_time
            
            if updates:
                db.reference().update(updates)
                last_price_cache[token] = ltp
                # Instant Memory Release
                del updates 

# --- 4. WATCHDOG MONITOR ---
def watchdog_monitor():
    global is_ws_ready
    while True:
        if is_ws_ready and is_market_open():
            if time.time() - last_tick_time > 45:
                print("⚠️ [WATCHDOG] Stream Frozen! Restarting...")
                if sws: 
                    try: sws.close()
                    except: pass
                is_ws_ready = False
        eventlet.sleep(15)

def login_and_connect():
    global sws, is_ws_ready
    while True:
        if is_market_open():
            try:
                print("🔄 [AUTH] Logging into Angel One...")
                obj = SmartConnect(api_key=API_KEY)
                session = obj.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
                
                if session.get('status'):
                    sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                    sws.on_data = on_data
                    sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('🟢 [ENGINE] System Live')")
                    sws.on_close = lambda ws, c, r: print("🔌 [OFFLINE] Connection Lost.")
                    eventlet.spawn(sws.connect)
                    break
            except Exception as e:
                print(f"❌ [AUTH] Login Error: {e}")
        else:
            print("💤 [SLEEP] Market Closed. Engine resting...")
            eventlet.sleep(300) 
        eventlet.sleep(10)

# --- 5. MAINTENANCE LOOP ---
def maintenance_loop():
    global active_subscriptions, token_to_fb_keys
    while True:
        if is_market_open():
            try:
                ref = db.reference('central_watchlist').get()
                if ref:
                    temp_map, batch = {}, {1: [], 2: [], 3: [], 5: []}
                    for fb_key in ref.keys():
                        t_id = str(fb_key.split('_')[0])
                        k = fb_key.upper()
                        ex = 5 if "MCX" in k else (2 if any(x in k for x in ["NFO", "FUT", "OPT"]) else (3 if "BSE" in k else 1))
                        
                        # Sub-key for tracking (Token + Exchange)
                        sub_key = f"{t_id}_{ex}"
                        
                        if t_id not in temp_map:
                            temp_map[t_id] = []
                            if sub_key not in active_subscriptions:
                                batch[ex].append(t_id)
                                active_subscriptions.add(sub_key)
                        temp_map[t_id].append(fb_key)
                    
                    token_to_fb_keys = temp_map
                    if is_ws_ready:
                        for ex_type, tokens in batch.items():
                            if tokens:
                                sws.subscribe("myt_task", 1, [{"exchangeType": ex_type, "tokens": tokens}])
                                print(f"🚀 [SYNC] Subscribed {len(tokens)} symbols on Exchange {ex_type}")
                del ref
            except: pass
        eventlet.sleep(20)

# --- 6. SERVER ---
if __name__ == '__main__':
    eventlet.spawn(login_and_connect)
    eventlet.spawn(maintenance_loop)
    eventlet.spawn(watchdog_monitor)
    
    from eventlet import wsgi
    port = int(os.environ.get("PORT", 10000))
    def app(env, res):
        res('200 OK', [('Content-Type', 'text/plain')])
        return [b"MASTER BROKER ENGINE ACTIVE"]
    
    print(f"🌍 [SERVER] Listening on port {port}")
    wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
