import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, firebase_admin, pytz
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
IST = pytz.timezone('Asia/Kolkata')

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
active_subscriptions = set() # Format: "token_exchangeType"
token_to_fb_keys = {}        # Format: {"token": ["fb_key1", "fb_key2"]}
last_price_cache = {} 
last_tick_time = time.time() 

# --- 2. MARKET HOURS TIMER ---
def is_market_open():
    now = datetime.datetime.now(IST)
    if now.weekday() >= 5: return False 
    start_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
    end_time = now.replace(hour=23, minute=30, second=0, microsecond=0)
    return start_time <= now <= end_time

# --- 3. PRO TICK ENGINE ---
def on_data(wsapp, msg):
    global last_price_cache, last_tick_time
    if isinstance(msg, dict):
        token = msg.get('token')
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100
        
        if ltp > 0 and token in token_to_fb_keys:
            last_tick_time = time.time() 
            
            # Delta Logic: Price change nahi toh update nahi
            if last_price_cache.get(token) == ltp:
                return 

            now_time = datetime.datetime.now(IST).strftime("%H:%M:%S")
            updates = {}
            
            for fb_key in token_to_fb_keys[token]:
                # Dynamic Precision
                is_cds = "CDS" in fb_key.upper() 
                formatted_lp = "{:.4f}".format(ltp) if is_cds else "{:.2f}".format(ltp)
                
                updates[f"central_watchlist/{fb_key}/price"] = formatted_lp
                updates[f"central_watchlist/{fb_key}/utime"] = now_time
            
            if updates:
                db.reference().update(updates)
                last_price_cache[token] = ltp
                del updates 

# --- 4. WATCHDOG MONITOR ---
def watchdog_monitor():
    global is_ws_ready
    while True:
        if is_ws_ready and is_market_open():
            if time.time() - last_tick_time > 45:
                print("⚠️ [WATCHDOG] Stream Frozen! Resetting...")
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
                print(f"🔄 [AUTH] Logging into Angel One (IST: {datetime.datetime.now(IST).strftime('%H:%M:%S')})")
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
            print(f"💤 [SLEEP] Market Closed. Engine resting...")
            eventlet.sleep(300) 
        eventlet.sleep(10)

# --- 5. MAINTENANCE LOOP (Structure Fixed) ---
def maintenance_loop():
    global active_subscriptions, token_to_fb_keys
    while True:
        if is_market_open():
            try:
                ref = db.reference('central_watchlist').get()
                if ref:
                    temp_map, batch = {}, {1: [], 2: [], 3: [], 5: []}
                    
                    for fb_key, data in ref.items():
                        if not isinstance(data, dict): continue
                        
                        # Firebase ke ANDAR se data nikaalo
                        t_id = str(data.get('token', ''))
                        exch_seg = str(data.get('exch_seg', 'NSE')).upper()
                        
                        if not t_id or t_id == 'None': continue

                        # Exchange Mapping Logic
                        ex_type = 1
                        if "MCX" in exch_seg: ex_type = 5
                        elif "BSE" in exch_seg: ex_type = 3
                        elif any(x in exch_seg for x in ["NFO", "FUT", "OPT"]): ex_type = 2
                        
                        sub_key = f"{t_id}_{ex_type}"
                        
                        if t_id not in temp_map:
                            temp_map[t_id] = []
                            if sub_key not in active_subscriptions:
                                batch[ex_type].append(t_id)
                                active_subscriptions.add(sub_key)
                        
                        if fb_key not in temp_map[t_id]:
                            temp_map[t_id].append(fb_key)
                    
                    token_to_fb_keys = temp_map
                    
                    if is_ws_ready:
                        for ex_type, tokens in batch.items():
                            if tokens:
                                sws.subscribe("myt_task", 1, [{"exchangeType": ex_type, "tokens": tokens}])
                                print(f"🚀 [SYNC] Subscribed {len(tokens)} symbols on Exchange {ex_type}")
                del ref
            except Exception as e:
                print(f"⚠️ [MAINTENANCE ERROR]: {e}")
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
