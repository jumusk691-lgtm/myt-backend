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
print("✅ Firebase Connected! Deep-Matching Engine Ready.")

# --- CONFIGURATION ---
API_KEY, CLIENT_CODE, PWD, TOTP_STR = "85HE4VA1", "S52638556", "0000", "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
active_subscriptions = set()
token_to_fb_keys = {} 
last_price_cache = {} 
last_tick_time = time.time()

# --- 2. MARKET HOURS TIMER ---
def is_market_open():
    now = datetime.datetime.now(IST)
    if now.weekday() >= 5: return False 
    return 9 <= now.hour < 24

# --- 3. PRO TICK ENGINE (Deep Matching) ---
def on_data(wsapp, msg):
    global last_price_cache, last_tick_time
    if isinstance(msg, dict) and 'token' in msg:
        token = msg.get('token')
        last_tick_time = time.time() 
        
        # LTP conversion (AngelOne sends in paise)
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100
        
        if ltp > 0 and token in token_to_fb_keys:
            # Cache check to reduce Firebase writes
            if last_price_cache.get(token) == ltp: 
                return 

            now_time = datetime.datetime.now(IST).strftime("%H:%M:%S")
            updates = {}
            for fb_key in token_to_fb_keys[token]:
                # MATCHING LOGIC: Check if it's MCX/Gold for 4-decimal precision
                # We check the key name or the cached exchange info
                is_hi_prec = any(x in fb_key.upper() for x in ["MCX", "CDS", "GOLD", "SILVER", "CRUDE"])
                
                formatted_lp = "{:.4f}".format(ltp) if is_hi_prec else "{:.2f}".format(ltp)
                
                updates[f"central_watchlist/{fb_key}/price"] = formatted_lp
                updates[f"central_watchlist/{fb_key}/utime"] = now_time
            
            if updates:
                try:
                    db.reference().update(updates)
                    last_price_cache[token] = ltp
                except Exception as e:
                    print(f"Update Error: {e}")

# --- 4. AUTH & CONNECTION ---
def login_and_connect():
    global sws, is_ws_ready, active_subscriptions
    while True:
        try:
            print(f"🔄 [AUTH] Connecting Angel One...")
            obj = SmartConnect(api_key=API_KEY)
            session = obj.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
            if session.get('status'):
                active_subscriptions.clear()
                sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                sws.on_data = on_data
                sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('🟢 [SYSTEM] Engine Live')")
                sws.on_close = lambda ws, c, r: exec("global is_ws_ready; is_ws_ready=False")
                eventlet.spawn(sws.connect)
                break
        except Exception as e: 
            print(f"❌ Connection Error: {e}")
        eventlet.sleep(15)

# --- 5. MAINTENANCE LOOP (Deep Symbol Matching) ---
def maintenance_loop():
    global active_subscriptions, token_to_fb_keys
    while True:
        try:
            # Fetch full watchlist from Firebase
            ref = db.reference('central_watchlist').get()
            if ref and is_ws_ready:
                temp_map, batch = {}, {1: [], 2: [], 3: [], 5: []}
                
                for fb_key, data in ref.items():
                    if not isinstance(data, dict): continue
                    
                    # Android app sends 'token' or 'symbol'
                    t_id = str(data.get('token', ''))
                    ex_name = str(data.get('exch_seg', 'NSE')).upper()
                    
                    if not t_id or t_id == 'None' or t_id == "": 
                        continue

                    # Deep Matching: Store fb_key against token
                    if t_id not in temp_map: 
                        temp_map[t_id] = []
                    temp_map[t_id].append(fb_key)

                    # Determine Exchange Type for AngelOne
                    # NSE: 1, NFO: 2, BSE: 3, MCX: 5
                    ex_type = 1
                    if "MCX" in ex_name: ex_type = 5
                    elif "BSE" in ex_name: ex_type = 3
                    elif "NFO" in ex_name or "FUT" in ex_name or "OPT" in ex_name: ex_type = 2
                    
                    sub_key = f"{t_id}_{ex_type}"
                    if sub_key not in active_subscriptions:
                        batch[ex_type].append(t_id)
                        active_subscriptions.add(sub_key)
                
                token_to_fb_keys = temp_map
                
                # Subscribe in batches
                for ex_type, tokens in batch.items():
                    if tokens:
                        # AngleOne limit is usually 50 symbols per request
                        for i in range(0, len(tokens), 50):
                            chunk = tokens[i:i + 50]
                            sws.subscribe("myt_task", 1, [{"exchangeType": ex_type, "tokens": chunk}])
                            print(f"🚀 [MATCHED] Subscribed {len(chunk)} symbols on Exchange {ex_type}")
            
            del ref # Memory cleanup
        except Exception as e: 
            print(f"⚠️ Loop Error: {e}")
        eventlet.sleep(30)

# --- 6. SERVER & WATCHDOG ---
def watchdog_monitor():
    global is_ws_ready
    while True:
        if is_ws_ready and is_market_open():
            if time.time() - last_tick_time > 60:
                print("⚠️ [WATCHDOG] Stream Frozen! Reconnecting...")
                is_ws_ready = False
                if sws: 
                    try: sws.close()
                    except: pass
                eventlet.spawn(login_and_connect)
        eventlet.sleep(20)

if __name__ == '__main__':
    eventlet.spawn(login_and_connect)
    eventlet.spawn(maintenance_loop)
    eventlet.spawn(watchdog_monitor)
    
    from eventlet import wsgi
    port = int(os.environ.get("PORT", 10000))
    def app(env, res):
        res('200 OK', [('Content-Type', 'text/plain')])
        return [b"MATCHING ENGINE ACTIVE"]
    wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
