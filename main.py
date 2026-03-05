import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, firebase_admin, pytz, json
from firebase_admin import credentials, db
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- 1. FIREBASE SETUP (As per your Screenshot) ---
JSON_FILE = "trade-f600a-firebase-adminsdk-fbsvc-269ab50c0c.json"
if not firebase_admin._apps:
    cred = credentials.Certificate(JSON_FILE)
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://trade-f600a-default-rtdb.firebaseio.com/'
    })

# --- CONFIGURATION ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
token_to_fb_keys = {}  # Mapping: { "472781": ["472781_bO2O..."] }
last_price_cache = {} 
last_tick_time = time.time()

# --- 2. MARKET HOURS (Indian Market + MCX) ---
def is_market_open():
    now = datetime.datetime.now(IST)
    if now.weekday() >= 5: return False # Sat/Sun Off
    # MCX 11:50 PM tak chalta hai
    return 9 <= now.hour < 24

# --- 3. TICK ENGINE (Deep Price Matching) ---
def on_data(wsapp, msg):
    global last_tick_time, last_price_cache
    
    # AngelOne sends data as dict
    if isinstance(msg, dict) and 'token' in msg:
        token = msg.get('token')
        last_tick_time = time.time()
        
        # LTP Conversion: AngelOne sends in paise (e.g. 7200000 for 72000.00)
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100
        
        if ltp > 0 and token in token_to_fb_keys:
            # Skip if price hasn't changed to save Firebase Bandwidth
            if last_price_cache.get(token) == ltp:
                return

            now_time = datetime.datetime.now(IST).strftime("%H:%M:%S")
            updates = {}
            
            for fb_key in token_to_fb_keys[token]:
                # PRECISION LOGIC: MCX/Currency needs 4 decimals, others 2
                # screenshot mein GOLDM hai, iska exch_seg: "MCX" hai
                # Hum node path se precision check karenge
                path = f"central_watchlist/{fb_key}"
                
                # Default 2 decimal, MCX ke liye 4
                is_mcx = "MCX" in fb_key.upper() or "GOLD" in fb_key.upper()
                formatted_price = "{:.4f}".format(ltp) if is_mcx else "{:.2f}".format(ltp)
                
                updates[f"{path}/price"] = str(formatted_price)
                updates[f"{path}/utime"] = now_time
                
                # pChange calculation (if close price available in msg)
                if 'close' in msg and msg['close'] > 0:
                    cp = float(msg['close']) / 100
                    p_chng = ((ltp - cp) / cp) * 100
                    updates[f"{path}/pChange"] = "{:.2f}".format(p_chng)

            if updates:
                try:
                    db.reference().update(updates)
                    last_price_cache[token] = ltp
                except Exception as e:
                    print(f"❌ FB Update Error: {e}")

# --- 4. AUTH & SMART WEB SOCKET ---
def login_and_connect():
    global sws, is_ws_ready
    while True:
        try:
            print(f"🔄 [AUTH] Logging into AngelOne...")
            smart_api = SmartConnect(api_key=API_KEY)
            session = smart_api.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
            
            if session.get('status'):
                feed_token = session['data']['feedToken']
                jwt_token = session['data']['jwtToken']
                
                sws = SmartWebSocketV2(jwt_token, API_KEY, CLIENT_CODE, feed_token)
                sws.on_data = on_data
                sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('🟢 Market Engine Live')")
                sws.on_close = lambda ws, c, r: exec("global is_ws_ready; is_ws_ready=False")
                
                eventlet.spawn(sws.connect)
                break
            else:
                print(f"❌ Session Failed: {session.get('message')}")
        except Exception as e:
            print(f"⚠️ Auth Exception: {e}")
        eventlet.sleep(20)

# --- 5. SYNC LOOP (Matches Screenshot Structure) ---
def sync_watchlist():
    global token_to_fb_keys
    while True:
        try:
            if is_ws_ready:
                # Screenshot path: central_watchlist
                full_data = db.reference('central_watchlist').get()
                
                if full_data:
                    new_token_map = {}
                    subscriptions = {1: [], 2: [], 5: []} # 1:NSE, 2:NFO, 5:MCX
                    
                    for fb_key, val in full_data.items():
                        token = str(val.get('token', ''))
                        exch = str(val.get('exch_seg', 'NSE')).upper()
                        
                        if not token or token == "None": continue
                        
                        # Map token to fb_key (one token can be in many users' watchlists)
                        if token not in new_token_map: new_token_map[token] = []
                        new_token_map[token].append(fb_key)
                        
                        # Determine AngelOne Exchange Type
                        e_type = 1 # NSE
                        if "MCX" in exch: e_type = 5
                        elif "NFO" in exch or "FUT" in exch: e_type = 2
                        
                        subscriptions[e_type].append(token)
                    
                    token_to_fb_keys = new_token_map
                    
                    # Subscribe to all tokens found in Firebase
                    for etype, tokens in subscriptions.items():
                        if tokens:
                            # Split into 50 tokens per batch (AngelOne Limit)
                            for i in range(0, len(tokens), 50):
                                batch = tokens[i:i+50]
                                sws.subscribe("myt_task", 1, [{"exchangeType": etype, "tokens": batch}])
                                print(f"🚀 Subscribed {len(batch)} tokens on Exch {etype}")
                                
            eventlet.sleep(60) # Har 1 minute mein sync karo agar naya symbol add hua ho
        except Exception as e:
            print(f"⚠️ Sync Loop Error: {e}")
            eventlet.sleep(10)

if __name__ == '__main__':
    # Start all threads
    eventlet.spawn(login_and_connect)
    eventlet.spawn(sync_watchlist)
    
    # Simple Web Server for Render
    from eventlet import wsgi
    def app(env, res):
        res('200 OK', [('Content-Type', 'text/plain')])
        return [b"TRADE ENGINE IS RUNNING"]
    
    port = int(os.environ.get("PORT", 10000))
    wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
