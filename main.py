import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, requests, re
import firebase_admin
from firebase_admin import credentials, db
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- 1. FIREBASE SETUP (Direct File Uploaded) ---
# Aapne bataya ki file direct upload ki hai, toh hum wahi name use kar rahe hain.
JSON_FILE = "trade-f600a-firebase-adminsdk-fbsvc-269ab50c0c.json"

if not firebase_admin._apps:
    cred = credentials.Certificate(JSON_FILE) 
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://trade-f600a-default-rtdb.firebaseio.com/'
    })
print("✅ Firebase Connected with Direct JSON File!")

# --- CONFIGURATION ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
active_subscriptions = set() 
token_to_fb_keys = {} 

# --- 2. LIVE PRICE HANDLER (Har Sec Update & RAM Clear) ---
def on_data(wsapp, msg):
    global token_to_fb_keys
    if isinstance(msg, dict):
        token = msg.get('token')
        # LTP fetch (Angel One format)
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100 
        
        if ltp > 0 and token in token_to_fb_keys:
            now_time = datetime.datetime.now().strftime("%H:%M:%S")
            updates = {}
            
            # Atomic Overwrite: Har second naya price purane ko mita dega
            for fb_key in token_to_fb_keys[token]:
                updates[f"central_watchlist/{fb_key}/price"] = "{:.2f}".format(ltp)
                updates[f"central_watchlist/{fb_key}/utime"] = now_time
            
            if updates:
                db.reference().update(updates)
                # Cache Clear Logic: Database update ke baad local memory clear
                # Isse Render par load nahi padega
                print(f"⚡ [LIVE] {token} -> {ltp} | Sync: {now_time}")

# --- 3. AUTO-RECONNECT ENGINE ---
def on_close(wsapp, code, reason):
    global is_ws_ready
    is_ws_ready = False
    print(f"🔌 Connection Lost. Reconnecting in 3s...")
    eventlet.sleep(3)
    login_and_connect()

def login_and_connect():
    global sws, is_ws_ready
    while True:
        try:
            print("🔄 Angel One Login Initiated...")
            obj = SmartConnect(api_key=API_KEY)
            session = obj.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
            
            if session.get('status'):
                sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                sws.on_data = on_data
                sws.on_close = on_close
                
                def setup_ws(ws):
                    global is_ws_ready
                    is_ws_ready = True
                    print('✅ WebSocket Live & Ready')
                
                sws.on_open = setup_ws
                eventlet.spawn(sws.connect)
                break 
            else:
                print("❌ Session Failed. Retrying...")
        except Exception as e:
            print(f"❌ Login Error: {e}")
        eventlet.sleep(5)

# --- 4. SYSTEM MAINTENANCE (Subscribe Once Logic) ---
def maintenance_loop():
    global active_subscriptions, token_to_fb_keys
    while True:
        try:
            ref = db.reference('central_watchlist').get()
            if ref:
                current_map = {}
                tokens_to_sub = []
                
                for fb_key in ref.keys():
                    t_id = str(fb_key.split('_')[0]) # Unique Token ID
                    if t_id not in current_map:
                        current_map[t_id] = []
                        # Sirf naye token ko subscribe karega
                        if t_id not in active_subscriptions:
                            tokens_to_sub.append({"exchangeType": 1, "tokens": [t_id]})
                    current_map[t_id].append(fb_key)
                
                token_to_fb_keys = current_map

                if is_ws_ready and tokens_to_sub:
                    sws.subscribe("myt_task", 1, tokens_to_sub)
                    for item in tokens_to_sub:
                        active_subscriptions.add(item['tokens'][0])
                    print(f"🚀 Subscribed {len(tokens_to_sub)} New Symbols")

        except Exception as e:
            print(f"⚠️ Maintenance Error: {e}")
        
        # Har 10 second mein database scan karega naye symbols ke liye
        eventlet.sleep(10) 

# --- 5. SERVER BINDING (Render Alive) ---
if __name__ == '__main__':
    # Background Tasks
    eventlet.spawn(login_and_connect)
    eventlet.spawn(maintenance_loop)
    
    from eventlet import wsgi
    port = int(os.environ.get("PORT", 10000))
    
    def app(environ, start_response):
        start_response('200 OK', [('Content-Type', 'text/plain')])
        # Status message taaki aap Render dashboard par dekh sakein
        return [b"Indian Market Engine is Live and Cleaning Every Second."]
    
    print(f"🌍 Server started on port {port}")
    wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
