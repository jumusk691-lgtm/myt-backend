import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, requests
import firebase_admin
from firebase_admin import credentials, db
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- 1. FIREBASE SETUP ---
# Ensure "serviceAccountKey.json" is added in Render Secret Files
cred = credentials.Certificate("serviceAccountKey.json") 
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://trade-f600a-default-rtdb.firebaseio.com/'
})

# --- CONFIGURATION ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
active_subscriptions = set() 
last_sent_prices = {}
token_to_fb_keys = {} 

# --- 2. DAILY SYNC LOGIC ---
def check_and_sync_morning():
    while True:
        now = datetime.datetime.now()
        if now.hour == 9 and now.minute == 0:
            print("🚀 9:00 AM: Cleaning central_watchlist for fresh start...")
            time.sleep(65) 
        eventlet.sleep(30)

# --- 3. LIVE PRICE HANDLER ---
def on_data(wsapp, msg):
    global last_sent_prices, token_to_fb_keys
    if isinstance(msg, dict):
        token = msg.get('token')
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp', 0)
        ltp = float(ltp_raw) / 100 
        
        if ltp > 0 and token in token_to_fb_keys:
            if ltp != last_sent_prices.get(token):
                formatted_lp = "{:.2f}".format(ltp)
                for fb_key in token_to_fb_keys[token]:
                    db.reference(f'central_watchlist/{fb_key}').update({
                        "price": formatted_lp,
                        "utime": datetime.datetime.now().strftime("%H:%M:%S")
                    })
                last_sent_prices[token] = ltp

# --- 4. AUTO-RECONNECT ENGINE ---
def login_and_connect():
    global sws, is_ws_ready
    while True:
        try:
            print("🔄 Attempting Angel Login...")
            obj = SmartConnect(api_key=API_KEY)
            session = obj.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
            
            if session.get('status'):
                sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                sws.on_data = on_data
                sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('✅ WebSocket Connected')")
                eventlet.spawn(sws.connect)
                break 
        except Exception as e:
            print(f"❌ Connection Failed: {e}. Retrying in 5s...")
            time.sleep(5)

# --- 5. SYSTEM MAINTENANCE ---
def maintenance_loop():
    global active_subscriptions, token_to_fb_keys, last_sent_prices
    while True:
        try:
            ref = db.reference('central_watchlist').get()
            if ref:
                current_map = {}
                tokens_to_sub = []
                
                for fb_key in ref.keys():
                    t_id = fb_key.split('_')[0]
                    if t_id not in current_map:
                        current_map[t_id] = []
                        if t_id not in active_subscriptions:
                            tokens_to_sub.append({"exchangeType": 1, "tokens": [str(t_id)]})
                    current_map[t_id].append(fb_key)
                
                token_to_fb_keys = current_map

                if is_ws_ready and tokens_to_sub:
                    sws.subscribe("myt_task", 1, tokens_to_sub)
                    for item in tokens_to_sub:
                        active_subscriptions.add(item['tokens'][0])
                    print(f"🚀 New Subscriptions: {len(tokens_to_sub)}")

            if datetime.datetime.now().second < 5: 
                last_sent_prices.clear()
                print("🧹 RAM Cleanup: last_sent_prices cleared.")

        except Exception as e:
            print(f"⚠️ Maintenance Error: {e}")
        
        eventlet.sleep(20) 

# --- 6. RENDER ALIVE ---
def cron_keep_alive():
    while True:
        print(f"⏰ Heartbeat: {datetime.datetime.now()} - System Live")
        eventlet.sleep(900) 

# --- 7. MAIN ENGINE AND SERVER BINDING ---
if __name__ == '__main__':
    login_and_connect()
    
    eventlet.spawn(maintenance_loop)
    eventlet.spawn(check_and_sync_morning)
    eventlet.spawn(cron_keep_alive)
    
    # FIXED: Proper import for Render compatibility
    from eventlet import wsgi
    
    port = int(os.environ.get("PORT", 10000))
    def app(environ, start_response):
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b"Render Live Engine Active"]
    
    print(f"🌍 Server starting on port {port}")
    # Using the imported wsgi module to prevent AttributeError
    wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
