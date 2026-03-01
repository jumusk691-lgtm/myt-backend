import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, requests
import firebase_admin
from firebase_admin import credentials, db
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- 1. FIREBASE SETUP (Point 6: Firebase Only) ---
# Paste your service account key path or use environment variables
cred = credentials.Certificate("serviceAccountKey.json") 
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://trade-f600a-default-rtdb.firebaseio.com/'
})

# --- CONFIGURATION ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"

# --- GLOBAL STATE (Point 4 & 7: RAM/Cleanup Management) ---
sws = None
is_ws_ready = False
active_subscriptions = set() # Point 2: Unique Subscription
last_sent_prices = {} # Point 3: 0.1s update check

# --- 2. 9:00 AM TOKEN SYNC (Point 1 & 8) ---
def daily_token_sync():
    print(f"[{datetime.datetime.now()}] 🚀 Running 9:00 AM Sync...")
    # Token master download aur purane tokens delete karne ka logic yahan aayega
    # Firebase 'central_watchlist' ko clean karke naya data inject karein
    db.reference('central_watchlist').delete() 
    print("✅ Purana data deleted. Ready for new tokens.")

# --- 3. LIVE PRICE HANDLER (Point 3 & 9) ---
def on_data(wsapp, msg):
    global last_sent_prices
    if isinstance(msg, dict):
        token = msg.get('token')
        # Point 9: Market price format (/100 if required by Angel API)
        ltp = float(msg.get('last_traded_price', 0))
        
        if ltp > 0 and token:
            # Point 3: 0.1s Speed - Update only if price changed
            if ltp != last_sent_prices.get(token):
                # Update in Firebase 'central_watchlist'
                # Isse market band hone par bhi last price save rahegi
                db.reference(f'central_watchlist/{token}').update({
                    "price": ltp,
                    "last_update": str(datetime.datetime.now())
                })
                last_sent_prices[token] = ltp

# --- 4. AUTO-RECONNECT LOGIC (Point 5) ---
def login_and_connect():
    global sws, is_ws_ready
    while True:
        try:
            print("🔄 Attempting Login & Connection...")
            obj = SmartConnect(api_key=API_KEY)
            session = obj.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
            
            if session.get('status'):
                sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                sws.on_data = on_data
                sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('✅ WebSocket Connected')")
                
                # Start WebSocket in a thread
                eventlet.spawn(sws.connect)
                break 
        except Exception as e:
            print(f"❌ Connection Error: {e}. Retrying in 5s...")
            time.sleep(5)

# --- 5. SMART TUNNEL & CLEANUP (Point 2, 4 & 8) ---
def maintain_system():
    global active_subscriptions, last_sent_prices
    minute_counter = 0
    
    while True:
        try:
            # 1-Minute Resource Cleanup (Point 4)
            minute_counter += 1
            if minute_counter >= 120: # Approx 1 min (0.5s * 120)
                print("🧹 1-Min Cleanup: Flushing temporary logs...")
                last_sent_prices.clear() 
                minute_counter = 0

            # Unique Subscription Logic (Point 2)
            if is_ws_ready:
                ref = db.reference('central_watchlist').get()
                if ref:
                    # Sirf wahi tokens jo pehle se subscribe nahi hain
                    current_tokens = set(ref.keys())
                    new_tokens = current_tokens - active_subscriptions
                    
                    if new_tokens:
                        to_sub = [{"exchangeType": 1, "tokens": [str(t)]} for t in new_tokens]
                        sws.subscribe("bhai_task", 1, to_sub)
                        active_subscriptions.update(new_tokens)
                        print(f"🚀 Subscribed to {len(new_tokens)} new tokens")

        except Exception as e: print(f"⚠️ System Maintenance Error: {e}")
        eventlet.sleep(0.5)

# --- 6. CRON WAKEUP / 15-MIN TASK (Point 8) ---
def cron_wakeup():
    while True:
        # Har 15 minute mein wakeup signal (Point 8)
        print(f"⏰ Cron Heartbeat: {datetime.datetime.now()}")
        eventlet.sleep(900) 

if __name__ == '__main__':
    # Initial Login
    login_and_connect()
    
    # Run tasks
    eventlet.spawn(maintain_system)
    eventlet.spawn(cron_wakeup)
    
    # Run Web Server for Render
    port = int(os.environ.get("PORT", 10000))
    print(f"🌍 Server running on port {port}")
    # Dummy app to keep Render happy
    def app(environ, start_response):
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b"Render Live Engine is Running"]
    
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
