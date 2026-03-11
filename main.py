import eventlet
eventlet.monkey_patch()  # Must be the very first line

import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from flask import Flask, request
from flask_socketio import SocketIO, emit

# --- CONFIG & CREDENTIALS ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRnbHV6c2xqYnhrZG93cWFwamhvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzMwNjI0NDcsImV4cCI6MjA4ODYzODQ0N30.5dvATkqcnVn7FgJcmhcjpJsOANZxrALhKQoFaQTdzHY"
BUCKET_NAME = "Myt"

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
subscribed_tokens_set = set() # Format: "exchange_token"

# --- TICK ENGINE ---
def on_data(wsapp, msg):
    if isinstance(msg, dict) and 'token' in msg:
        # Angel SmartApi V2 provides 'exch_feed_time' or we can infer exchange
        # Send everything back to APK for real-time update
        payload = {
            "t": str(msg.get('token')),
            "p": str(float(msg.get('last_traded_price', 0)) / 100),
            "e": msg.get('exchange_type') # Sending exchange type back to APK
        }
        socketio.emit('live_update', payload)

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("🟢 Engine Connected: All Segments (1,2,3,4,5) Ready")

def on_error(wsapp, error):
    print(f"❌ WS Error: {error}")

# --- CONNECTION MANAGER ---
def run_trading_engine():
    global sws, is_ws_ready
    while True:
        try:
            now = datetime.datetime.now(IST)
            # 8 AM to Midnight (Covers NSE & MCX hours)
            if 8 <= now.hour < 24:
                if not is_ws_ready:
                    print("🔄 Connecting to Angel One...")
                    smart_api = SmartConnect(api_key=API_KEY)
                    totp = pyotp.TOTP(TOTP_STR).now()
                    session = smart_api.generateSession(CLIENT_CODE, PWD, totp)
                    
                    if session.get('status'):
                        sws = SmartWebSocketV2(
                            session['data']['jwtToken'], 
                            API_KEY, CLIENT_CODE, 
                            session['data']['feedToken']
                        )
                        sws.on_data = on_data
                        sws.on_open = on_open
                        sws.on_error = on_error
                        sws.connect() 
            else:
                if is_ws_ready and sws:
                    sws.close()
                    is_ws_ready = False
                    subscribed_tokens_set.clear()
        except Exception as e:
            print(f"Loop Error: {e}")
        eventlet.sleep(15)

# --- MULTI-SEGMENT SUBSCRIBE LOGIC ---
@socketio.on('subscribe')
def handle_subscribe(json_data):
    """
    Expects JSON: {"tokens": ["token1", "token2"], "exchange": 1}
    Exchanges: 1=NSE, 2=BSE, 3=NFO, 4=MCX, 5=BFO
    """
    global subscribed_tokens_set, sws, is_ws_ready
    
    token_list = json_data.get('tokens', [])
    exchange = int(json_data.get('exchange', 1)) # Default to NSE (1)
    
    if is_ws_ready and sws:
        # Filter only tokens not already subscribed for this specific exchange
        new_tokens = [t for t in token_list if f"{exchange}_{t}" not in subscribed_tokens_set]
        
        if new_tokens:
            correlation_id = f"sub_{exchange}_{int(time.time())}"
            payload = [{"exchangeType": exchange, "tokens": new_tokens}]
            
            # Action 1 = Subscribe
            sws.subscribe(correlation_id, 1, payload)
            
            for t in new_tokens:
                subscribed_tokens_set.add(f"{exchange}_{t}")
                
            print(f"📡 Subscribed {len(new_tokens)} symbols for Exchange: {exchange}")
    else:
        print("⚠️ WebSocket Not Ready. Subscription Queued or Ignored.")

@app.route('/')
def health():
    return f"Live: {is_ws_ready}", 200

if __name__ == '__main__':
    socketio.start_background_task(run_trading_engine)
    port = int(os.environ.get("PORT", 10000))
    socketio.run(app, host='0.0.0.0', port=port)
