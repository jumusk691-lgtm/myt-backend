import eventlet
eventlet.monkey_patch(all=True)

import os
import pyotp
import socketio
import redis
import json
from datetime import datetime
from supabase import create_client
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- GLOBAL STATE ---
sws_instance = None
is_ws_ready = False 
r = None
supabase = None
active_subscriptions = set()

def initialize_clients():
    global r, supabase
    # Redis configuration
    redis_url = os.environ.get("REDIS_URL")
    r = redis.from_url(redis_url, decode_responses=True)
    
    # Supabase configuration
    supabase_url = "https://rcosgmsyisybusmuxzei.supabase.co"
    # Ensure your Key is correct in Environment Variables
    supabase_key = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...") 
    supabase = create_client(supabase_url, supabase_key)

initialize_clients()

sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
socketio_app = socketio.WSGIApp(sio)

# --- DATABASE & CLEANUP LOGIC ---
def cleanup_expired_contracts():
    """Purana data remove karna jo expire ho chuka hai"""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        print(f"🧹 Cleaning up expired contracts for date: {today}")
        
        # 1. Supabase से एक्सपायर्ड टोकन हटाना (Table: watchlist_items, Column: expiry)
        # मान लेते हैं आपकी टेबल में expiry कॉलम है
        response = supabase.table("watchlist_items").delete().lt("expiry", today).execute()
        
        # 2. Redis Cleanup: जो टोकन अब एक्टिव नहीं हैं उन्हें Redis से भी हटाना
        # (यह तब काम करेगा जब आप Redis में एक्सपायरी के साथ डेटा स्टोर करें)
        keys = r.keys("price:*")
        # यहाँ आप लॉजिक बढ़ा सकते हैं अगर आपके पास टोकन-एक्सपायरी मैपिंग हो
        
        print(f"✅ Cleanup complete.")
    except Exception as e:
        print(f"❌ Cleanup Error: {e}")

# --- SMART LOGIC FOR EXCHANGE TYPES ---
def get_exchange_type(token):
    try:
        t = int(token)
        # Nifty/BankNifty/FinNifty Options Range
        if 35000 <= t <= 999999: return 2  # NFO
        # MCX Range (Commodity)
        if t >= 1000000: return 5          # MCX
        # NSE Cash (Stocks)
        return 1                           # NSE
    except:
        return 2

# --- CALLBACKS ---
def on_data(wsapp, msg):
    if isinstance(msg, dict):
        token = msg.get('token')
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp')
        
        if ltp_raw is not None and token:
            lp = str(float(ltp_raw) / 100)
            token_str = str(token).strip()
            
            # Redis Update
            r.set(f"price:{token_str}", lp)
            
            # Live Broadcast to Frontend
            sio.emit('livePrice', {"tk": token_str, "lp": lp})

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("✅ Angel WebSocket Connected")

def on_error(wsapp, error):
    global is_ws_ready
    print(f"❌ WS Error: {error}")

def on_close(wsapp):
    global is_ws_ready, active_subscriptions
    is_ws_ready = False
    active_subscriptions.clear()
    print("🔌 Reconnecting...")
    eventlet.sleep(5)
    login_to_angel()

def login_to_angel():
    global sws_instance, is_ws_ready
    try:
        API_KEY = "85HE4VA1"
        CLIENT_ID = "S52638556"
        PIN = "0000" 
        TOTP_KEY = "XFTXZ2445N4V2UMB7EWUCBDRMU"
        
        obj = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_KEY).now()
        session = obj.generateSession(CLIENT_ID, PIN, totp)
        
        if session.get('status'):
            sws_instance = SmartWebSocketV2(
                session['data']['jwtToken'], API_KEY, CLIENT_ID, session['data']['feedToken']
            )
            sws_instance.on_data = on_data
            sws_instance.on_open = on_open
            sws_instance.on_error = on_error
            sws_instance.on_close = on_close
            
            eventlet.spawn(sws_instance.connect)
            print("🚀 Angel Login Successful")
            
            # लॉगिन के बाद पुराना डेटा साफ करें
            cleanup_expired_contracts()
            
    except Exception as e:
        print(f"❌ Login Failed: {e}")

# --- EVENTS ---
@sio.event
def connect(sid, environ):
    print(f"📱 User Connected: {sid}")
    if not is_ws_ready:
        login_to_angel()

@sio.event
def subscribe(sid, data):
    """Upcoming expiry add karne ke liye Frontend se naye tokens bhejein"""
    global sws_instance, is_ws_ready, active_subscriptions
    
    if data and sws_instance and is_ws_ready:
        def do_subscribe():
            tokens_to_sub = data if isinstance(data, list) else [data]
            
            # Filter tokens
            new_tokens = [str(t) for t in tokens_to_sub]
            if not new_tokens: return

            grouped = {}
            for t in new_tokens:
                etype = get_exchange_type(t)
                if etype not in grouped: grouped[etype] = []
                grouped[etype].append(t)

            for etype, tokens in grouped.items():
                correlation_id = f"sub_{sid}_{etype}"
                try:
                    # Mode 1 for LTP updates
                    sws_instance.subscribe(correlation_id, 1, [{"exchangeType": etype, "tokens": tokens}])
                    for t in tokens: active_subscriptions.add(t)
                    print(f"📡 Subscribed: {len(tokens)} tokens (Ex: {etype})")
                except Exception as e:
                    print(f"❌ Sub Error: {e}")
                eventlet.sleep(0.1)

        eventlet.spawn(do_subscribe)

# --- SERVER START ---
def app(environ, start_response):
    if environ.get('PATH_INFO') == '/':
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b"MYT PRO BACKEND ACTIVE - MCX/NFO/STOCKS SUPPORTED"]
    return socketio_app(environ, start_response)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
