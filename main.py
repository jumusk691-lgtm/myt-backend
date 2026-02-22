import eventlet
eventlet.monkey_patch(all=True)

import os
import pyotp
import socketio
from datetime import datetime
from supabase import create_client
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- CONFIGURATION ---
# Bhai, humne Redis hata diya hai, ab sirf Supabase chalega
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJjb3NnbXN5aXN5YnVzbXV4emVpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDgzOTEzNCwiZXhwIjoyMDg2NDE1MTM0fQ.5BofQbMKiMLGFjqcIGaCwpoO9pLZnuLg7nojP0aGhJw")

# --- GLOBAL STATE ---
sws_instance = None
is_ws_ready = False 
supabase = None
subscribed_tokens = set()  # Master list: Ek token ek hi baar subscribe hoga

def initialize_clients():
    global supabase
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Supabase Connected - Ready for 1 Lakh Users!")
    except Exception as e:
        print(f"❌ Init Error: {e}")

initialize_clients()

sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
socketio_app = socketio.WSGIApp(sio)

# --- CALLBACKS (Price Update Logic) ---
def on_data(wsapp, msg):
    if isinstance(msg, dict):
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp')
        token = msg.get('token')
        
        if ltp_raw is not None and token:
            try:
                # 1. Price Conversion
                lp = "{:.2f}".format(float(ltp_raw) / 100)
                token_str = str(token).strip()
                
                # 2. LOGIC: Seedha Supabase update karo
                # Isse 1 lakh users ko Real-time price dikhega
                supabase.table("symbols").update({"price": lp}).eq("token", token_str).execute()
                
                # 3. Optional: Socket emit for instant frontend update
                sio.emit('livePrice', {"tk": token_str, "lp": lp})
                
            except Exception as e:
                # Bhai agar table name alag hai toh yahan check kar lena
                pass 

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("✅ Angel WebSocket Active - Pipe is Open")
    # Agar server restart hua toh purane symbols re-subscribe karna
    if subscribed_tokens:
        resub_list = list(subscribed_tokens)
        smart_subscribe(resub_list)

def on_error(wsapp, error):
    print(f"❌ WS Error: {error}")

def on_close(wsapp, status=None, msg=None):
    global is_ws_ready
    is_ws_ready = False
    print("🔌 Connection Closed. Reconnecting and Refreshing Token...")
    eventlet.sleep(5)
    login_to_angel()

# --- TOKEN REFRESH & LOGIN ---
def login_to_angel():
    global sws_instance
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
            print("🚀 Angel Login Successful - New Session Active")
    except Exception as e:
        print(f"❌ Login Failed: {e}")

# --- MASTER SUBSCRIPTION LOGIC ---
def smart_subscribe(tokens):
    """
    Bhai, ye logic ensure karta hai ki subscription sirf ek baar ho
    par price hamesha update hota rahe.
    """
    global subscribed_tokens, sws_instance, is_ws_ready
    
    # Sirf wo tokens nikaalo jo pehle se subscribe nahi hain
    new_tokens = [t for t in tokens if t not in subscribed_tokens]
    
    if new_tokens and sws_instance and is_ws_ready:
        try:
            formatted_list = []
            for t in new_tokens:
                ex_type = 2 if len(t) > 5 else 1 # NFO vs NSE simple logic
                formatted_list.append({"exchangeType": ex_type, "tokens": [t]})
                subscribed_tokens.add(t) # Master list mein add kar diya
            
            # Ek hi baar mein batch subscribe
            sws_instance.subscribe("bhai_master_sub", 1, formatted_list)
            print(f"🔥 Added {len(new_tokens)} new symbols to Live Stream.")
        except Exception as e:
            print(f"Subscribe Error: {e}")

@sio.event
def watch_list(sid, data):
    # APK se jab tokens aayenge: ['26000', '3045']
    if isinstance(data, list):
        smart_subscribe(data)

# --- SCORE & USER LOGIC ---
@sio.event
def add_score(sid, data):
    """
    Logic: User ka score update karna bina purana data delete kiye.
    """
    mobile = data.get('mobile')
    points = data.get('points')
    
    user_res = supabase.table("users").select("score").eq("mobile", mobile).single().execute()
    new_score = user_res.data['score'] + points
    
    supabase.table("users").update({"score": new_score}).eq("mobile", mobile).execute()
    sio.emit('score_updated', {"score": new_score}, room=sid)

def app(environ, start_response):
    if environ.get('PATH_INFO') == '/':
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b"BACKEND IS LIVE - SUPABASE MODE"]
    return socketio_app(environ, start_response)

if __name__ == '__main__':
    login_to_angel()
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
