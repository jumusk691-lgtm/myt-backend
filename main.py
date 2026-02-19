import eventlet
eventlet.monkey_patch(all=True)

import os
import pyotp
import socketio
import redis
from datetime import datetime
from supabase import create_client
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# --- CONFIGURATION ---
REDIS_URL = os.environ.get("REDIS_URL", "redis://default:AR6NAAImcDE4YTZjMDM1ZTkzMDc0ZmJiOTM5YzhjZGI2OWY3MDA5ZXAxNzgyMQ@happy-moth-7821.upstash.io:6379")
SUPABASE_URL = "https://rcosgmsyisybusmuxzei.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "XzoayMr3B5UOVjKGNjjUxU07ZOCFnjPSfe7xO2Gt1OmORLEemCvILjG5O4damYqTT3quUDGmMvgcC+i5FEhthQ==")

# --- GLOBAL STATE ---
sws_instance = None
is_ws_ready = False 
r = None
supabase = None

def initialize_clients():
    global r, supabase
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5)
        print("✅ Redis Connected")
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Supabase Connected")
    except Exception as e:
        print(f"❌ Init Error: {e}")

initialize_clients()

sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
socketio_app = socketio.WSGIApp(sio)

# --- CALLBACKS ---
def on_data(wsapp, msg):
    if isinstance(msg, dict):
        ltp_raw = msg.get('last_traded_price') or msg.get('ltp')
        token = msg.get('token')
        
        if ltp_raw is not None and token:
            try:
                # Angel One sends price in paisa, convert to Rs.
                lp = "{:.2f}".format(float(ltp_raw) / 100)
                token_str = str(token).strip()
                
                # Update Redis and Broadcast
                r.set(f"price:{token_str}", lp)
                sio.emit('livePrice', {"tk": token_str, "lp": lp})
                # print(f"DEBUG: {token_str} -> {lp}") # Un-comment to see prices in Render logs
            except:
                pass 

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("✅ Angel WebSocket Active")

def on_error(wsapp, error):
    print(f"❌ WS Error: {error}")

def on_close(wsapp, status=None, msg=None):
    global is_ws_ready
    is_ws_ready = False
    print("🔌 Connection Closed. Reconnecting...")
    eventlet.sleep(5)
    login_to_angel()

def login_to_angel():
    global sws_instance
    try:
        # Credentials (Inhe Environment Variables mein daalna safe hota hai)
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
    except Exception as e:
        print(f"❌ Login Failed: {e}")

@sio.event
def subscribe(sid, data):
    """
    Expected 'data' format from Frontend: 
    [{"ex": 1, "token": "26000"}, {"ex": 2, "token": "35012"}]
    """
    if data and sws_instance and is_ws_ready:
        try:
            # Agar frontend sirf list of tokens bhej raha hai
            if isinstance(data, list) and isinstance(data[0], str):
                # Default logic: Token length 5+ usually means NFO/MCX
                formatted_tokens = []
                for t in data:
                    ex_type = 2 if len(t) > 5 else 1
                    formatted_tokens.append({"exchangeType": ex_type, "tokens": [t]})
                
                for item in formatted_tokens:
                    sws_instance.subscribe(f"sub_{sid}", 1, [item])
            else:
                # Agar frontend sahi format (exchange + token) bhej raha hai
                sws_instance.subscribe(f"sub_{sid}", 1, data)
        except Exception as e:
            print(f"Subscribe Error: {e}")

def app(environ, start_response):
    if environ.get('PATH_INFO') == '/':
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b"BACKEND IS LIVE"]
    return socketio_app(environ, start_response)

if __name__ == '__main__':
    login_to_angel()
    port = int(os.environ.get("PORT", 10000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
