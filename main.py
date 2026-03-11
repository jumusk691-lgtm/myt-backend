import eventlet
eventlet.monkey_patch()

import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from flask import Flask
from flask_socketio import SocketIO, join_room

# --- 1. CONFIG ---
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
subscribed_tokens_set = set() 
last_master_update_date = None

# --- 2. MASTER DATA SYNC (8:30 AM Logic) ---
def refresh_supabase_master():
    print(f"🔄 [System] Overwriting Master Data...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            json_data = response.json()
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
                temp_path = tmp.name
            conn = sqlite3.connect(temp_path)
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS symbols")
            cursor.execute('''CREATE TABLE symbols (token TEXT, symbol TEXT, name TEXT, expiry TEXT,
                             strike TEXT, lotsize TEXT, instrumenttype TEXT, exch_seg TEXT, tick_size TEXT)''')
            data = [(str(i.get('token')), i.get('symbol'), i.get('name'), i.get('expiry'), i.get('strike'),
                     i.get('lotsize'), i.get('instrumenttype'), i.get('exch_seg'), i.get('tick_size'))
                    for i in json_data if i.get('token')]
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", data)
            conn.commit()
            conn.close()
            with open(temp_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(path="angel_master.db", file=f.read(),
                                                         file_options={"x-upsert": "true", "content-type": "application/octet-stream"})
            os.remove(temp_path)
            return True
    except Exception as e:
        print(f"❌ Master Error: {e}")
        return False

# --- 3. TICK ENGINE ---
def on_data(wsapp, msg):
    if isinstance(msg, dict) and 'token' in msg:
        token = str(msg.get('token'))
        ltp = float(msg.get('last_traded_price', 0)) / 100
        
        if ltp <= 0: return

        payload = {
            "t": token,
            "p": "{:.2f}".format(ltp),
            "e": str(msg.get('exchange_type', '1'))
        }
        
        # Purana Percentage Change logic
        if 'close' in msg and float(msg['close']) > 0:
            cp = float(msg['close']) / 100
            payload["pc"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

        socketio.emit('live_update', payload, room=token)

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("🟢 Engine Live: All Segments Ready")

# --- 4. CONNECTION MANAGER ---
def run_trading_engine():
    global sws, is_ws_ready, subscribed_tokens_set, last_master_update_date
    while True:
        try:
            now = datetime.datetime.now(IST)
            if now.hour == 8 and 30 <= now.minute <= 59 and last_master_update_date != now.date():
                if refresh_supabase_master(): last_master_update_date = now.date()

            if 8 <= now.hour < 24:
                if not is_ws_ready:
                    smart_api = SmartConnect(api_key=API_KEY)
                    totp = pyotp.TOTP(TOTP_STR).now()
                    session = smart_api.generateSession(CLIENT_CODE, PWD, totp)
                    
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        sws.on_open = on_open
                        sws.on_error = lambda ws, err: print(f"❌ Error: {err}")
                        sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                        sws.connect()
            else:
                if is_ws_ready:
                    sws.close()
                    is_ws_ready = False
                    subscribed_tokens_set.clear()
        except Exception as e:
            print(f"Loop Error: {e}")
        eventlet.sleep(15)

# --- 5. PURANA SMART SUBSCRIBE LOGIC (Mixed with New SocketIO) ---
@socketio.on('subscribe')
def handle_subscribe(json_data):
    """
    APK should send: {"watchlist": [{"token": "123", "symbol": "GOLD", "exch": "MCX"}]}
    """
    global subscribed_tokens_set, sws, is_ws_ready
    
    watchlist = json_data.get('watchlist', [])
    if not watchlist: return

    batches = {1: [], 2: [], 3: [], 4: [], 5: []}

    for item in watchlist:
        token = str(item.get('token'))
        symbol = str(item.get('symbol', '')).upper()
        exch = str(item.get('exch', 'NSE')).upper()
        
        if not token or token == "None": continue

        # User ko room mein join karwao
        join_room(token)

        # Wahi Purana Segment Logic (etype detection)
        if token not in subscribed_tokens_set:
            etype = 1 # NSE Cash default
            if "MCX" in exch: 
                etype = 5
            elif any(x in symbol for x in ["CE", "PE", "FUT"]) or "NFO" in exch:
                # Purana NFO/BFO detection
                etype = 4 if ("SENSEX" in symbol or "BFO" in exch) else 2
            elif "BSE" in exch: 
                etype = 3
            
            batches[etype].append(token)

    # Angel API Batch Subscribe
    if is_ws_ready and sws:
        for etype, tokens in batches.items():
            if tokens:
                for i in range(0, len(tokens), 50):
                    b = tokens[i:i+50]
                    sws.subscribe(f"myt_{etype}", 1, [{"exchangeType": etype, "tokens": b}])
                    for t in b: subscribed_tokens_set.add(t)
                    print(f"📡 Subscribed: {len(b)} tokens in Etype {etype}")

@app.route('/')
def health():
    return f"Live: {is_ws_ready} | Subscribed: {len(subscribed_tokens_set)}", 200

if __name__ == '__main__':
    socketio.start_background_task(run_trading_engine)
    port = int(os.environ.get("PORT", 10000))
    socketio.run(app, host='0.0.0.0', port=port)
