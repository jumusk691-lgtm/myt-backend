import eventlet
eventlet.monkey_patch(all=True)
import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from flask import Flask, request
from flask_socketio import SocketIO, emit

# --- 1. CONFIG & CREDENTIALS ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

# Supabase (Sirf Master File Upload ke liye)
SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRnbHV6c2xqYnhrZG93cWFwamhvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzMwNjI0NDcsImV4cCI6MjA4ODYzODQ0N30.5dvATkqcnVn7FgJcmhcjpJsOANZxrALhKQoFaQTdzHY"
BUCKET_NAME = "Myt"

# Flask & SocketIO Setup for Render
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
subscribed_tokens_set = set() # Unique subscription track karne ke liye
active_clients = set() # Live connections handle karne ke liye
last_master_update_date = None

# --- 2. MASTER DATA SYNC (8:30 AM OVERWRITE) ---
def refresh_supabase_master():
    print(f"🔄 [System] Fresh Master DB Overwrite...")
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
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", 
                    file=f.read(), 
                    file_options={"x-upsert": "true", "content-type": "application/octet-stream"}
                )
            os.remove(temp_path)
            print("✅ [Success] Master File Updated in Supabase Storage.")
            return True
    except Exception as e:
        print(f"❌ Master Update Error: {e}")
        return False

# --- 3. TICK ENGINE (DIRECT BROADCAST) ---
def on_data(wsapp, msg):
    if isinstance(msg, dict) and 'token' in msg:
        token = str(msg.get('token'))
        ltp = float(msg.get('last_traded_price', 0)) / 100
        
        # LIVE BROADCAST TO APK
        # Ye logic ab direct SocketIO ke zariye APK ko data bhejega
        payload = {
            "t": token,
            "p": str(ltp)
        }
        socketio.emit('live_update', payload)
        # print(f"Ticker: {token} -> {ltp}")

# --- 4. CONNECTION MANAGER ---
def manage_connection():
    global sws, is_ws_ready, subscribed_tokens_set, last_master_update_date
    while True:
        now = datetime.datetime.now(IST)
        
        # Morning Update (8:30 AM)
        if now.hour == 8 and 30 <= now.minute <= 45 and last_master_update_date != now.date():
            if refresh_supabase_master(): 
                last_master_update_date = now.date()

        # Market Connection (Open hours)
        if 8 <= now.hour < 24:
            if not is_ws_ready:
                try:
                    smart_api = SmartConnect(api_key=API_KEY)
                    totp = pyotp.TOTP(TOTP_STR).now()
                    session = smart_api.generateSession(CLIENT_CODE, PWD, totp)
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        sws.on_open = lambda ws: exec("global is_ws_ready; is_ws_ready=True; print('🟢 API WebSocket Live')")
                        sws.on_close = lambda ws,c,r: exec("global is_ws_ready; is_ws_ready=False")
                        eventlet.spawn(sws.connect)
                except Exception as e:
                    print(f"WS Connect Error: {e}")
        else:
            if is_ws_ready:
                if sws: sws.close()
                is_ws_ready = False
                subscribed_tokens_set.clear()
        eventlet.sleep(60)

# --- 5. DIRECT SYNC (APK TO RENDER) ---
@socketio.on('subscribe')
def handle_subscribe(json_data):
    global subscribed_tokens_set, sws, is_ws_ready
    token_list = json_data.get('tokens', [])
    etype = 1 # NSE Default
    
    if is_ws_ready and sws:
        unique_tokens = [t for t in token_list if t not in subscribed_tokens_set]
        if unique_tokens:
            for i in range(0, len(unique_tokens), 50):
                batch = unique_tokens[i:i+50]
                sws.subscribe("myt", 1, [{"exchangeType": etype, "tokens": batch}])
                for t in batch: subscribed_tokens_set.add(t)
                print(f"📡 Direct Subscribed: {len(batch)} tokens via SocketIO")

@app.route('/')
def health_check():
    return "DIRECT ENGINE LIVE", 200

if __name__ == '__main__':
    # Start background connection manager
    eventlet.spawn(manage_connection)
    
    # Start SocketIO Server on Render Port
    port = int(os.environ.get("PORT", 10000))
    socketio.run(app, host='0.0.0.0', port=port)
