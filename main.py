import eventlet
eventlet.monkey_patch()

import os, pyotp, time, datetime, pytz, requests, sqlite3, tempfile, json
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from flask import Flask, send_file, request, after_this_request
from flask_socketio import SocketIO, join_room

# --- 1. CONFIG ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# --- GLOBAL STATE ---
sws = None
is_ws_ready = False
subscribed_tokens_set = set() 
last_master_update_date = None

# --- 2. MASTER DATA SYNC (Logic: Only Overwrite) ---
def refresh_supabase_master():
    print(f"🔄 [System] Updating Master Data (Overwriting Existing File)...")
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
            
            # x-upsert: true ensures the file is overwritten on Supabase
            with open(temp_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", 
                    file=f.read(),
                    file_options={"x-upsert": "true", "content-type": "application/octet-stream"}
                )
            os.remove(temp_path)
            print("✅ [System] Master Data Overwritten Successfully.")
            return True
    except Exception as e:
        print(f"❌ Master Error: {e}")
        return False

# --- 3. TICK ENGINE ---
def on_data(wsapp, msg):
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            ltp = float(msg.get('last_traded_price', 0)) / 100
            
            if ltp <= 0: return

            payload = {
                "t": token,
                "p": "{:.2f}".format(ltp)
            }
            
            if 'close' in msg and float(msg['close']) > 0:
                cp = float(msg['close']) / 100
                payload["pc"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

            # Room Based Broadcast: Sirf un users ko jayega jo is token room mein hain
            socketio.emit('live_update', payload, room=token)
    except Exception as e:
        print(f"Tick Data Error: {e}")

def on_open(wsapp):
    global is_ws_ready
    is_ws_ready = True
    print("🟢 Engine Live: Connected to Angel WebSocket")

# --- 4. CONNECTION MANAGER ---
def run_trading_engine():
    global sws, is_ws_ready, subscribed_tokens_set, last_master_update_date
    
    # --- HAR DEPLOY PAR UPDATE ---
    refresh_supabase_master()
    
    while True:
        try:
            now = datetime.datetime.now(IST)
            # --- HAR SUBHA UPDATE ---
            if now.hour == 8 and 30 <= now.minute <= 45 and last_master_update_date != now.date():
                if refresh_supabase_master(): last_master_update_date = now.date()

            if 7 <= now.hour < 24:
                if not is_ws_ready:
                    smart_api = SmartConnect(api_key=API_KEY)
                    totp = pyotp.TOTP(TOTP_STR).now()
                    session = smart_api.generateSession(CLIENT_CODE, PWD, totp)
                    
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        sws.on_open = on_open
                        sws.on_error = lambda ws, err: print(f"❌ WebSocket Error: {err}")
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

# --- 5. SMART SEGMENT DETECTION & SUBSCRIBE ---
@socketio.on('subscribe')
def handle_subscribe(json_data):
    global subscribed_tokens_set, sws, is_ws_ready
    
    watchlist = json_data.get('watchlist', [])
    if not watchlist: 
        print("⚠️ No watchlist received")
        return

    batches = {1: [], 2: [], 3: [], 4: [], 5: []}

    for item in watchlist:
        token = str(item.get('token'))
        exch = str(item.get('exch', 'NSE')).upper()
        symbol = str(item.get('symbol', '')).upper()
        
        if not token or token == "None" or token == "": continue

        # [CRITICAL FIX]: Room join har device ke liye hona chahiye
        join_room(token)
        print(f"👤 Device joined room: {token}")

        # Check karo agar token Angel One se subscribe karna hai ya pehle se ho chuka hai
        if token not in subscribed_tokens_set:
            if "MCX" in exch: 
                etype = 5
            elif "NFO" in exch or any(x in symbol for x in ["CE", "PE", "FUT"]):
                etype = 2
            elif "BFO" in exch:
                etype = 4
            elif "BSE" in exch: 
                etype = 3
            else:
                etype = 1
            
            batches[etype].append(token)

    # Angel One Engine ko subscription bhej rahe hain
    if is_ws_ready and sws:
        for etype, tokens in batches.items():
            if tokens:
                for i in range(0, len(tokens), 50):
                    chunk = tokens[i:i+50]
                    sws.subscribe(f"myt_sub_{etype}", 1, [{"exchangeType": etype, "tokens": chunk}])
                    for t in chunk: subscribed_tokens_set.add(t)
                    print(f"📡 Angel One: Subscribed {len(chunk)} tokens to Etype {etype}")
    else:
        print("🟠 API not ready, room joined but subscription queued.")

# --- 6. DATABASE DOWNLOAD (For Android Client) ---
@app.route('/download_db')
def download_db():
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        res = supabase.storage.from_(BUCKET_NAME).download("angel_master.db")
        
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(res)
            tmp_path = tmp.name
        
        @after_this_request
        def cleanup(response):
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return response
            
        return send_file(tmp_path, as_attachment=True, download_name="angel_master.db")
    except Exception as e:
        return f"Error: {e}", 500

# --- 7. HISTORICAL DATA (For 3-Month Candles) ---
@app.route('/history')
def get_history():
    token = request.args.get('token')
    exch = request.args.get('exch', 'NSE').upper()
    interval = request.args.get('interval', 'FIVE_MINUTE') 

    if not token: return "Token required", 400

    try:
        smart_api = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_STR).now()
        smart_api.generateSession(CLIENT_CODE, PWD, totp)

        to_date = datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')
        from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=90)).strftime('%Y-%m-%d %H:%M')

        params = {
            "exchange": exch,
            "symboltoken": token,
            "interval": interval,
            "fromdate": from_date,
            "todate": to_date
        }

        res = smart_api.getCandleData(params)
        
        if res.get('status') and res.get('data'):
            formatted = []
            for c in res['data']:
                t_obj = datetime.datetime.strptime(c[0], "%Y-%m-%dT%H:%M:%S%z")
                formatted.append({
                    "time": int(t_obj.timestamp()),
                    "open": float(c[1]), "high": float(c[2]),
                    "low": float(c[3]), "close": float(c[4])
                })
            return json.dumps(formatted)
        return "No data found", 404
    except Exception as e:
        return str(e), 500

@app.route('/')
def health():
    status = "READY" if is_ws_ready else "OFFLINE"
    return f"Engine: {status} | Tokens: {len(subscribed_tokens_set)}", 200

if __name__ == '__main__':
    socketio.start_background_task(run_trading_engine)
    port = int(os.environ.get("PORT", 10000))
    socketio.run(app, host='0.0.0.0', port=port)
