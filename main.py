# File Name: main.py
import time
import json
import struct
import pyotp
import threading
import os
import requests
from datetime import datetime
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ⚡ CLOUD UPGRADE: Flask and Socket.IO implementations to throw live data directly over HTTP/WS URL
from flask import Flask
import socketio
import eventlet

# ==========================================================
# 🛑 MANUAL OVERRIDE CONFIGURATION
# ==========================================================
MANUAL_JWT_TOKEN = ""  # <-- Emergency me yahan token paste karein.

# ==========================================================
# 🔥 24/7 UNLIMITED ALIVE TRICK (MP3 PLAYER LOGIC)
# ==========================================================
def keep_alive_audio():
    os.environ['SDL_VIDEODRIVER'] = 'dummy'
    try:
        import pygame
        pygame.mixer.init()
        
        if os.path.exists("silent.mp3"):
            pygame.mixer.music.load("silent.mp3")
            print("🎵 [Player Engine]: Audio Loop Started Successfully (24/7 Mode Active).")
            while True:
                if not pygame.mixer.music.get_busy():
                    pygame.mixer.music.play()
                time.sleep(1)
        else:
            print("⚠️ [Player Warning]: silent.mp3 not found in this folder. Please generate it once.")
    except Exception as e:
        print(f"⚠️ Audio Player Error (Install pygame if needed): {e}")

# Background me music player thread ko sabse pehle chalu karein
threading.Thread(target=keep_alive_audio, daemon=True).start()


# ==========================================================
# 1. CONFIGURATION (GLOBAL CORE CLOUD DATA MATRIX)
# ==========================================================
API_KEY = "Z80WG5Sg"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"

# 🌐 Flask & Socket.IO initialization for universal cloud deployment hosting rules
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
flask_app = Flask(__name__)
wsgi_app = socketio.WSGIApp(sio, flask_app)

# Track active global client sessions directly inside server ram matrix
connected_clients = set()

# ==========================================================
# 2. MASTER ENGINE CLASS (INDIAN MARKET ONLY - CLOUD STREAM)
# ==========================================================
class MunhTitanEngine:
    def __init__(self):
        self.obj = SmartConnect(api_key=API_KEY)
        self.sws = None
        self.is_running = True
        self.jwt_token = MANUAL_JWT_TOKEN if MANUAL_JWT_TOKEN != "" else None
        self.feed_token = None
        self.refresh_token = None 
        self.is_ws_connected = False 
        
        self.token_exchange_cache = {} 
        self.last_known_prices = {} 

        self.session_lock = threading.Lock()
        self.is_refreshing_session = False
        self.last_login_time = 0
        self.last_daily_reset_date = "" # Track daily session wipe date

        self.exchange_map = {
            "NSE": 1,
            "NFO": 2,
            "BSE": 3,
            "BFO": 4, 
            "MCX": 5, 
        }

    def login_manager(self):
        with self.session_lock:
            if MANUAL_JWT_TOKEN != "":
                print("🎟️ [Manual Mode]: Using hardcoded manual JWT Token. Skipping API auto-login sequence.")
                self.jwt_token = MANUAL_JWT_TOKEN
                self.obj.setAccessToken(self.jwt_token)
                return True

            current_now = time.time()
            if current_now - self.last_login_time < 10:
                print("⏳ [Rate Limiter]: Fast login skip. Holding server call protection line.")
                return True

            try:
                print("🔑 [System]: Hard Re-allocating New SmartConnect Instance Session...")
                self.obj = SmartConnect(api_key=API_KEY)
                
                totp = pyotp.TOTP(TOTP_STR).now()
                data = self.obj.generateSession(CLIENT_CODE, MPIN, totp)
                self.last_login_time = time.time()
                
                if data['status']:
                    print("✅ [System]: Pure Authentic Session Active! Clean Tokens Injected.")
                    self.jwt_token = data['data']['jwtToken']
                    self.feed_token = self.obj.feed_token
                    self.refresh_token = data['data']['refreshToken'] 
                    
                    self.obj.setAccessToken(self.jwt_token)
                    
                    custom_headers = {
                        'Authorization': f"Bearer {self.jwt_token}",
                        'X-PrivateKey': API_KEY,
                        'X-ClientLocalIP': "127.0.0.1",
                        'X-ClientPublicIP': "106.193.147.98",
                        'X-MACAddress': "c3:7b:dc:a6:e9:d1",
                        'Accept': "application/json",
                        'Content-type': "application/json",
                        'X-UserType': "USER",
                        'X-SourceID': "WEB"
                    }
                    if hasattr(self.obj, 'setHeaders'):
                        self.obj.setHeaders(custom_headers)
                    
                    time.sleep(2.5)
                    return True
                else:
                    print(f"❌ [Error]: {data['message']}. Activating cool-down delay.")
                    time.sleep(5)
                    return False
            except Exception as e:
                print(f"💀 [Crash]: Login manager critical failure: {e}")
                time.sleep(5)
                return False

    def session_watchdog_loop(self):
        print("🛡️ [Watchdog Guard]: Session Monitoring Guard Activated (24/7 Shield Loaded).")
        while self.is_running:
            time.sleep(30) # Reduced sleep timer to 30s for ultra responsive auto-recovery
            if not self.is_running: break
            if MANUAL_JWT_TOKEN != "": continue

            try:
                # ⏰ --- FULL AUTOMATIC MORNING MANDATORY RE-LOGIN SHIELD ---
                now_dt = datetime.now()
                current_date_str = now_dt.strftime("%Y-%m-%d")
                
                # Agar subah ke 08:15 baje hain ya usse upar aur aaj ke din ka hard reset nahi hua hai
                if now_dt.hour == 8 and now_dt.minute >= 15 and self.last_daily_reset_date != current_date_str:
                    print(f"🌅 [Morning Reset]: Triggering mandatory daily fresh authentication login loop at {now_dt.strftime('%H:%M:%S')}...")
                    self.is_refreshing_session = True
                    if self.login_manager():
                        self.last_daily_reset_date = current_date_str
                        print("✅ [Morning Reset]: Fresh daily token matrix synced successfully.")
                        if self.sws:
                            print("🔄 [Morning Reset]: Closing old websocket connection handler...")
                            try: self.sws.close()
                            except Exception: pass
                    self.is_refreshing_session = False
                    continue

                # Normal token survival watchdog check
                if self.obj and self.jwt_token and not self.is_refreshing_session:
                    test_profile = self.obj.getProfile(self.refresh_token)
                    if not test_profile or not test_profile.get("status"):
                        print("⚠️ [Watchdog Guard]: Session decay detected! Triggering auto override matrix...")
                        self.is_refreshing_session = True
                        if self.login_manager():
                            print("✅ [Watchdog Guard]: Session restored by background shield successfully.")
                            if self.sws and self.is_ws_connected:
                                threading.Thread(target=self.trigger_smart_resubscription, daemon=True).start()
                        self.is_refreshing_session = False
            except Exception as watchdog_err:
                print(f"❌ [Watchdog Exception]: Core structural network tracking error: {watchdog_err}")
                self.is_refreshing_session = False

    def send_delayed_price(self, token, price_str):
        # ⚡ MQTT Removed -> Now using universal Socket.IO emit to broadcast high-speed binary packs
        try:
            token_int = int(token)
            price_int = int(round(float(price_str) * 100))
            binary_packet = struct.pack('>ii', token_int, price_int)
            sio.emit('live_data', binary_packet)
        except Exception:
            pass

    def trigger_smart_resubscription(self):
        try:
            if not self.token_exchange_cache:
                return

            print(f"🔄 [Resubscribe Auto-Recovery]: Re-registering {len(self.token_exchange_cache)} tokens directly into WebSocket tunnel...")
            grouped_subs = {}
            
            for token, exch_name in self.token_exchange_cache.items():
                exch_type = self.exchange_map.get(exch_name, 1)
                if exch_type not in grouped_subs:
                    grouped_subs[exch_type] = []
                grouped_subs[exch_type].append(token)
                
            if grouped_subs and self.sws and self.is_ws_connected:
                sub_params = []
                for exch_type, tokens_list in grouped_subs.items():
                    sub_params.append({
                        "exchangeType": exch_type,
                        "tokens": tokens_list
                    })
                self.sws.subscribe("munh_batch", 1, sub_params)
                print("✅ [Resubscribe]: Stateless recovery completed. Streaming active.")
                
                for t, p in self.last_known_prices.items():
                    self.send_delayed_price(t, p)
        except Exception as resub_err:
            print(f"❌ [Resubscribe Error]: Failed to execute auto-restore matrix: {resub_err}")

    # ⚡ Smart logic to bridge Android Socket.IO incoming layout commands over Python architecture
    def process_incoming_ui_payload(self, raw_payload_str):
        try:
            payload = json.loads(raw_payload_str)
            action = payload.get("action", "sub") 
            grouped_subs = {}
            grouped_unsubs = {}

            if action == "sync_watchlist":
                print("🔄 [Watchlist Sync Event]: Syncing full layout packet directly from UI...")
                batch_list = payload.get("batch", [])
                self.token_exchange_cache.clear()
                
                for item in batch_list:
                    exch_name = item.get('exch') or item.get('exch_seg')
                    token = str(item.get('token'))
                    symbol_name = item.get('symbol', item.get('name', '')).upper()
                    
                    if exch_name and token and token != "None":
                        if "SENSEX" in symbol_name or "BANKEX" in symbol_name or (token.isdigit() and int(token) >= 1100000 and exch_name == "NFO"):
                            exch_name = "BFO" if token != "1" else "BSE"
                            
                        exch_type = self.exchange_map.get(exch_name, 1)
                        self.token_exchange_cache[token] = exch_name
                        if exch_type not in grouped_subs:
                            grouped_subs[exch_type] = []
                        grouped_subs[exch_type].append(token)
                        
                        cached_price = self.last_known_prices.get(token, "0.00")
                        if cached_price not in ["0.0", "0.00", "0"]:
                            threading.Timer(0.1, self.send_delayed_price, args=[token, cached_price]).start()

            elif "batch" in payload:
                batch_list = payload.get("batch", [])
                for item in batch_list:
                    exch_name = item.get('exch') or item.get('exch_seg')
                    token = str(item.get('token'))
                    symbol_name = item.get('symbol', item.get('name', '')).upper()
                    
                    if exch_name and token and token != "None":
                        if "SENSEX" in symbol_name or "BANKEX" in symbol_name or (token.isdigit() and int(token) >= 1100000 and exch_name == "NFO"):
                            exch_name = "BFO" if token != "1" else "BSE"

                        exch_type = self.exchange_map.get(exch_name, 1)

                        if action == "unsub":
                            if exch_type not in grouped_unsubs:
                                grouped_unsubs[exch_type] = []
                            grouped_unsubs[exch_type].append(token)
                            if token in self.token_exchange_cache:
                                del self.token_exchange_cache[token]
                        else: 
                            self.token_exchange_cache[token] = exch_name
                            if exch_type not in grouped_subs:
                                grouped_subs[exch_type] = []
                            grouped_subs[exch_type].append(token)
                            
                            cached_price = self.last_known_prices.get(token, "0.00")
                            if cached_price not in ["0.0", "0.00", "0"]:
                                threading.Timer(0.1, self.send_delayed_price, args=[token, cached_price]).start()

            else:
                exch_name = payload.get('exch') or payload.get('exch_seg')
                token = str(payload.get('token'))
                symbol_name = payload.get('symbol', payload.get('name', '')).upper()
                
                if exch_name and token and token != "None":
                    if "SENSEX" in symbol_name or "BANKEX" in symbol_name or (token.isdigit() and int(token) >= 1100000 and exch_name == "NFO"):
                        exch_name = "BFO" if token != "1" else "BSE"
                        
                    exch_type = self.exchange_map.get(exch_name, 1)
                    
                    if action == "unsub":
                        if exch_type not in grouped_unsubs:
                            grouped_unsubs[exch_type] = []
                        grouped_unsubs[exch_type].append(token)
                        if token in self.token_exchange_cache:
                            del self.token_exchange_cache[token]
                    else:
                        self.token_exchange_cache[token] = exch_name
                        if exch_type not in grouped_subs:
                            grouped_subs[exch_type] = []
                        grouped_subs[exch_type].append(token)
                        
                        cached_price = self.last_known_prices.get(token, "0.00")
                        if cached_price not in ["0.0", "0.00", "0"]:
                            threading.Timer(0.1, self.send_delayed_price, args=[token, cached_price]).start()

            if grouped_unsubs and self.sws and self.is_ws_connected:
                unsub_params = []
                for exch_type, tokens_list in grouped_unsubs.items():
                    unsub_params.append({
                        "exchangeType": exch_type,
                        "tokens": tokens_list
                    })
                self.sws.unsubscribe("munh_batch", 1, unsub_params)

            if grouped_subs:
                sub_params = []
                for exch_type, tokens_list in grouped_subs.items():
                    sub_params.append({
                        "exchangeType": exch_type,
                        "tokens": tokens_list
                    })
                if self.sws and self.is_ws_connected:
                    self.sws.subscribe("munh_batch", 1, sub_params)

        except Exception as e:
            print(f"❌ [Socket Engine Data Parse Error]: Failed to map layout payload context: {e}")

    def on_data(self, wsapp, msg):
        token = msg.get('token') or msg.get('tk')
        lp = msg.get('last_traded_price') or msg.get('lp')
        
        if token and lp:
            raw_price = float(lp)
            token_str = str(token)
            
            if token_str == "1" or token_str == "99919012" or token_str == "99926000":
                price = raw_price / 100.0 if raw_price > 5000000 else raw_price
            elif raw_price > 5000.0 and not (token_str.startswith('4') or token_str.startswith('5')): 
                price = raw_price / 100.0
            elif raw_price >= 100000.0:
                price = raw_price / 100.0
            else:
                price = raw_price / 100.0 if isinstance(lp, int) else raw_price

            formatted_price = "{:.2f}".format(price)
            self.last_known_prices[token_str] = formatted_price

            try:
                token_int = int(token_str)
                price_int = int(round(price * 100))
                binary_packet = struct.pack('>ii', token_int, price_int)
                # ⚡ MQTT Replacement: Emit binary zero-copy stream packet directly across server channels
                sio.emit('live_data', binary_packet)
            except Exception:
                pass

    def on_close(self, wsapp, close_status_code, close_msg):
        print(f"⚠️ [Socket]: Connection Closed ({close_status_code}): {close_msg}")
        self.is_ws_connected = False
        if self.is_running:
            threading.Timer(5.0, self.start_websocket).start()

    def on_error(self, wsapp, error):
        print(f"❌ [Socket Error]: WebSocket encountered error: {error}")
        self.is_ws_connected = False

    def start_websocket(self):
        if not self.is_running: return
        
        while self.is_refreshing_session:
            time.sleep(0.5)
            
        print("🔄 [Socket]: Initializing SmartWebSocketV2 Stream Layer...")
        
        def on_open(wsapp):
            print("🌐 [Socket]: WebSocket Connection Open! Recovering state...")
            self.is_ws_connected = True
            
            init_subs = [
                {"exchangeType": 1, "tokens": ["99926000"]},
                {"exchangeType": 3, "tokens": ["1", "99919012"]}
            ]
            try:
                self.sws.subscribe("munh_batch", 1, init_subs)
            except Exception:
                pass
                
            self.trigger_smart_resubscription()

        resolved_feed = self.feed_token if self.feed_token else "manual_feed_bypassed"
        self.sws = SmartWebSocketV2(self.jwt_token, API_KEY, CLIENT_CODE, resolved_feed)
        self.sws.on_data = self.on_data
        self.sws.on_open = on_open
        self.sws.on_close = self.on_close
        self.sws.on_error = self.on_error
        threading.Thread(target=self.sws.connect, daemon=True).start()

    def start_engine(self):
        while not self.login_manager():
            print("⏳ [System]: Retrying master initialization login in 10s...")
            time.sleep(10)
            
        threading.Thread(target=self.session_watchdog_loop, daemon=True).start()
        self.start_websocket()

# Instantiate global core engine context
engine = MunhTitanEngine()

# ==========================================================
# ⚡ GLOBAL SOCKET.IO DEPLOYMENT WIRE EVENTS LOGIC
# ==========================================================
@sio.event
def connect(sid, environ):
    print(f"📱 [Cloud Client Connected]: Security ID -> {sid}")
    connected_clients.add(sid)
    
    # Immediately push cached live prices of indexes to newly joined clients to prevent blank screens
    for token in ["99926000", "99919012"]:
        if token in engine.last_known_prices:
            engine.send_delayed_price(token, engine.last_known_prices[token])

@sio.event
def subscribe_request(sid, data):
    # Triggers anytime Android app requests a subscription/sync burst
    # Translates incoming communication safely into SmartAPI commands
    if data:
        engine.process_incoming_ui_payload(data)

@sio.event
def disconnect(sid):
    print(f"📱 [Cloud Client Disconnected]: Security ID -> {sid}")
    if sid in connected_clients:
        connected_clients.remove(sid)

# HTTP Health check endpoint for global platforms (Render/Railway/Render etc.) to detect life signals
@flask_app.route('/')
def health_check():
    return {"status": "alive", "engine_connected": engine.is_ws_connected}, 200

if __name__ == "__main__":
    # Start the core trading engine logic in a background secure thread layer
    threading.Thread(target=engine.start_engine, daemon=True).start()
    
    # Capture platform dynamic variables ports or default to standard fallback 5000 line
    port = int(os.environ.get('PORT', 5000))
    print(f"🚀 [Munh Titan Web Server Engine]: Booted. Serving binary pipes over port {port}...")
    
    # Run absolute production optimized WSGI web server wrapper block
    eventlet.wsgi.server(eventlet.listen(('', port)), wsgi_app)
