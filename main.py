import eventlet
eventlet.monkey_patch()  # 🔥 CRITICAL FIX: Sockets, Threads, aur Requests ko Eventlet compatible banane ke liye sabse upar anivarya hai!

import time
import json
import struct
import pyotp
import threading
import os
import requests
import re
from datetime import datetime
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ⚡ Firebase Realtime Database URL
FIREBASE_DB_URL = "https://trade-f600a-default-rtdb.firebaseio.com/"

# ==========================================================
# 🛑 SCORE TRACKER LOGIC
# ==========================================================
user_score = 0
score_lock = threading.Lock()

def add_to_score(points):
    global user_score
    with score_lock:
        user_score += points
    return user_score

# ==========================================================
# ⚡ Web Server Matrix Engine Integrations
# ==========================================================
from flask import Flask, redirect, url_for, request, jsonify
import socketio

# ==========================================================
# 🛑 MANUAL OVERRIDE CONFIGURATION
# ==========================================================
MANUAL_JWT_TOKEN = "" 

# ==========================================================
# 🔥 24/7 UNLIMITED ALIVE TRICK (MP3 MIXER ENGINE)
# ==========================================================
def keep_alive_audio():
    os.environ['SDL_VIDEODRIVER'] = 'dummy'
    try:
        import pygame
        pygame.mixer.init()
        if os.path.exists("silent.mp3"):
            pygame.mixer.music.load("silent.mp3")
            print("🎵 [Player Engine]: Audio Process Continuous Loop Triggered.")
            while True:
                if not pygame.mixer.music.get_busy(): 
                    pygame.mixer.music.play()
                time.sleep(1)
    except Exception as e:
        pass

threading.Thread(target=keep_alive_audio, daemon=True).start()

# ==========================================================
# 🛑 EXCHANGE CODES VARIABLES MATRIX
# ==========================================================
nse = 1
nfo = 2
bse = 3
bfo = 4
mcx = 5

# ==========================================================
# 1. CORE PARAMETERS CONFIGURATION
# ==========================================================
API_KEY = "Z80WG5Sg"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
APP_THEME_COLORS = ["black", "blue"] 

sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
flask_app = Flask(__name__)
wsgi_app = socketio.WSGIApp(sio, flask_app)
connected_clients = set()

# ==========================================================
# 2. TITAN ENGINE SYSTEM LOGIC
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
        self.active_subscribed_tokens = set() 
        
        self.session_lock = threading.Lock()
        self.is_refreshing_session = False
        self.last_login_time = 0
        self.last_daily_reset_date = "" 
        
        self.exchange_map = {"NSE": nse, "NFO": nfo, "BSE": bse, "BFO": bfo, "MCX": mcx}
        
        self.REQ_NIFTY_TOKEN = "99926000"
        self.REQ_SENSEX_TOKEN = "99919012"
        self.token_exchange_cache[self.REQ_NIFTY_TOKEN] = "NSE"
        self.token_exchange_cache[self.REQ_SENSEX_TOKEN] = "BSE"

        # 🔥 SMART 1-SECOND BATCH & STORAGE PURGE MATRIX
        self.buffered_ticks = {}        # Structure: {token: (price_str, timestamp)}
        self.buffer_lock = threading.Lock()
        self.BATCH_WINDOW_SECONDS = 1.0  # Exactly 1 Second Batching Interval
        self.DATA_PURGE_SECONDS = 60.0   # Clear data older than 1 minute

    def login_manager(self):
        """ऑटोमैटिक 2FA जनरेट करके नया टोकन सेट करने का कोर लॉजिक"""
        with self.session_lock:
            if MANUAL_JWT_TOKEN != "":
                self.jwt_token = MANUAL_JWT_TOKEN
                self.obj.setAccessToken(self.jwt_token)
                return True
                
            current_now = time.time()
            if current_now - self.last_login_time < 60:
                return True
                
            try:
                self.obj = SmartConnect(api_key=API_KEY)
                totp = pyotp.TOTP(TOTP_STR).now()
                data = self.obj.generateSession(CLIENT_CODE, MPIN, totp)
                self.last_login_time = time.time()
                
                if data and data.get('status'):
                    self.jwt_token = data['data']['jwtToken']
                    self.feed_token = self.obj.feed_token
                    self.refresh_token = data['data']['refreshToken'] 
                    
                    # ऑब्जेक्ट में नया टोकन फ़ीड करना
                    self.obj.setAccessToken(self.jwt_token)
                    print("🔑 [Login Manager]: New Session Generated Successfully.")
                    return True
                else:
                    print(f"⚠️ [Login Manager]: Login failed with message: {data.get('message')}")
                    return False
            except Exception as e:
                print(f"❌ [Login Manager] Exception occurred: {str(e)}")
                time.sleep(5)
                return False

    def session_watchdog_loop(self):
        """बैकग्राउंड वॉचडॉग जो 5 मिनट में टोकन एक्सपायरी चेक करके ऑटो-रीकनेक्ट करता है"""
        while self.is_running:
            time.sleep(300) 
            if not self.is_running: 
                break
            if MANUAL_JWT_TOKEN != "": 
                continue
            
            try:
                now_dt = datetime.now()
                # सुबह 8:15 बजे का ऑटोमैटिक डेली फ्रेश रिसेट
                if now_dt.hour == 8 and now_dt.minute >= 15 and self.last_daily_reset_date != now_dt.strftime("%Y-%m-%d"):
                    self.is_refreshing_session = True
                    if self.login_manager(): 
                        self.last_daily_reset_date = now_dt.strftime("%Y-%m-%d")
                        if self.sws:
                            print("🔄 [Watchdog]: Daily 8:15 AM reset. Restarting WebSocket with fresh token...")
                            self.sws.close() # क्लोज करने पर on_close खुद इसे नए टोकन से चालू कर देगा
                    self.is_refreshing_session = False
                    continue
                
                # लाइव टोकन वैलिडेशन चेक
                if self.obj and self.jwt_token and not self.is_refreshing_session:
                    profile_res = self.obj.getProfile() # शुद्ध रूप से बिना पैरामीटर के ऑथेंटिकेशन टेस्ट
                    
                    if not profile_res or not profile_res.get("status") or profile_res.get("errorCode") == "AG8001":
                        print("🚨 [Watchdog]: Invalid Token (AG8001) Detected! Triggering Auto-Login...")
                        self.is_refreshing_session = True
                        if self.login_manager():
                            if self.sws:
                                print("🔄 [Watchdog]: Token renewed successfully. Re-syncing WebSocket Connection...")
                                self.sws.close() # पुराना कनेक्शन तोड़कर नए टोकन के साथ फ्रेश रीकनेक्ट ट्रिगर
                        self.is_refreshing_session = False
            except Exception as e: 
                print(f"⚠️ [Watchdog Loop Error]: {str(e)}")

    def send_batch_binary_to_apk(self, updates_list):
        """🔥 PURE BATCH BINARY PACKET EMITTER (Combines multiple symbols into one single chunk)"""
        if not updates_list:
            return
        try:
            batch_buffer = bytearray()
            symbols_counted = 0
            
            for token, price_str in updates_list:
                token_int = int(token)
                raw_float = float(price_str)
                
                if token == self.REQ_NIFTY_TOKEN:
                    price_int = int(round(raw_float * 100.0 * 100.0))
                else:
                    price_int = int(round(raw_float * 100.0))
                    
                # 8 Bytes block append (4 bytes Token + 4 bytes Price) Big-Endian
                batch_buffer.extend(struct.pack('>ii', token_int, price_int))
                symbols_counted += 1
                
            if batch_buffer:
                sio.emit('live_data', bytes(batch_buffer))
                # Logs को शांत रखने के लिए प्रिंट को कमेंट या सीमित कर सकते हैं
        except Exception: 
            pass

    def add_tick_to_batch_buffer(self, token, price_str):
        """🔥 RAM STABILIZED BUFFER CONTROL (Saves inside volatile memory with time logs)"""
        with self.buffer_lock:
            self.buffered_ticks[token] = (price_str, time.time())

    def start_1sec_flush_and_purge_loop(self):
        """🔥 ULTRA-FAST MEMORY FLUSHER AND 1-MINUTE PURGE ENGINE"""
        while self.is_running:
            time.sleep(self.BATCH_WINDOW_SECONDS)
            
            updates_to_send = []
            current_time = time.time()
            expired_tokens = []
            
            with self.buffer_lock:
                for token, (price_str, ts) in list(self.buffered_ticks.items()):
                    updates_to_send.append((token, price_str))
                    
                    # 60 सेकंड से पुराना डेटा रैम से डिलीट करना
                    if current_time - ts > self.DATA_PURGE_SECONDS:
                        expired_tokens.append(token)
                
                for token in expired_tokens:
                    if token in self.buffered_ticks:
                        del self.buffered_ticks[token]
                        
            if updates_to_send:
                self.send_batch_binary_to_apk(updates_to_send)

    def trigger_smart_resubscription(self):
        """वेबसोकेट ऑन होते ही पुराने सारे एक्टिव सिम्बल्स को दोबारा सब्सक्राइब करने का ऑटो लॉजिक"""
        try:
            if not self.active_subscribed_tokens: 
                return
            grouped_subs = {}
            for token in self.active_subscribed_tokens:
                exch_name = self.token_exchange_cache.get(token)
                if not exch_name:
                    exch_name = self.detect_exchange_by_token(token)
                    self.token_exchange_cache[token] = exch_name
                exch_type = self.exchange_map.get(exch_name, nse)
                if exch_type not in grouped_subs: 
                    grouped_subs[exch_type] = []
                grouped_subs[exch_type].append(token)
                
            if grouped_subs and self.sws and self.is_ws_connected:
                sub_params = [{"exchangeType": k, "tokens": v} for k, v in grouped_subs.items()]
                self.sws.subscribe("munh_batch", 1, sub_params)
                print(f"📡 [WebSocket Engine]: Re-subscribed to {len(self.active_subscribed_tokens)} tokens successfully.")
                
                # सिंक कैश अपडेट्स भेजना
                sync_updates = []
                for t in self.active_subscribed_tokens:
                    if t in self.last_known_prices:
                        sync_updates.append((t, self.last_known_prices[t]))
                if sync_updates:
                    self.send_batch_binary_to_apk(sync_updates)
        except Exception: 
            pass

    def process_incoming_ui_payload(self, raw_payload_str):
        try:
            if not raw_payload_str: 
                return
            payload = json.loads(raw_payload_str) if isinstance(raw_payload_str, str) else raw_payload_str
            if not payload or not isinstance(payload, dict):
                return

            action = payload.get("action", "sub")
            input_tokens = payload.get("tokens", [])
            tokens_to_process = []

            if isinstance(input_tokens, list):
                for item in input_tokens:
                    token_str = str(item).strip()
                    if token_str and token_str != "None":
                        detected_exch = self.detect_exchange_by_token(token_str)
                        tokens_to_process.append((token_str, detected_exch))
                        if action == "sub" and token_str not in self.active_subscribed_tokens:
                            self.active_subscribed_tokens.add(token_str)
                        elif action == "unsub" and token_str in self.active_subscribed_tokens:
                            self.active_subscribed_tokens.remove(token_str)
            else:
                token_str = str(input_tokens).strip()
                if token_str and token_str != "None":
                    detected_exch = self.detect_exchange_by_token(token_str)
                    tokens_to_process.append((token_str, detected_exch))
                    if action == "sub" and token_str not in self.active_subscribed_tokens:
                        self.active_subscribed_tokens.add(token_str)
                    elif action == "unsub" and token_str in self.active_subscribed_tokens:
                        self.active_subscribed_tokens.remove(token_str)

            if not tokens_to_process: 
                return

            if action == "sub":
                seg_groups = {}
                for token, exch in tokens_to_process:
                    self.token_exchange_cache[token] = exch
                    exch_type = self.exchange_map.get(exch, nse)
                    if exch_type not in seg_groups:
                        seg_groups[exch_type] = []
                    seg_groups[exch_type].append(token)

                if self.sws and self.is_ws_connected:
                    sub_params = [{"exchangeType": exch_type, "tokens": tokens} for exch_type, tokens in seg_groups.items()]
                    self.sws.subscribe("munh_batch", 1, sub_params)
                    
                    immediate_updates = []
                    for token, _ in tokens_to_process:
                        if token in self.last_known_prices:
                            immediate_updates.append((token, self.last_known_prices[token]))
                    if immediate_updates:
                        self.send_batch_binary_to_apk(immediate_updates)
                            
            elif action == "unsub":
                seg_unsub_groups = {}
                for token, exch in tokens_to_process:
                    if token in self.token_exchange_cache:
                        del self.token_exchange_cache[token]
                    exch_type = self.exchange_map.get(exch, nse)
                    if exch_type not in seg_unsub_groups:
                        seg_unsub_groups[exch_type] = []
                    seg_unsub_groups[exch_type].append(token)
                    
                if self.sws and self.is_ws_connected and seg_unsub_groups:
                    unsub_params = [{"exchangeType": exch_type, "tokens": tokens} for exch_type, tokens in seg_unsub_groups.items()]
                    self.sws.unsubscribe("munh_batch", 1, unsub_params)

        except Exception: 
            pass

    def detect_exchange_by_token(self, token, symbol=""):
        """⚡ MASTER AUTO DETECTOR FOR NSE, BSE, MCX, NFO, BFO"""
        symbol = str(symbol).upper()
        token_str = str(token).strip()
        
        if token_str == self.REQ_NIFTY_TOKEN:
            return "NSE"
        if token_str == self.REQ_SENSEX_TOKEN:
            return "BSE"
            
        if "GOLD" in symbol or "SILVER" in symbol or "CRUDEOIL" in symbol or "COPPER" in symbol:
            return "MCX"
        if "NIFTY" in symbol or "BANKNIFTY" in symbol:
            return "NFO" if ("CE" in symbol or "PE" in symbol or "FUT" in symbol) else "NSE"
        
        try:
            token_int = int(token_str)
            if 10000 <= token_int <= 90000:
                return "NFO"
            if token_int >= 210000:
                return "MCX"
            if 500000 <= token_int <= 600000:
                return "BSE"
        except ValueError:
            pass
        return "NSE" 

    def on_data(self, wsapp, msg):
        """वेबसोकेट से आने वाले लाइव टिक को बिना रुकावट रैम बफर में डालना"""
        try:
            if isinstance(msg, str):
                try: msg = json.loads(msg)
                except Exception: pass

            if isinstance(msg, list):
                for single_tick in msg:
                    if isinstance(single_tick, dict):
                        t_ = single_tick.get('token') or single_tick.get('tk')
                        l_ = single_tick.get('last_traded_price') or single_tick.get('lp')
                        if t_ and l_:
                            t_str = str(t_)
                            f_price = "{:.2f}".format(float(l_) / 100.0)
                            self.last_known_prices[t_str] = f_price
                            self.add_tick_to_batch_buffer(t_str, f_price)
                            
            elif isinstance(msg, dict):
                token = msg.get('token') or msg.get('tk')
                lp = msg.get('last_traded_price') or msg.get('lp')
                if token and lp:
                    token_str = str(token)
                    formatted_price = "{:.2f}".format(float(lp) / 100.0)
                    self.last_known_prices[token_str] = formatted_price
                    self.add_tick_to_batch_buffer(token_str, formatted_price)
        except Exception:
            pass

    def on_close(self, wsapp, close_status_code, close_msg):
        self.is_ws_connected = False
        print("🔴 [WebSocket Engine]: Connection Closed. Re-instantiating in 5 seconds...")
        if self.is_running: 
            threading.Timer(5.0, self.start_websocket).start()

    def on_error(self, wsapp, error): 
        self.is_ws_connected = False
        print(f"⚠️ [WebSocket Engine] Error came: {str(error)}")

    def start_websocket(self):
        if not self.is_running: 
            return
        if not self.jwt_token:
            self.login_manager()
            
        if self.jwt_token:
            try:
                self.sws = SmartWebSocketV2(self.jwt_token, API_KEY, CLIENT_CODE, self.feed_token)
                self.sws.on_data = self.on_data
                
                def on_open_handler(ws):
                    self.is_ws_connected = True
                    print("🟢 [WebSocket Engine]: Connection Live & Established with Angel One!")
                    self.trigger_smart_resubscription()
                    
                self.sws.on_open = on_open_handler
                self.sws.on_close = self.on_close
                self.sws.on_error = self.on_error
                threading.Thread(target=self.sws.connect, daemon=True).start()
            except Exception as e:
                print(f"❌ [WebSocket Connection Error]: {str(e)}. Retrying...")
                threading.Timer(5.0, self.start_websocket).start()

    def start_engine(self):
        print("⚙️ [Core Engine]: Booting up Munh Titan System...")
        while not self.login_manager(): 
            time.sleep(5)
        threading.Thread(target=self.session_watchdog_loop, daemon=True).start()
        threading.Thread(target=self.start_1sec_flush_and_purge_loop, daemon=True).start()
        self.start_websocket()

engine = MunhTitanEngine()

# ==========================================================
# 3. SOCKETIO EVENTS LISTENER
# ==========================================================
@sio.event
def connect(sid, environ): 
    connected_clients.add(sid)
    print(f"🟢 [Client Connected]: SID -> {sid}")

@sio.event
def subscribe_request(sid, data): 
    engine.process_incoming_ui_payload(data)

@sio.event
def disconnect(sid): 
    if sid in connected_clients:
        connected_clients.remove(sid)
    print(f"🔴 [Client Disconnected]: SID -> {sid}")

# ==========================================================
# 4. WEB SERVER HTTP FLASK ROUTES
# ==========================================================
@flask_app.route('/')
def health_check(): 
    return {"status": "alive", "current_score": user_score, "themes_active": APP_THEME_COLORS}, 200

@flask_app.route('/register', methods=['GET', 'POST'])
def register():
    return redirect(url_for('sign_page'))

@flask_app.route('/sign', methods=['GET'])
def sign_page():
    return {"status": "success", "message": "Welcome to the subsequent Sign Page logic."}, 200

@flask_app.route('/add_score', methods=['POST'])
def update_score():
    data = request.json
    points = data.get('points', 0) if data else 0
    new_total = add_to_score(points)
    return jsonify({"message": "Score updated successfully", "total_score": new_total}), 200

# ==========================================================
# 5. SERVER START SYSTEM
# ==========================================================
if __name__ == "__main__":
    threading.Thread(target=engine.start_engine, daemon=True).start()
    
    port = int(os.environ.get('PORT', 5000))
    print(f"🚀 [Matrix Server]: Operating on Port {port}...")
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), wsgi_app, log_output=False)
