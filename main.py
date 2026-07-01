import eventlet
eventlet.monkey_patch()

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
from flask import Flask, redirect, url_for, request, jsonify
import socketio

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
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
flask_app = Flask(__name__)
wsgi_app = socketio.WSGIApp(sio, flask_app)
connected_clients = set()

# ==========================================================
# 🔥 24/7 UNLIMITED ALIVE TRICK
# ==========================================================
def keep_alive_audio():
    os.environ['SDL_VIDEODRIVER'] = 'dummy'
    try:
        import pygame
        pygame.mixer.init()
        if os.path.exists("silent.mp3"):
            pygame.mixer.music.load("silent.mp3")
            while True:
                if not pygame.mixer.music.get_busy(): 
                    pygame.mixer.music.play()
                time.sleep(1)
    except: pass
threading.Thread(target=keep_alive_audio, daemon=True).start()

# ==========================================================
# 🛑 CONFIGURATION & GLOBALS
# ==========================================================
API_KEY = "Z80WG5Sg"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
nse, nfo, bse, bfo, mcx = 1, 2, 3, 4, 5

# ==========================================================
# 2. TITAN ENGINE SYSTEM LOGIC
# ==========================================================
class MunhTitanEngine:
    def __init__(self):
        self.obj = SmartConnect(api_key=API_KEY)
        self.sws = None
        self.is_running = True
        self.jwt_token = None
        self.feed_token = None
        self.is_ws_connected = False 
        
        self.token_exchange_cache = {} 
        self.last_known_prices = {} 
        self.active_subscribed_tokens = set() 
        
        self.session_lock = threading.Lock()
        self.exchange_map = {"NSE": nse, "NFO": nfo, "BSE": bse, "BFO": bfo, "MCX": mcx}
        
        # Buffer Config for "Live" Performance
        self.buffered_ticks = {}
        self.buffer_lock = threading.Lock()
        self.BATCH_WINDOW_SECONDS = 0.1  # 100ms Flush Window
        self.DATA_PURGE_SECONDS = 60.0

    def chunk_list(self, l, n):
        """Split subscription list into chunks of size n"""
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def login_manager(self):
        with self.session_lock:
            try:
                self.obj = SmartConnect(api_key=API_KEY)
                totp = pyotp.TOTP(TOTP_STR).now()
                data = self.obj.generateSession(CLIENT_CODE, MPIN, totp)
                if data.get('status'):
                    self.jwt_token = data['data']['jwtToken']
                    self.feed_token = data['data']['feedToken']
                    self.obj.setAccessToken(self.jwt_token)
                    print("🔑 [Login Manager]: Session Generated.")
                    return True
            except Exception as e:
                print(f"❌ Login Failed: {e}")
                time.sleep(5)
                return False

    def session_watchdog_loop(self):
        while self.is_running:
            time.sleep(60)
            # Simple health check
            if not self.is_ws_connected:
                print("🔄 [Watchdog]: Connection lost. Re-authenticating...")
                self.login_manager()
                self.start_websocket()

    def send_batch_binary_to_apk(self, updates_list):
        if not updates_list: return
        try:
            batch_buffer = bytearray()
            for token, price_str in updates_list:
                token_int = int(token)
                price_int = int(round(float(price_str) * 100.0))
                batch_buffer.extend(struct.pack('>ii', token_int, price_int))
            if batch_buffer:
                sio.emit('live_data', bytes(batch_buffer))
        except: pass

    def add_tick_to_batch_buffer(self, token, price_str):
        with self.buffer_lock:
            self.buffered_ticks[token] = (price_str, time.time())

    def start_1sec_flush_and_purge_loop(self):
        while self.is_running:
            time.sleep(self.BATCH_WINDOW_SECONDS)
            updates_to_send = []
            
            with self.buffer_lock:
                for token, (price_str, ts) in list(self.buffered_ticks.items()):
                    updates_to_send.append((token, price_str))
                self.buffered_ticks.clear()
            
            if updates_to_send:
                self.send_batch_binary_to_apk(updates_to_send)

    def trigger_smart_resubscription(self):
        """Batch subscription to avoid API limits"""
        if not self.active_subscribed_tokens or not self.sws or not self.is_ws_connected: return
        
        grouped_subs = {}
        for token in self.active_subscribed_tokens:
            exch = self.token_exchange_cache.get(token, "NSE")
            exch_type = self.exchange_map.get(exch, nse)
            if exch_type not in grouped_subs: grouped_subs[exch_type] = []
            grouped_subs[exch_type].append(token)
        
        for exch_type, tokens in grouped_subs.items():
            # Subscribe in chunks of 500 to prevent errors
            for chunk in self.chunk_list(tokens, 500):
                self.sws.subscribe("munh_batch", 1, [{"exchangeType": exch_type, "tokens": chunk}])
                time.sleep(0.05) # Prevent socket flood

    def process_incoming_ui_payload(self, raw_payload_str):
        try:
            payload = json.loads(raw_payload_str) if isinstance(raw_payload_str, str) else raw_payload_str
            action = payload.get("action", "sub")
            tokens = payload.get("tokens", [])
            token_list = tokens if isinstance(tokens, list) else [tokens]
            
            for t in token_list:
                token_str = str(t).strip()
                if not token_str or token_str == "None": continue
                
                if action == "sub":
                    self.active_subscribed_tokens.add(token_str)
                    exch = self.detect_exchange_by_token(token_str)
                    self.token_exchange_cache[token_str] = exch
                    # Immediate single sub if connected
                    if self.sws and self.is_ws_connected:
                        self.sws.subscribe("munh_batch", 1, [{"exchangeType": self.exchange_map[exch], "tokens": [token_str]}])
                elif action == "unsub":
                    self.active_subscribed_tokens.discard(token_str)
        except: pass

    def detect_exchange_by_token(self, token):
        try:
            t = int(token)
            if 10000 <= t <= 90000: return "NFO"
            if t >= 210000: return "MCX"
            if 500000 <= t <= 600000: return "BSE"
        except: pass
        return "NSE"

    def on_data(self, wsapp, msg):
        try:
            data = msg if isinstance(msg, list) else [msg]
            for tick in data:
                tk = str(tick.get('token') or tick.get('tk'))
                lp = tick.get('last_traded_price') or tick.get('lp')
                if tk and lp:
                    price = "{:.2f}".format(float(lp) / 100.0)
                    self.last_known_prices[tk] = price
                    self.add_tick_to_batch_buffer(tk, price)
        except: pass

    def start_websocket(self):
        if not self.jwt_token: self.login_manager()
        try:
            self.sws = SmartWebSocketV2(self.jwt_token, API_KEY, CLIENT_CODE, self.feed_token)
            self.sws.on_data = self.on_data
            self.sws.on_open = lambda ws: [setattr(self, 'is_ws_connected', True), self.trigger_smart_resubscription()]
            self.sws.on_close = lambda ws, code, msg: setattr(self, 'is_ws_connected', False)
            self.sws.on_error = lambda ws, error: setattr(self, 'is_ws_connected', False)
            threading.Thread(target=self.sws.connect, daemon=True).start()
        except:
            threading.Timer(5.0, self.start_websocket).start()

    def start_engine(self):
        self.login_manager()
        threading.Thread(target=self.session_watchdog_loop, daemon=True).start()
        threading.Thread(target=self.start_1sec_flush_and_purge_loop, daemon=True).start()
        self.start_websocket()

engine = MunhTitanEngine()

@sio.event
def connect(sid, environ): 
    connected_clients.add(sid)

@sio.event
def subscribe_request(sid, data): 
    engine.process_incoming_ui_payload(data)

@flask_app.route('/')
def health_check(): return {"status": "alive", "score": user_score}, 200

@flask_app.route('/sign')
def sign_page(): return {"status": "success"}, 200

if __name__ == "__main__":
    threading.Thread(target=engine.start_engine, daemon=True).start()
    port = int(os.environ.get('PORT', 5000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), wsgi_app, log_output=False)
