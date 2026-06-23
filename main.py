# File Name: main.py (Internal Reference Name: Munh)
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
# 🛑 FIREBASE REST SYNCER LOGIC (SMART CACHE UPGRADE)
# ==========================================================
def sync_url_to_firebase():
    log_file = "tunnel_log.txt"
    endpoint_url = f"{FIREBASE_DB_URL.rstrip('/')}/server_config/socket_url.json"
    last_synced_url = ""
    
    while True:
        try:
            if os.path.exists(log_file):
                with open(log_file, "r") as f:
                    content = f.read()
                    match = re.search(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com", content)
                    if match:
                        new_url = match.group(0)
                        if new_url != last_synced_url:
                            response = requests.put(endpoint_url, json=new_url)
                            if response.status_code == 200:
                                print(f"🚀 [Firebase Sync Success]: Target URL Updated -> {new_url}")
                                last_synced_url = new_url
        except Exception:
            # 🔥 SILENT RECONNECT: Internet na hone par terminal par kachra print nahi karega
            time.sleep(10)
            continue
        time.sleep(10)

# ⚡ Web Server Matrix Engine Integrations
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

    def login_manager(self):
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
                if data['status']:
                    self.jwt_token = data['data']['jwtToken']
                    self.feed_token = self.obj.feed_token
                    self.refresh_token = data['data']['refreshToken'] 
                    self.obj.setAccessToken(self.jwt_token)
                    print("🔑 [Login Manager]: Session Generated Successfully.")
                    return True
            except Exception:
                # 🔥 SILENT AUTO-RETRY: Offline hone par bina chillaye chupchaap 5 sec wait karega
                time.sleep(5)
                return False

    def session_watchdog_loop(self):
        while self.is_running:
            time.sleep(300) 
            if not self.is_running: 
                break
            if MANUAL_JWT_TOKEN != "": 
                continue
            
            try:
                now_dt = datetime.now()
                if now_dt.hour == 8 and now_dt.minute >= 15 and self.last_daily_reset_date != now_dt.strftime("%Y-%m-%d"):
                    self.is_refreshing_session = True
                    if self.login_manager(): 
                        self.last_daily_reset_date = now_dt.strftime("%Y-%m-%d")
                        if self.sws and self.is_ws_connected: 
                            threading.Thread(target=self.trigger_smart_resubscription, daemon=True).start()
                    self.is_refreshing_session = False
                    continue
                
                if self.obj and self.jwt_token and not self.is_refreshing_session and not self.is_ws_connected:
                    token_to_verify = self.refresh_token if self.refresh_token else ""
                    profile_res = self.obj.getProfile(token_to_verify)
                    
                    if not profile_res or not profile_res.get("status") or profile_res.get("errorCode") == "AG8001":
                        self.is_refreshing_session = True
                        if self.login_manager():
                            if self.sws and self.is_ws_connected: 
                                threading.Thread(target=self.trigger_smart_resubscription, daemon=True).start()
                        self.is_refreshing_session = False
            except Exception: 
                pass

    def send_delayed_price(self, token, price_str):
        try:
            token_str = str(token)
            exch_name = self.token_exchange_cache.get(token_str)
            if not exch_name:
                exch_name = self.detect_exchange_by_token(token_str)
                self.token_exchange_cache[token_str] = exch_name
                
            exch_type = self.exchange_map.get(exch_name, nse)
            
            json_packet = {
                'exchangeType': exch_type, 
                'token': token_str, 
                'ltp': float(price_str),
                't': token_str,
                'price': str(price_str)
            }
            sio.emit('live_data', json_packet)
            sio.emit('price_update', json_packet)
        except Exception: 
            pass

    def trigger_smart_resubscription(self):
        try:
            if not self.active_subscribed_tokens: 
                return
            grouped_subs = {}
            for token in self.active_subscribed_tokens:
                exch_name = self.token_exchange_cache.get(token, "NSE")
                exch_type = self.exchange_map.get(exch_name, nse)
                if exch_type not in grouped_subs: 
                    grouped_subs[exch_type] = []
                grouped_subs[exch_type].append(token)
                
            if grouped_subs and self.sws and self.is_ws_connected:
                sub_params = [{"exchangeType": k, "tokens": v} for k, v in grouped_subs.items()]
                self.sws.subscribe("munh_batch", 1, sub_params)
                for t in self.active_subscribed_tokens:
                    if t in self.last_known_prices:
                        self.send_delayed_price(t, self.last_known_prices[t])
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
            global_exchange = payload.get("exchangeType") or payload.get("exchange")
            
            if isinstance(global_exchange, int):
                reverse_map = {nse: "NSE", nfo: "NFO", bse: "BSE", bfo: "BFO", mcx: "MCX"}
                global_exchange = reverse_map.get(global_exchange, "NSE")
                
            input_tokens = payload.get("tokens", [])
            tokens_to_sub = []

            if isinstance(input_tokens, list):
                for item in input_tokens:
                    token_str = str(item)
                    if token_str and token_str != "None":
                        if action == "sub":
                            tokens_to_sub.append((token_str, global_exchange if global_exchange else self.detect_exchange_by_token(token_str)))
                        if action == "sub" and token_str not in self.active_subscribed_tokens:
                            self.active_subscribed_tokens.add(token_str)
                        elif action == "unsub":
                            tokens_to_sub.append((token_str, global_exchange if global_exchange else self.detect_exchange_by_token(token_str)))
            else:
                token_str = str(input_tokens)
                if token_str and token_str != "None":
                    if action == "sub":
                        tokens_to_sub.append((token_str, global_exchange if global_exchange else self.detect_exchange_by_token(token_str)))
                    if action == "sub" and token_str not in self.active_subscribed_tokens:
                        self.active_subscribed_tokens.add(token_str)
                    elif action == "unsub":
                        tokens_to_sub.append((token_str, global_exchange if global_exchange else self.detect_exchange_by_token(token_str)))

            if not tokens_to_sub: 
                return

            if action == "sub":
                seg_groups = {}
                for token, exch in tokens_to_sub:
                    self.token_exchange_cache[token] = exch
                    exch_type = self.exchange_map.get(exch, nse)
                    if exch_type not in seg_groups:
                        seg_groups[exch_type] = []
                    seg_groups[exch_type].append(token)

                if self.sws and self.is_ws_connected:
                    sub_params = [{"exchangeType": exch_type, "tokens": tokens} for exch_type, tokens in seg_groups.items()]
                    self.sws.subscribe("munh_batch", 1, sub_params)
                    for token, _ in tokens_to_sub:
                        if token in self.last_known_prices:
                            self.send_delayed_price(token, self.last_known_prices[token])
                            
            elif action == "unsub":
                for token, _ in tokens_to_sub:
                    if token in self.token_exchange_cache:
                        del self.token_exchange_cache[token]
                    if token in self.active_subscribed_tokens:
                        self.active_subscribed_tokens.remove(token)

        except Exception: 
            pass

    def detect_exchange_by_token(self, token, symbol=""):
        symbol = str(symbol).upper()
        if "GOLD" in symbol or "SILVER" in symbol or "CRUDEOIL" in symbol or "COPPER" in symbol:
            return "MCX"
        if "NIFTY" in symbol or "BANKNIFTY" in symbol:
            return "NFO" if ("CE" in symbol or "PE" in symbol or "FUT" in symbol) else "NSE"
        
        try:
            token_int = int(token)
            if token_int in [99926000, 99919012]: 
                return "NSE" if token_int == 99926000 else "BSE"
            if 10000 <= token_int <= 90000:
                return "NFO"
            if token_int >= 210000:
                return "MCX"
        except ValueError:
            pass
        return "NSE" 

    def on_data(self, wsapp, msg):
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
                            self.send_delayed_price(t_str, f_price)
                            
            elif isinstance(msg, dict):
                token = msg.get('token') or msg.get('tk')
                lp = msg.get('last_traded_price') or msg.get('lp')
                if token and lp:
                    token_str = str(token)
                    formatted_price = "{:.2f}".format(float(lp) / 100.0)
                    self.last_known_prices[token_str] = formatted_price
                    self.send_delayed_price(token_str, formatted_price)
        except Exception:
            pass

    def on_close(self, wsapp, close_status_code, close_msg):
        self.is_ws_connected = False
        if self.is_running: 
            threading.Timer(5.0, self.start_websocket).start()

    def on_error(self, wsapp, error): 
        self.is_ws_connected = False

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
                    print("🟢 [WebSocket Engine]: Connection Live & Established!")
                    self.trigger_smart_resubscription()
                    
                self.sws.on_open = on_open_handler
                self.sws.on_close = self.on_close
                self.sws.on_error = self.on_error
                threading.Thread(target=self.sws.connect, daemon=True).start()
            except Exception:
                threading.Timer(5.0, self.start_websocket).start()

    def start_engine(self):
        print("⚙️ [Core Engine]: Booting up Munh Titan System...")
        while not self.login_manager(): 
            time.sleep(5)
        threading.Thread(target=self.session_watchdog_loop, daemon=True).start()
        self.start_websocket()

engine = MunhTitanEngine()

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

if __name__ == "__main__":
    threading.Thread(target=engine.start_engine, daemon=True).start()
    threading.Thread(target=sync_url_to_firebase, daemon=True).start()
    
    port = int(os.environ.get('PORT', 5000))
    eventlet.wsgi.server(eventlet.listen(('', port)), wsgi_app, log_output=False)
