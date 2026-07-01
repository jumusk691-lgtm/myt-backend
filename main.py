import eventlet
eventlet.monkey_patch()

import time
import json
import struct
import pyotp
import threading
import os
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from flask import Flask
import socketio

# ==========================================================
# 🛑 CONFIGURATION & GLOBALS
# ==========================================================
API_KEY = "Z80WG5Sg"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"

# Permanent Indices
PERMANENT_TOKENS = [{"exchangeType": 1, "tokens": ["26000", "26009"]}]

# ==========================================================
# ⚡ Web Server Engine
# ==========================================================
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
flask_app = Flask(__name__)
wsgi_app = socketio.WSGIApp(sio, flask_app)

# ==========================================================
# ⚡ TITAN ENGINE (DEBUG-ENABLED MODE)
# ==========================================================
class MunhTitanEngine:
    def __init__(self):
        self.obj = SmartConnect(api_key=API_KEY)
        self.sws = None
        self.is_running = True
        self.jwt_token = None
        self.feed_token = None
        self.is_ws_connected = False
        self.active_subscriptions = {} 

    def login_manager(self):
        try:
            totp = pyotp.TOTP(TOTP_STR).now()
            data = self.obj.generateSession(CLIENT_CODE, MPIN, totp)
            if data and data.get('status'):
                self.jwt_token = data['data']['jwtToken']
                self.feed_token = data['data']['feedToken']
                self.obj.setAccessToken(self.jwt_token)
                print("✅ Session Generated Successfully")
                return True
            else:
                print(f"❌ Login Error: {data}")
                return False
        except Exception as e:
            print(f"❌ Login Exception: {e}")
            return False

    def on_data(self, wsapp, msg):
        """⚡ FIREHOSE: Instant Pass-through with Debug"""
        try:
            data = msg if isinstance(msg, list) else [msg]
            for tick in data:
                tk = str(tick.get('token') or tick.get('tk'))
                lp = tick.get('last_traded_price') or tick.get('lp')
                
                # Debug: Print first few ticks to see if data is arriving
                # print(f"DEBUG: Received Tick - Token: {tk}, Price: {lp}") 
                
                if tk and lp:
                    price_val = int(round(float(lp) / 100.0 * 100.0))
                    sio.emit('live_data', struct.pack('>ii', int(tk), price_val))
        except Exception as e:
            print(f"❌ Data Error: {e}")

    def resubscribe_all(self):
        """Subscribe Permanent Indices + Watchlist with Logs"""
        if not self.sws or not self.is_ws_connected: return
        
        print("🔄 Resubscribing all tokens...")
        
        # 1. Permanent Indices
        self.sws.subscribe("munh_batch", 1, PERMANENT_TOKENS)
        print(f"✅ Subscribed Indices: {PERMANENT_TOKENS}")
        
        # 2. Watchlist
        grouped = {}
        for token, exch in self.active_subscriptions.items():
            if exch not in grouped: grouped[exch] = []
            grouped[exch].append(token)
        
        for exch, tokens in grouped.items():
            self.sws.subscribe("munh_batch", 1, [{"exchangeType": exch, "tokens": tokens}])
            print(f"✅ Subscribed Watchlist Tokens: {tokens} for Exchange: {exch}")

    def process_incoming_ui_payload(self, data):
        try:
            payload = json.loads(data) if isinstance(data, str) else data
            action = payload.get("action", "sub")
            token = str(payload.get("token", "")).strip()
            exch = int(payload.get("exchange", 1)) 

            if action == "sub" and token:
                self.active_subscriptions[token] = exch
                if self.sws and self.is_ws_connected:
                    self.sws.subscribe("munh_batch", 1, [{"exchangeType": exch, "tokens": [token]}])
                    print(f"➕ Added & Subscribed: {token}")
            elif action == "unsub" and token:
                self.active_subscriptions.pop(token, None)
                print(f"➖ Unsubscribed: {token}")
        except Exception as e:
            print(f"❌ Payload Error: {e}")

    def start_websocket(self):
        if not self.login_manager(): 
            threading.Timer(5.0, self.start_websocket).start()
            return
            
        try:
            self.sws = SmartWebSocketV2(self.jwt_token, API_KEY, CLIENT_CODE, self.feed_token)
            self.sws.on_data = self.on_data
            self.sws.on_open = lambda ws: [setattr(self, 'is_ws_connected', True), print("🌐 WebSocket Opened"), self.resubscribe_all()]
            self.sws.on_close = lambda ws, c, m: [setattr(self, 'is_ws_connected', False), print("⚠️ WebSocket Closed")]
            self.sws.on_error = lambda ws, e: [setattr(self, 'is_ws_connected', False), print(f"❌ WebSocket Error: {e}")]
            threading.Thread(target=self.sws.connect, daemon=True).start()
        except Exception as e:
            print(f"❌ Start WS Error: {e}")
            threading.Timer(5.0, self.start_websocket).start()

    def start_engine(self):
        threading.Thread(target=self.start_websocket, daemon=True).start()

engine = MunhTitanEngine()

@sio.event
def subscribe_request(sid, data): 
    engine.process_incoming_ui_payload(data)

if __name__ == "__main__":
    engine.start_engine()
    port = int(os.environ.get('PORT', 5000))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), wsgi_app, log_output=False)
