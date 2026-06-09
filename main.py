import time
import json
import threading
import asyncio
import websockets
import pyotp
import firebase_admin
from firebase_admin import credentials, db
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ==========================================================
# 🛑 CONFIGURATION
# ==========================================================
API_KEY = "Z80WG5Sg"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"

# Firebase Setup
cred = credentials.Certificate("serviceAccountKey.json") # File path ensure karein
firebase_admin.initialize_app(cred, {'databaseURL': 'https://trade-f600a-default-rtdb.firebaseio.com/'})
ref = db.reference('central_watchlist')

# ==========================================================
# 🚀 CORE ENGINE
# ==========================================================
class MunhTitanEngine:
    def __init__(self):
        self.obj = SmartConnect(api_key=API_KEY)
        self.sws = None
        self.jwt_token = None
        self.feed_token = None
        self.is_running = True
        self.is_ws_connected = False
        self.exchange_map = {"NSE": 1, "NFO": 2, "BSE": 3, "BFO": 4, "MCX": 5}

    def login(self):
        try:
            totp = pyotp.TOTP(TOTP_STR).now()
            data = self.obj.generateSession(CLIENT_CODE, MPIN, totp)
            if data['status']:
                self.jwt_token = data['data']['jwtToken']
                self.feed_token = self.obj.feed_token
                return True
            return False
        except Exception as e:
            print(f"Login Error: {e}")
            return False

    def on_data(self, wsapp, msg):
        token = str(msg.get('token') or msg.get('tk'))
        lp = msg.get('last_traded_price') or msg.get('lp')
        if token and lp:
            price = float(lp) / 100.0
            # LIVE FIREBASE UPDATE
            db.reference(f'live_prices/{token}').set({
                'price': price,
                'timestamp': time.time()
            })

    def sync_and_subscribe(self):
        # Central Watchlist se data read karna
        watchlist = ref.get()
        if not watchlist: return

        subs = {}
        for key, val in watchlist.items():
            token = val.get('token')
            exch = val.get('exch')
            exch_type = self.exchange_map.get(exch, 1)
            subs.setdefault(exch_type, []).append(token)

        # Subscribe
        if self.sws and self.is_ws_connected:
            sub_params = [{"exchangeType": et, "tokens": t} for et, t in subs.items()]
            self.sws.subscribe("munh_batch", 1, sub_params)
            print("✅ [Sync]: Watchlist synced and subscribed successfully.")

    def start_websocket(self):
        self.sws = SmartWebSocketV2(self.jwt_token, API_KEY, CLIENT_CODE, self.feed_token)
        self.sws.on_data = self.on_data
        self.sws.on_open = lambda ws: [setattr(self, 'is_ws_connected', True), self.sync_and_subscribe()]
        self.sws.on_close = lambda *args: setattr(self, 'is_ws_connected', False)
        threading.Thread(target=self.sws.connect, daemon=True).start()

    def run(self):
        if self.login():
            self.start_websocket()
            while True: time.sleep(1)

if __name__ == "__main__":
    MunhTitanEngine().run()
