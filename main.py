import asyncio
import json
import logging
import time
import datetime
import os
import threading
import pytz
import socketio
import aiohttp
import ssl
import websocket
import requests
from aiohttp import web

# --- 🕒 TIMEZONE & LOGGING SETUP ---
IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MUNH_TITAN_BACKEND")

# --- 🔑 UPSTOX CREDENTIALS ---
API_KEY = "eba0a80f-c907-42fa-a926-6672a120254d"
API_SECRET = os.getenv("UPSTOX_API_SECRET", "cg0pdqyg8t")
NEW_ANALYTICS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI2MkFIN0siLCJqdGkiOiI2YTdhMTJlZjk1YjgyYzEzZjc5OWEyMmIiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlzRXh0ZW5kZWQiOnRydWUsImlhdCI6MTc4NjM4NTEzNSwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxODE3OTM1MjAwfQ.0z7HMMUZUwJ6mRkzY3EUE1bB36_i1c7M-6yiNc8clgs"
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", NEW_ANALYTICS_TOKEN).strip()

# --- 🚀 GLOBAL STATES & TRACKING ---
LTP_CACHE = {}               
SUBSCRIBED_TOKENS = set(["NSE_INDEX|Nifty 50", "BSE_INDEX|SENSEX"])
PENDING_TOKENS_QUEUE = set()
upstox_ws_app = None
main_loop = None

# Socket.IO & Aiohttp Setup
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# --- 📡 BROADCAST TICK TO ANDROID CLIENTS ---
async def broadcast_tick(token: str, price: float):
    price_str = f"{price:.2f}"
    LTP_CACHE[token] = price_str
    
    payload = {
        "instrument_key": token,
        "ltp": price_str
    }
    await sio.emit("live_data", payload)
    logger.info(f"📡 Broadcasted Tick -> {token} : {price_str}")

    if token == "NSE_INDEX|Nifty 50":
        LTP_CACHE["NIFTY"] = price_str
        await sio.emit("live_data", {"instrument_key": "NIFTY", "ltp": price_str})
    elif token == "BSE_INDEX|SENSEX":
        LTP_CACHE["SENSEX"] = price_str
        await sio.emit("live_data", {"instrument_key": "SENSEX", "ltp": price_str})

# --- 🛡️ BATCHED SUBSCRIPTION SENDER ---
def process_pending_subscriptions():
    global upstox_ws_app
    while True:
        time.sleep(3.0)
        if PENDING_TOKENS_QUEUE and upstox_ws_app:
            try:
                tokens_to_send = list(PENDING_TOKENS_QUEUE)
                PENDING_TOKENS_QUEUE.clear()
                
                sub_msg = {
                    "guid": f"batch_sub_{int(time.time())}",
                    "method": "sub",
                    "data": {
                        "mode": "full",
                        "instrumentKeys": tokens_to_send
                    }
                }
                upstox_ws_app.send(json.dumps(sub_msg))
                logger.info(f"🚀 Sent New Batch Subscription for {len(tokens_to_send)} tokens to Upstox WS.")
            except Exception as e:
                logger.error(f"❌ Batch Subscription Error: {e}")

# --- 🌐 UPSTOX WEBSOCKET STREAM CONNECTOR ---
def start_upstox_websocket():
    try:
        import MarketDataFeedV3_pb2 as pb
    except ImportError:
        pb = None

    def on_open(ws):
        logger.info("✅ Upstox Market Feed WebSocket Connected Successfully!")
        if SUBSCRIBED_TOKENS:
            try:
                sub_msg = {
                    "guid": "initial_sub",
                    "method": "sub",
                    "data": {
                        "mode": "full",
                        "instrumentKeys": list(SUBSCRIBED_TOKENS)
                    }
                }
                ws.send(json.dumps(sub_msg))
            except Exception as e:
                logger.error(f"❌ Initial WS Sub Send Error: {e}")

    def on_message(ws, message):
        try:
            if isinstance(message, bytes):
                if pb:
                    feed_response = pb.FeedResponse()
                    feed_response.ParseFromString(message)
                    for instrument_key, feed_val in feed_response.feeds.items():
                        ltp = None
                        if feed_val.HasField("market_ff"):
                            ltp = getattr(feed_val.market_ff, 'ltp', None)
                        if not ltp and feed_val.HasField("ff"):
                            ltp = feed_val.ff.market_ff.ltp
                        
                        if ltp and main_loop:
                            asyncio.run_coroutine_threadsafe(broadcast_tick(instrument_key, float(ltp)), main_loop)
            elif isinstance(message, str):
                data_dict = json.loads(message)
                feeds = data_dict.get("feeds", {})
                for instrument_key, data in feeds.items():
                    ltp = data.get("ff", {}).get("marketFF", {}).get("ltp")
                    if ltp and main_loop:
                        asyncio.run_coroutine_threadsafe(broadcast_tick(instrument_key, float(ltp)), main_loop)
        except Exception as e:
            logger.error(f"❌ WS Message Parse Error: {e}")

    def on_error(ws, error):
        logger.error(f"❌ Upstox WS Error: {error}")

    def on_close(ws, code, reason):
        logger.warning(f"⚠️ Upstox WS Closed: {reason} (Code: {code})")

    while True:
        try:
            headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
            auth_resp = requests.get("https://api.upstox.com/v3/feed/market-data-feed/authorize", headers=headers, timeout=10)
            
            if auth_resp.status_code == 200:
                socket_uri = auth_resp.json().get("data", {}).get("authorizedRedirectUri")
                if socket_uri:
                    global upstox_ws_app
                    upstox_ws_app = websocket.WebSocketApp(
                        socket_uri, on_open=on_open, on_message=on_message,
                        on_error=on_error, on_close=on_close
                    )
                    upstox_ws_app.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            else:
                logger.error(f"❌ Upstox Authorization Failed (Status {auth_resp.status_code}): {auth_resp.text}")
        except Exception as e:
            logger.error(f"❌ Upstox Connection Exception: {e}")
            
        time.sleep(30)

# --- 🌐 SOCKET.IO CLIENT HANDLERS ---
@sio.event
async def connect(sid, environ):
    logger.info(f"📱 Android Client Connected: {sid}")
    if LTP_CACHE:
        await sio.emit('initial_ltps', LTP_CACHE, room=sid)

@sio.event
async def subscribe_request(sid, data):
    try:
        if isinstance(data, str):
            data = json.loads(data)
            
        tokens_input = data.get("instrumentKeys") or data.get("tokens", [])
        new_tokens_added = 0
        
        for item in tokens_input:
            token = str(item).strip().replace(":", "|")
            if token:
                if token not in SUBSCRIBED_TOKENS:
                    SUBSCRIBED_TOKENS.add(token)
                    PENDING_TOKENS_QUEUE.add(token)
                    new_tokens_added += 5
                
                cached_price = LTP_CACHE.get(token, "0.00")
                await sio.emit("live_data", {"instrument_key": token, "ltp": cached_price}, room=sid)
                
        if new_tokens_added > 0:
            logger.info(f"📥 Added {new_tokens_added} brand new tokens. Total Unique Subscribed: {len(SUBSCRIBED_TOKENS)}")
    except Exception as e:
        logger.error(f"❌ Error in subscribe_request: {e}")

@sio.event
async def disconnect(sid):
    logger.info(f"📱 Android Client Disconnected: {sid}")

# --- 🌐 REST HTTP ENDPOINTS ---
async def home_route(request: web.Request):
    return web.json_response({
        "status": True,
        "message": "MUNH Titan Backend Service is Live & Running!",
        "version": "1.0.0"
    })

app.router.add_get('/', home_route)

# --- 🔄 BACKGROUND TASKS ---
async def start_background_tasks(app):
    global main_loop
    main_loop = asyncio.get_running_loop()
    threading.Thread(target=start_upstox_websocket, daemon=True).start()
    threading.Thread(target=process_pending_subscriptions, daemon=True).start()
    logger.info("✅ Fully Automated Backend Service Initialized.")

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
