import asyncio
import json
import logging
import time
import datetime
import os
import pytz
import socketio
import aiohttp
from aiohttp import web
import ssl
import websockets

# --- Upstox Official SDK ---
import upstox_client
from upstox_client.rest import ApiException

# --- 🕒 TIMEZONE & LOGGING SETUP ---
IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MUNH_TITAN_PROD_UPSTOX")

# --- ⚙️ INTERNAL APP STATE ENGINE ---
class AppState:
    def __init__(self):
        self.api_instance = None
        self.score = 0

state = AppState()

# --- 🔑 UPSTOX CREDENTIALS ---
API_KEY = "eba0a80f-c907-42fa-a926-6672a120254d"
API_SECRET = os.getenv("UPSTOX_API_SECRET", "cg0pdqyg8t")

# 1-YEAR ANALYTICS ACCESS TOKEN
NEW_ANALYTICS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI2MkFIN0siLCJqdGkiOiI2YTdhMTJlZjk1YjgyYzEzZjc5OWEyMmIiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlzRXh0ZW5kZWQiOnRydWUsImlhdCI6MTc4NjM4NTEzNSwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxODE3OTM1MjAwfQ.0z7HMMUZUwJ6mRkzY3EUE1bB36_i1c7M-6yiNc8clgs"
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", NEW_ANALYTICS_TOKEN).strip()

configuration = upstox_client.Configuration()
configuration.access_token = ACCESS_TOKEN

# --- 🚀 GLOBAL STATES & SCORE TRACKING ---
LTP_CACHE = {} 
SUBSCRIBED_TOKENS = set(["NSE_INDEX|Nifty 50", "BSE_INDEX|SENSEX"])

# Socket.IO & Aiohttp Setup
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# Helper function to broadcast live price tick to clients (Keeping exact original payload format)
async def broadcast_tick(token: str, price: float):
    price_str = f"{price:.2f}"
    LTP_CACHE[token] = price_str
    payload = {
        "instrument_key": token,
        "ltp": price_str
    }
    await sio.emit("live_data", payload)

    # Standardize Index aliases for Android Client mapping
    if token == "NSE_INDEX|Nifty 50":
        LTP_CACHE["NIFTY"] = price_str
        await sio.emit("live_data", {"instrument_key": "NIFTY", "ltp": price_str})
    elif token == "BSE_INDEX|SENSEX":
        LTP_CACHE["SENSEX"] = price_str
        await sio.emit("live_data", {"instrument_key": "SENSEX", "ltp": price_str})

# --- 🔄 UPSTOX WEBSOCKET FEED STREAMER (REPLACES REST POLLING) ---
async def start_upstox_websocket_streamer():
    logger.info("⚡ Upstox WebSocket Streamer Initializing...")
    
    # Get Authorized WebSocket URL from Upstox API
    api_version = "v2"
    url = f"https://api.upstox.com/{api_version}/feed/market-data-feed/authorize"
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

    ws_url = None
    async with aiohttp.ClientSession() as session:
        while ws_url is None:
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        res_data = await resp.json()
                        if res_data.get("status") == "success":
                            ws_url = res_data.get("data", {}).get("authorized_redirect_uri")
                            logger.info("✅ Successfully authorized Upstox WebSocket feed URL.")
                    else:
                        logger.error(f"❌ Failed to authorize WebSocket feed: {resp.status}")
            except Exception as e:
                logger.error(f"❌ Exception during WebSocket authorization: {e}")
            
            if ws_url is None:
                await asyncio.sleep(5)

    # Connect to Upstox WebSocket and stream data
    while True:
        try:
            async with websockets.connect(ws_url) as websocket:
                logger.info("🟢 Connected to Upstox Market Data Feed WebSocket!")
                
                # Send subscription message for tokens
                sub_message = {
                    "guid": "someguid",
                    "method": "sub",
                    "data": {
                        "mode": "ltp",
                        "instrumentKeys": list(SUBSCRIBED_TOKENS)
                    }
                }
                await websocket.send(json.dumps(sub_message))

                async for message in websocket:
                    try:
                        if isinstance(message, bytes):
                            pass
                        else:
                            data_dict = json.loads(message)
                            feeds = data_dict.get("feeds", {})
                            for inst_key, details in feeds.items():
                                ltp = details.get("ff", {}).get("market_ff", {}).get("ltp")
                                if ltp:
                                    await broadcast_tick(inst_key, float(ltp))
                    except Exception as parse_err:
                        pass

                    # Dynamically update subscriptions if new tokens are added by Android users
                    current_tokens = list(SUBSCRIBED_TOKENS)
                    sub_message = {
                        "guid": "dynamic_sub",
                        "method": "sub",
                        "data": {
                            "mode": "ltp",
                            "instrumentKeys": current_tokens
                        }
                    }
                    await websocket.send(json.dumps(sub_message))

        except Exception as ws_err:
            logger.warning(f"⚠️ WebSocket connection dropped: {ws_err}. Reconnecting in 3 seconds...")
            await asyncio.sleep(3.0)

# --- 🌐 SOCKET.IO HANDLERS ---
@sio.event
async def connect(sid, environ):
    logger.info(f"📱 Android Client Connected: {sid}")
    if LTP_CACHE:
        await sio.emit('initial_ltps', LTP_CACHE, room=sid)

async def handle_subscription(sid, data):
    try:
        if isinstance(data, str):
            data = json.loads(data)
        tokens_input = data.get("instrumentKeys") or data.get("tokens", [])
        for item in tokens_input:
            if isinstance(item, dict):
                token = str(item.get("token") or item.get("instrument_key", "")).strip()
            else:
                token = str(item).strip()
            if token:
                SUBSCRIBED_TOKENS.add(token)
        logger.info(f"Subscribed Upstox Keys Count: {len(SUBSCRIBED_TOKENS)}")
    except Exception as e:
        logger.error(f"❌ Error in subscribe_tokens: {e}")

@sio.event
async def subscribe_tokens(sid, data):
    await handle_subscription(sid, data)

@sio.event
async def subscribe_request(sid, data):
    await handle_subscription(sid, data)

@sio.event
async def disconnect(sid):
    logger.info(f"📱 Android Client Disconnected: {sid}")

# --- 🌐 REST HTTP API ENDPOINTS ---
async def home_route(request: web.Request):
    return web.json_response({
        "status": True,
        "message": "MUNH Titan Upstox Backend Service is Live & Running via WebSocket!",
        "version": "1.0.0"
    })

async def fetch_chart_data(request: web.Request):
    try:
        d = await request.json()
        instrument_key = str(d.get('token', '') or d.get('instrument_key', '')).strip()
        raw_interval = str(d.get('interval', "5minute")).strip().upper()
        if not instrument_key:
            return web.json_response({"status": False, "message": "Missing instrument_key"}, status=400)
        
        instrument_key = instrument_key.replace(":", "|")
        dt = datetime.datetime.now(IST)
        
        target_minutes = 5
        if "1MIN" in raw_interval:
            target_minutes = 1
        elif "15MIN" in raw_interval:
            target_minutes = 15
        elif "30MIN" in raw_interval:
            target_minutes = 30
        elif "1HOUR" in raw_interval or "60MIN" in raw_interval:
            target_minutes = 60

        mins_from_open = (dt.hour * 60 + dt.minute) - (9 * 60 + 15)
        if mins_from_open < 0:
            mins_from_open = 0

        return web.json_response({"status": True, "message": "SUCCESS", "data": []})
    except Exception as e:
        logger.error(f"Exception in fetch_chart_data: {e}")
        return web.json_response({"status": False, "message": str(e), "data": []}, status=500)

app.router.add_get('/', home_route)
app.router.add_post('/api/get_chart_data', fetch_chart_data)

# --- 🔄 BACKGROUND TASKS ---
async def start_background_tasks(app):
    asyncio.create_task(start_upstox_websocket_streamer())
    logger.info("✅ Upstox Backend Service Initialized with WebSocket Streamer Engine.")

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
