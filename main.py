import asyncio
import json
import logging
import datetime
import os
import pytz
import socketio
import aiohttp
import websockets
from aiohttp import web

# --- 🕒 TIMEZONE & LOGGING SETUP ---
IST = pytz.timezone('Asia/Kolkata')

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MUNH_TITAN_BACKEND")

# --- 🔑 UPSTOX CREDENTIALS ---
API_KEY = "eba0a80f-c907-42fa-a926-6672a120254d"
API_SECRET = os.getenv("UPSTOX_API_SECRET", "cg0pdqyg8t")

# 1-YEAR ANALYTICS ACCESS TOKEN
DEFAULT_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI2MkFIN0siLCJqdGkiOiI2YTdhMTJlZjk1YjgyYzEzZjc5OWEyMmIiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlzRXh0ZW5kZWQiOnRydWUsImlhdCI6MTc4NjM4NTEzNSwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxODE3OTM1MjAwfQ.0z7HMMUZUwJ6mRkzY3EUE1bB36_i1c7M-6yiNc8clgs"

ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", DEFAULT_TOKEN).strip()

# --- 🚀 REAL-TIME DATA STORAGE & STATE ---
LTP_CACHE = {}                 
SUBSCRIBED_TOKENS = set(["NSE_INDEX|Nifty 50", "BSE_INDEX|SENSEX"])
MAIN_EVENT_LOOP = None

# Socket.IO & Aiohttp Setup
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# --- 📡 BROADCAST LIVE TICK VIA SOCKET.IO ---
async def broadcast_tick(token: str, price: float):
    price_str = f"{price:.2f}"
    LTP_CACHE[token] = price_str
    
    payload = {
        "instrument_key": token,
        "ltp": price_str
    }
    await sio.emit("live_data", payload)

    # Standardize Index aliases for Android Client mapping
    if "Nifty 50" in token:
        LTP_CACHE["NIFTY"] = price_str
        await sio.emit("live_data", {"instrument_key": "NIFTY", "ltp": price_str})
    elif "SENSEX" in token:
        LTP_CACHE["SENSEX"] = price_str
        await sio.emit("live_data", {"instrument_key": "SENSEX", "ltp": price_str})

# --- 🔄 GET UPSTOX AUTHORIZED WEBSOCKET REDIRECT URI ---
async def get_upstox_market_data_feed_authorize():
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }
    url = "https://api.upstox.com/v2/feed/market-data-feed/authorize"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    res = await resp.json()
                    if res.get("status") == "success":
                        return res.get("data", {}).get("authorized_redirect_uri")
                else:
                    logger.error(f"❌ Failed to authorize market data feed: status {resp.status}")
    except Exception as e:
        logger.error(f"❌ Exception in get_upstox_market_data_feed_authorize: {e}")
    return None

# --- ⚡ UPSTOX REAL WEBSOCKET CONNECTION & STREAMING LOOP ---
async def start_upstox_real_websocket():
    logger.info("⚡ Starting Upstox Real WebSocket Connection Manager...")
    while True:
        try:
            auth_uri = await get_upstox_market_data_feed_authorize()
            if not auth_uri:
                logger.warning("⚠️ Authorized URI not received. Retrying WebSocket connection in 5 seconds...")
                await asyncio.sleep(5)
                continue

            logger.info("🟢 Connecting to Upstox Market Data WebSocket Feed...")
            async with websockets.connect(auth_uri) as websocket:
                logger.info("🟢 Upstox WebSocket Connected Successfully!")

                # Send initial subscription if tokens are present
                if SUBSCRIBED_TOKENS:
                    sub_payload = {
                        "guid": "munh-titan-guid",
                        "method": "sub",
                        "data": {
                            "mode": "ltpc",
                            "instrumentKeys": list(SUBSCRIBED_TOKENS)
                        }
                    }
                    await websocket.send(json.dumps(sub_payload))
                    logger.info(f"📡 Subscribed to tokens on connect: {list(SUBSCRIBED_TOKENS)}")

                # Listen to incoming live websocket ticks
                async for message in websocket:
                    try:
                        if isinstance(message, bytes):
                            # If message is binary protobuf or json bytes, decode safely
                            try:
                                text_msg = message.decode('utf-8')
                                data_json = json.loads(text_msg)
                                feeds = data_json.get("feeds", {})
                                for token, feed_data in feeds.items():
                                    ltpc = feed_data.get("ltpc", {})
                                    ltp = ltpc.get("ltp")
                                    if ltp is not None:
                                        await broadcast_tick(token, float(ltp))
                            except Exception:
                                pass # Binary protobuf fallback if needed
                        elif isinstance(message, str):
                            data_json = json.loads(message)
                            feeds = data_json.get("feeds", {})
                            for token, feed_data in feeds.items():
                                ltpc = feed_data.get("ltpc", {})
                                ltp = ltpc.get("ltp")
                                if ltp is not None:
                                    await broadcast_tick(token, float(ltp))
                    except Exception as parse_err:
                        logger.error(f"❌ Error parsing websocket message: {parse_err}")

        except websockets.exceptions.ConnectionClosed as cc:
            logger.warning(f"⚠️ Upstox WebSocket Connection Closed: {cc}. Reconnecting in 3 seconds...")
            await asyncio.sleep(3)
        except Exception as e:
            logger.error(f"❌ Upstox WebSocket Error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

# --- 📊 CHART DATA API ENDPOINT (/api/get_chart_data) ---
async def handle_get_chart_data(request):
    try:
        body = await request.json()
        token = body.get("token") or body.get("instrument_key") or ""
        interval = body.get("interval", "FIVE_MINUTE")
        todate = body.get("todate", "")
        fromdate = body.get("fromdate", "")
        
        if not token:
            return web.json_response({"status": "error", "message": "Missing token"}, status=400)
        
        interval_map = {
            "ONE_MINUTE": "1minute",
            "THREE_MINUTE": "3minute",
            "FIVE_MINUTE": "5minute",
            "TEN_MINUTE": "10minute",
            "FIFTEEN_MINUTE": "15minute",
            "THIRTY_MINUTE": "30minute",
            "ONE_HOUR": "60minute",
            "ONE_DAY": "day"
        }
        upstox_interval = interval_map.get(interval, "5minute")
        
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {ACCESS_TOKEN}'
        }
        
        if todate and fromdate:
            url = f"https://api.upstox.com/v2/historical-candle/{token}/{upstox_interval}/{todate}/{fromdate}"
        else:
            url = f"https://api.upstox.com/v2/historical-candle/{token}/{upstox_interval}"
            
        logger.info(f"📈 Fetching chart data from Upstox API: {url}")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                resp_text = await resp.text()
                if resp.status == 200:
                    data = json.loads(resp_text)
                    return web.json_response(data)
                else:
                    logger.error(f"❌ Upstox historical candle error status {resp.status}: {resp_text}")
                    return web.json_response({
                        "status": "error", 
                        "message": f"Upstox API error: {resp.status}",
                        "details": resp_text
                    }, status=resp.status)
    except Exception as e:
        logger.error(f"❌ Exception in handle_get_chart_data: {e}")
        return web.json_response({"status": "error", "message": str(e)}, status=500)

# --- 🔌 SUBSCRIPTION API ENDPOINT ---
async def handle_subscribe(request):
    try:
        body = await request.json()
        tokens = body.get("tokens", [])
        if tokens:
            for t in tokens:
                SUBSCRIBED_TOKENS.add(t)
            logger.info(f"📡 New Tokens Added to Subscriptions: {tokens}")
        return web.json_response({"status": "success", "subscribed": list(SUBSCRIBED_TOKENS)})
    except Exception as e:
        return web.json_response({"status": "error", "message": str(e)}, status=500)

# --- 🏠 HEALTH CHECK ROUTE ---
async def handle_health(request):
    return web.json_response({
        "status": "ok", 
        "service": "Munh Titan Backend", 
        "websocket_active": True,
        "subscribed_count": len(SUBSCRIBED_TOKENS),
        "time": datetime.datetime.now(IST).isoformat()
    })

# Setup routes
app.router.add_get('/', handle_health)
app.router.add_get('/health', handle_health)
app.router.add_post('/api/get_chart_data', handle_get_chart_data)
app.router.add_post('/api/subscribe', handle_subscribe)

# --- 🚀 BACKGROUND APP STARTUP ---
async def background_tasks(app):
    global MAIN_EVENT_LOOP
    MAIN_EVENT_LOOP = asyncio.get_running_loop()
    # Start the real WebSocket streaming background task
    asyncio.create_task(start_upstox_real_websocket())

app.on_startup.append(background_tasks)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app, host='0.0.0.0', port=port)
