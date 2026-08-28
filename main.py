import asyncio
import json
import logging
import datetime
import os
import pytz
import socketio
import aiohttp
from aiohttp import web

# --- Upstox Official SDK ---
import upstox_client
from upstox_client.rest import ApiException

# --- 🕒 TIMEZONE & LOGGING SETUP ---
IST = pytz.timezone('Asia/Kolkata')

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MUNH_TITAN_WEBSOCKET")

# --- 🔑 UPSTOX CREDENTIALS ---
API_KEY = "eba0a80f-c907-42fa-a926-6672a120254d"
API_SECRET = os.getenv("UPSTOX_API_SECRET", "cg0pdqyg8t")

# 1-YEAR ANALYTICS ACCESS TOKEN
DEFAULT_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI2MkFIN0siLCJqdGkiOiI2YTdhMTJlZjk1YjgyYzEzZjc5OWEyMmIiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlzRXh0ZW5kZWQiOnRydWUsImlhdCI6MTc4NjM4NTEzNSwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxODE3OTM1MjAwfQ.0z7HMMUZUwJ6mRkzY3EUE1bB36_i1c7M-6yiNc8clgs"

ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", DEFAULT_TOKEN).strip()

configuration = upstox_client.Configuration()
configuration.access_token = ACCESS_TOKEN

# --- 🚀 REAL WEBSOCKET DATA STORAGE ---
LTP_CACHE = {}                 
SUBSCRIBED_TOKENS = set(["NSE_INDEX|Nifty 50", "BSE_INDEX|SENSEX"])
MAIN_EVENT_LOOP = None
streamer = None

# Socket.IO & Aiohttp Setup
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# --- 📡 BROADCAST LIVE TICK FROM UPSTOX WEBSOCKET ---
async def broadcast_tick(token: str, price: float):
    price_str = f"{price:.2f}"
    LTP_CACHE[token] = price_str
    
    payload = {
        "instrument_key": token,
        "ltp": price_str
    }
    await sio.emit("live_data", payload)

    if "Nifty 50" in token:
        LTP_CACHE["NIFTY"] = price_str
        await sio.emit("live_data", {"instrument_key": "NIFTY", "ltp": price_str})
    elif "SENSEX" in token:
        LTP_CACHE["SENSEX"] = price_str
        await sio.emit("live_data", {"instrument_key": "SENSEX", "ltp": price_str})

# --- 🔄 FETCH REST LTP FALLBACK ---
async def fetch_rest_ltp(tokens_list):
    if not tokens_list:
        return
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }
    try:
        tokens_str = ",".join(tokens_list)
        url = f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={tokens_str}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    res_data = await resp.json()
                    if res_data.get("status") == "success":
                        feeds = res_data.get("data", {})
                        for inst_key, details in feeds.items():
                            ltp_val = details.get("last_price")
                            if ltp_val is not None:
                                await broadcast_tick(inst_key, float(ltp_val))
                                logger.info(f"📊 REST LTP Fallback Loaded -> {inst_key}: {ltp_val}")
    except Exception as e:
        logger.error(f"❌ Error fetching REST LTP fallback: {e}")

# --- 🔄 UPSTOX OFFICIAL WEBSOCKET STREAMER ---
async def start_upstox_websocket_streamer():
    global streamer
    logger.info("⚡ Upstox MarketDataStreamerV3 WebSocket Connecting...")
    
    def on_open():
        logger.info("🟢 Upstox WebSocket Connected Successfully via SDK!")
        if SUBSCRIBED_TOKENS:
            try:
                streamer.subscribe(list(SUBSCRIBED_TOKENS), "ltpc")
                logger.info(f"📡 Subscribed Tokens on WS Open: {list(SUBSCRIBED_TOKENS)}")
                if MAIN_EVENT_LOOP and MAIN_EVENT_LOOP.is_running():
                    asyncio.run_coroutine_threadsafe(fetch_rest_ltp(list(SUBSCRIBED_TOKENS)), MAIN_EVENT_LOOP)
            except Exception as e:
                logger.error(f"❌ Error in streamer subscription: {e}")

    def on_message(message):
        try:
            if MAIN_EVENT_LOOP and MAIN_EVENT_LOOP.is_running():
                if isinstance(message, dict):
                    feeds = message.get("feeds", {})
                    for token, data in feeds.items():
                        ltpc = data.get("ltpc", {})
                        ltp = ltpc.get("ltp")
                        if ltp is not None:
                            asyncio.run_coroutine_threadsafe(broadcast_tick(token, float(ltp)), MAIN_EVENT_LOOP)
        except Exception as e:
            logger.error(f"❌ Error processing WS message: {e}")

    def on_error(error):
        logger.error(f"❌ Upstox WS Error: {error}")

    def on_close(close_code, reason):
        logger.warning(f"⚠️ Upstox WS Closed: {close_code} - {reason}")

    try:
        from upstox_client.streamer import MarketDataStreamerV3
        
        streamer = MarketDataStreamerV3(
            app_key=API_KEY,
            access_token=ACCESS_TOKEN
        )
        streamer.on("open", on_open)
        streamer.on("message", on_message)
        streamer.on("error", on_error)
        streamer.on("close", on_close)
        
        streamer.connect()
    except Exception as e:
        logger.error(f"❌ Failed to start Upstox WebSocket Streamer: {e}")

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
            
        logger.info(f"📈 Fetching chart data from Upstox: {url}")
        
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
            if streamer and hasattr(streamer, "subscribe"):
                streamer.subscribe(tokens, "ltpc")
            asyncio.create_task(fetch_rest_ltp(tokens))
        return web.json_response({"status": "success", "subscribed": list(SUBSCRIBED_TOKENS)})
    except Exception as e:
        return web.json_response({"status": "error", "message": str(e)}, status=500)

# --- 🏠 HEALTH CHECK ROUTE ---
async def handle_health(request):
    return web.json_response({"status": "ok", "service": "Munh Titan Backend", "time": datetime.datetime.now(IST).isoformat()})

# Setup routes
app.router.add_get('/', handle_health)
app.router.add_get('/health', handle_health)
app.router.add_post('/api/get_chart_data', handle_get_chart_data)
app.router.add_post('/api/subscribe', handle_subscribe)

# --- 🚀 BACKGROUND APP STARTUP ---
async def background_tasks(app):
    global MAIN_EVENT_LOOP
    MAIN_EVENT_LOOP = asyncio.get_running_loop()
    asyncio.create_task(start_upstox_websocket_streamer())

app.on_startup.append(background_tasks)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app, host='0.0.0.0', port=port)
