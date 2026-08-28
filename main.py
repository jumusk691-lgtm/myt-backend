import asyncio
import json
import logging
import datetime
import os
import pytz
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
CONNECTED_CLIENTS = set()
MAIN_EVENT_LOOP = None
streamer = None

# Aiohttp App Setup
app = web.Application()

# --- 📡 BROADCAST LIVE TICK & OHLC TO NATIVE WEBSOCKET CLIENTS ---
async def broadcast_tick(token: str, price: float, ohlc_data: dict = None):
    price_str = f"{price:.2f}"
    LTP_CACHE[token] = price_str
    
    payload = {
        "instrument_key": token,
        "ltp": price_str,
        "ohlc": ohlc_data if ohlc_data else {}
    }
    
    # Standardize Index aliases for Android Client mapping
    if "Nifty 50" in token:
        LTP_CACHE["NIFTY"] = price_str
    elif "SENSEX" in token:
        LTP_CACHE["SENSEX"] = price_str

    if CONNECTED_CLIENTS:
        message_str = json.dumps(payload)
        disconnected = set()
        for ws in CONNECTED_CLIENTS:
            try:
                await ws.send_str(message_str)
            except Exception as e:
                logger.error(f"❌ Error sending tick to client: {e}")
                disconnected.add(ws)
        
        for ws in disconnected:
            CONNECTED_CLIENTS.discard(ws)

# --- 🔄 UPSTOX OFFICIAL WEBSOCKET STREAMER (FULL MODE FOR OHLC & CANDLES) ---
async def start_upstox_websocket_streamer():
    global streamer
    logger.info("⚡ Upstox MarketDataStreamerV3 WebSocket Connecting (Full Mode)...")
    
    def on_open():
        logger.info("🟢 Upstox WebSocket Connected Successfully via SDK!")
        if SUBSCRIBED_TOKENS:
            try:
                # Subscribing with 'full' mode so we get LTP + OHLC directly via WS
                streamer.subscribe(list(SUBSCRIBED_TOKENS), "full")
                logger.info(f"📡 Subscribed Tokens on WS Open (Full Mode): {list(SUBSCRIBED_TOKENS)}")
            except Exception as e:
                logger.error(f"❌ Error in streamer subscription: {e}")

    def on_message(message):
        try:
            if isinstance(message, str):
                message = json.loads(message)

            if isinstance(message, dict):
                feeds = message.get("feeds", {})
                for inst_key, details in feeds.items():
                    ltp_val = None
                    ohlc_val = {}
                    
                    # Extracting data from 'full' mode or alternative feed structures
                    if "full" in details:
                        market_full = details["full"].get("market_full", {})
                        ltp_val = market_full.get("ltp")
                        ohlc_val = market_full.get("ohlc", {})
                    elif "market_ff" in details:
                        market_ff = details.get("market_ff", {})
                        ltp_val = market_ff.get("ltp")
                        ohlc_val = market_ff.get("ohlc", {})
                    elif "ff" in details:
                        ff_data = details.get("ff", {}).get("market_ff", {})
                        ltp_val = ff_data.get("ltp")
                        ohlc_val = ff_data.get("ohlc", {})
                    elif "ltpc" in details:
                        ltp_val = details["ltpc"].get("ltp")
                    
                    if ltp_val is not None and MAIN_EVENT_LOOP and MAIN_EVENT_LOOP.is_running():
                        asyncio.run_coroutine_threadsafe(
                            broadcast_tick(inst_key, float(ltp_val), ohlc_val), 
                            MAIN_EVENT_LOOP
                        )
        except Exception as e:
            logger.error(f"❌ Error parsing websocket message: {e}")

    def on_error(error):
        logger.error(f"❌ Upstox Streamer Error: {error}")

    def on_close(close_status_code, close_msg):
        logger.warning(f"⚠️ Upstox Streamer Closed: {close_msg}")

    try:
        api_client = upstox_client.ApiClient(configuration)
        streamer = upstox_client.MarketDataStreamerV3(api_client)

        streamer.on("open", on_open)
        streamer.on("message", on_message)
        streamer.on("error", on_error)
        streamer.on("close", on_close)

        await asyncio.to_thread(streamer.connect)

    except Exception as e:
        logger.error(f"❌ Exception in MarketDataStreamerV3: {e}")
        await asyncio.sleep(5.0)
        asyncio.create_task(start_upstox_websocket_streamer())

# --- 🌐 NATIVE WEBSOCKET HANDLER FOR ANDROID CLIENT ---
async def websocket_handler(request: web.Request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    CONNECTED_CLIENTS.add(ws)
    logger.info("📱 Native WebSocket Android Client Connected")

    # Send initial LTP cache on connection
    if LTP_CACHE:
        try:
            await ws.send_str(json.dumps({"type": "initial_ltps", "data": LTP_CACHE}))
        except Exception as e:
            logger.error(f"❌ Error sending initial LTPs: {e}")

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    tokens_input = data.get("instrumentKeys") or data.get("tokens", [])
                    new_tokens = []
                    
                    for item in tokens_input:
                        if isinstance(item, dict):
                            token = str(item.get("token") or item.get("instrument_key", "")).strip()
                        else:
                            token = str(item).strip()
                            
                        if token:
                            norm_token = token.replace(":", "|")
                            SUBSCRIBED_TOKENS.add(norm_token)
                            new_tokens.append(norm_token)
                            try:
                                if streamer:
                                    streamer.subscribe([norm_token], "full")
                                    logger.info(f"📡 Dynamically Subscribed via Upstox WS (Full Mode): {norm_token}")
                            except Exception as sub_err:
                                logger.error(f"❌ Dynamic WebSocket subscription error for {norm_token}: {sub_err}")

                    logger.info(f"Total Subscribed Upstox Keys Count: {len(SUBSCRIBED_TOKENS)}")
                except Exception as parse_err:
                    logger.error(f"❌ Error parsing client websocket message: {parse_err}")
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.error(f"❌ WebSocket connection closed with exception {ws.exception()}")
    except Exception as e:
        logger.error(f"❌ WebSocket session error: {e}")
    finally:
        CONNECTED_CLIENTS.discard(ws)
        logger.info("📱 Native WebSocket Android Client Disconnected")

    return ws

# --- 🌐 HEALTH CHECK ROUTE ---
async def home_route(request: web.Request):
    return web.json_response({
        "status": True,
        "message": "MUNH Titan Upstox Pure WebSocket & Full Candle Streamer Service is Live!",
        "version": "1.2.0"
    })

app.router.add_get('/', home_route)
app.router.add_get('/ws', websocket_handler)

# --- 🔄 BACKGROUND TASKS ---
async def start_background_tasks(app):
    global MAIN_EVENT_LOOP
    MAIN_EVENT_LOOP = asyncio.get_running_loop()
    asyncio.create_task(start_upstox_websocket_streamer())
    logger.info("✅ Upstox WebSocket Streamer Background Task Started.")

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
