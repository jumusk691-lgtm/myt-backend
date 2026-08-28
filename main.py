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
MAIN_EVENT_LOOP = None
streamer = None

# Socket.IO & Aiohttp Setup
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# REST Fallback fetcher to grab instant index prices via Upstox Market Quote API
async def fetch_initial_index_prices():
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }
    quote_url = "https://api.upstox.com/v2/market-quote/ltp?instrument_key=NSE_INDEX%7CNifty%2050,BSE_INDEX%7CSENSEX"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(quote_url, headers=headers) as resp:
                if resp.status == 200:
                    res_json = await resp.json()
                    if res_json.get("status") == "success":
                        data = res_json.get("data", {})
                        for key, val in data.items():
                            ltp = val.get("last_price")
                            if ltp is not None:
                                price_str = f"{float(ltp):.2f}"
                                # Map both pipe and colon variations so Android client catches them instantly
                                alt_key = key.replace("|", ":")
                                LTP_CACHE[key] = price_str
                                LTP_CACHE[alt_key] = price_str
                                
                                if "Nifty 50" in key:
                                    LTP_CACHE["NIFTY"] = price_str
                                    await sio.emit("live_data", {"instrument_key": "NIFTY", "ltp": price_str})
                                    await sio.emit("live_data", {"instrument_key": key, "ltp": price_str})
                                    await sio.emit("live_data", {"instrument_key": alt_key, "ltp": price_str})
                                elif "SENSEX" in key:
                                    LTP_CACHE["SENSEX"] = price_str
                                    await sio.emit("live_data", {"instrument_key": "SENSEX", "ltp": price_str})
                                    await sio.emit("live_data", {"instrument_key": key, "ltp": price_str})
                                    await sio.emit("live_data", {"instrument_key": alt_key, "ltp": price_str})
                        logger.info(f"📊 Initial REST Indices LTP Fetched & Broadcasted: {LTP_CACHE}")
    except Exception as e:
        logger.error(f"❌ Error fetching REST initial quote: {e}")

# Helper function to broadcast live price tick to clients with multi-format keys
async def broadcast_tick(token: str, price: float):
    price_str = f"{price:.2f}"
    alt_token = token.replace("|", ":") if "|" in token else token.replace(":", "|")
    
    LTP_CACHE[token] = price_str
    LTP_CACHE[alt_token] = price_str
    
    payload = {
        "instrument_key": token,
        "ltp": price_str
    }
    await sio.emit("live_data", payload)
    
    # Also emit with alternate token formatting
    await sio.emit("live_data", {"instrument_key": alt_token, "ltp": price_str})

    # Standardize Index aliases for Android Client mapping
    if "Nifty 50" in token or token == "NIFTY":
        LTP_CACHE["NIFTY"] = price_str
        await sio.emit("live_data", {"instrument_key": "NIFTY", "ltp": price_str})
    elif "SENSEX" in token or token == "SENSEX":
        LTP_CACHE["SENSEX"] = price_str
        await sio.emit("live_data", {"instrument_key": "SENSEX", "ltp": price_str})

# --- 🔄 UPSTOX OFFICIAL SDK MARKET DATA STREAMER V3 ---
async def start_upstox_websocket_streamer():
    global streamer
    logger.info("⚡ Upstox MarketDataStreamerV3 Initializing...")
    
    def on_open():
        logger.info("🟢 Connected to Upstox Market Data Feed V3 WebSocket successfully!")
        if SUBSCRIBED_TOKENS:
            try:
                streamer.subscribe(list(SUBSCRIBED_TOKENS), "ltpc")
                logger.info(f"📡 Initial Subscribed Tokens Sent: {list(SUBSCRIBED_TOKENS)}")
            except Exception as e:
                logger.error(f"❌ Error in initial subscription: {e}")

    def on_message(message):
        try:
            if isinstance(message, str):
                message = json.loads(message)

            if isinstance(message, dict):
                feeds = message.get("feeds", {})
                for inst_key, details in feeds.items():
                    ltp_val = None
                    if "ltpc" in details:
                        ltp_val = details["ltpc"].get("ltp")
                    elif "market_ff" in details:
                        ltp_val = details.get("market_ff", {}).get("ltp")
                    elif "ff" in details:
                        ltp_val = details.get("ff", {}).get("market_ff", {}).get("ltp")
                    
                    if ltp_val is not None and MAIN_EVENT_LOOP and MAIN_EVENT_LOOP.is_running():
                        asyncio.run_coroutine_threadsafe(broadcast_tick(inst_key, float(ltp_val)), MAIN_EVENT_LOOP)
        except Exception as e:
            logger.error(f"❌ Error parsing message: {e}")

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

# --- 🌐 SOCKET.IO HANDLERS ---
@sio.event
async def connect(sid, environ):
    logger.info(f"📱 Android Client Connected: {sid}")
    SUBSCRIBED_TOKENS.add("NSE_INDEX|Nifty 50")
    SUBSCRIBED_TOKENS.add("BSE_INDEX|SENSEX")
    
    # Fetch and push immediate LTP data via REST fallback
    await fetch_initial_index_prices()
    
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
                # Handle both delimiters for Upstox compatibility
                norm_token = token.replace(":", "|")
                SUBSCRIBED_TOKENS.add(norm_token)
                try:
                    if streamer:
                        streamer.subscribe([norm_token], "ltpc")
                except Exception as sub_err:
                    logger.error(f"❌ Dynamic subscription error for {norm_token}: {sub_err}")
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
        "message": "MUNH Titan Upstox Backend Service is Live & Running via MarketDataStreamerV3!",
        "version": "1.0.4"
    })

async def fetch_chart_data(request: web.Request):
    try:
        d = await request.json()
        instrument_key = str(d.get('token', '') or d.get('instrument_key', '')).strip()
        raw_interval = str(d.get('interval', "FIVE_MINUTE")).strip().upper()
        
        limit = int(d.get('limit', 150))
        passed_from = d.get('fromdate')
        passed_to = d.get('todate')

        if not instrument_key:
            return web.json_response({"status": False, "message": "Missing instrument_key"}, status=400)
        
        instrument_key = instrument_key.replace(":", "|")
        
        interval_map = {
            "ONE_MINUTE": "1minute", "1M": "1minute", "1MIN": "1minute",
            "THREE_MINUTE": "3minute", "3M": "3minute", "3MIN": "3minute",
            "FIVE_MINUTE": "5minute", "5M": "5minute", "5MIN": "5minute",
            "TEN_MINUTE": "10minute", "10M": "10minute", "10MIN": "10minute",
            "FIFTEEN_MINUTE": "15minute", "15M": "15minute", "15MIN": "15minute",
            "THIRTY_MINUTE": "30minute", "30M": "30minute", "30MIN": "30minute",
            "ONE_HOUR": "60minute", "1H": "60minute", "60MIN": "60minute",
            "ONE_DAY": "day", "1D": "day", "DAY": "day"
        }
        interval = interval_map.get(raw_interval, "5minute")
        
        dt = datetime.datetime.now(IST)
        to_date = passed_to if passed_to else dt.strftime("%Y-%m-%d")
        
        if passed_from:
            from_date = passed_from.split(" ")[0] if " " in passed_from else passed_from
        else:
            from_date = (dt - datetime.timedelta(days=45)).strftime("%Y-%m-%d")
        
        historical_url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}"
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {ACCESS_TOKEN}'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(historical_url, headers=headers) as resp:
                if resp.status == 200:
                    res_json = await resp.json()
                    status_val = res_json.get("status")
                    if status_val == "success" or status_val is True:
                        candles = res_json.get("data", {}).get("candles", [])
                        candles.reverse()
                        
                        formatted_candles = []
                        for c in candles:
                            if len(c) >= 6:
                                try:
                                    formatted_candles.append([
                                        str(c[0]),
                                        float(c[1]),
                                        float(c[2]),
                                        float(c[3]),
                                        float(c[4]),
                                        float(c[5])
                                    ])
                                except (ValueError, TypeError):
                                    continue

                        if len(formatted_candles) > limit:
                            formatted_candles = formatted_candles[-limit:]

                        return web.json_response({
                            "status": True, 
                            "message": "SUCCESS", 
                            "data": formatted_candles
                        })
                
                resp_text = await resp.text()
                logger.warning(f"⚠️ Upstox Historical API Error [{resp.status}]: {resp_text}")
                return web.json_response({
                    "status": False, 
                    "message": f"Upstox API returned status {resp.status}", 
                    "data": []
                }, status=200)

    except Exception as e:
        logger.error(f"Exception in fetch_chart_data: {e}")
        return web.json_response({
            "status": False, 
            "message": str(e), 
            "data": []
        }, status=200)

app.router.add_get('/', home_route)
app.router.add_post('/api/get_chart_data', fetch_chart_data)

# --- 🔄 BACKGROUND TASKS ---
async def start_background_tasks(app):
    global MAIN_EVENT_LOOP
    MAIN_EVENT_LOOP = asyncio.get_running_loop()
    asyncio.create_task(start_upstox_websocket_streamer())
    asyncio.create_task(fetch_initial_index_prices())
    logger.info("✅ Upstox Backend Service Initialized with Official MarketDataStreamerV3 & REST Fallback.")

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
