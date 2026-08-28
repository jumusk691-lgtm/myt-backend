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

    # Standardize Index aliases for Android Client mapping
    if "Nifty 50" in token:
        LTP_CACHE["NIFTY"] = price_str
        await sio.emit("live_data", {"instrument_key": "NIFTY", "ltp": price_str})
    elif "SENSEX" in token:
        LTP_CACHE["SENSEX"] = price_str
        await sio.emit("live_data", {"instrument_key": "SENSEX", "ltp": price_str})

# --- 🔄 FETCH REST LTP FALLBACK (SO 0.00 NEVER SHOWS) ---
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

# --- 🔄 UPSTOX OFFICIAL WEBSOCKET STREAMER (100% REAL-TIME) ---
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

# --- 🌐 SOCKET.IO HANDLERS FOR ANDROID CLIENT ---
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
                        streamer.subscribe([norm_token], "ltpc")
                        logger.info(f"📡 Dynamically Subscribed via Upstox WS: {norm_token}")
                except Exception as sub_err:
                    logger.error(f"❌ Dynamic WebSocket subscription error for {norm_token}: {sub_err}")
                
        if new_tokens:
            asyncio.create_task(fetch_rest_ltp(new_tokens))

        logger.info(f"Total Subscribed Upstox Keys Count: {len(SUBSCRIBED_TOKENS)}")
    except Exception as e:
        logger.error(f"❌ Error in subscribe tokens handler: {e}")

@sio.event
async def subscribe_tokens(sid, data):
    await handle_subscription(sid, data)

@sio.event
async def subscribe_request(sid, data):
    await handle_subscription(sid, data)

@sio.event
async def disconnect(sid):
    logger.info(f"📱 Android Client Disconnected: {sid}")

# --- 🌐 REST HTTP API ENDPOINTS (UPSTOX V3 API WITH 60-DAY CHUNKING FOR ALL TIMEFRAMES) ---
async def home_route(request: web.Request):
    return web.json_response({
        "status": True,
        "message": "MUNH Titan Upstox WebSocket Streamer Service is Live!",
        "version": "1.0.9"
    })

async def fetch_chart_data(request: web.Request):
    try:
        d = await request.json()
        instrument_key = str(d.get('token', '') or d.get('instrument_key', '')).strip()
        raw_interval = str(d.get('interval', "5minute")).strip().upper()

        if not instrument_key:
            return web.json_response({"status": False, "message": "Missing instrument_key", "data": []}, status=400)

        instrument_key = instrument_key.replace(":", "|")

        # --- 🚀 UPSTOX V3 MAPPING (UNIT & INTERVAL) FOR ALL TIMEFRAMES ---
        unit = "minutes"
        interval = "5"

        if raw_interval in ["1MINUTE", "1M", "1"]:
            unit, interval = "minutes", "1"
        elif raw_interval in ["3MINUTE", "3M", "3"]:
            unit, interval = "minutes", "3"
        elif raw_interval in ["5MINUTE", "5M", "5"]:
            unit, interval = "minutes", "5"
        elif raw_interval in ["10MINUTE", "10M", "10"]:
            unit, interval = "minutes", "10"
        elif raw_interval in ["15MINUTE", "15M", "15"]:
            unit, interval = "minutes", "15"
        elif raw_interval in ["30MINUTE", "30M", "30"]:
            unit, interval = "minutes", "30"
        elif raw_interval in ["60MINUTE", "1HOUR", "1H", "60"]:
            unit, interval = "hours", "1"
        elif raw_interval in ["DAY", "ONE_DAY", "1D"]:
            unit, interval = "days", "1"
        elif raw_interval in ["WEEK", "1W", "1WEEK"]:
            unit, interval = "weeks", "1"
        elif raw_interval in ["MONTH", "1MON", "1MONTH"]:
            unit, interval = "months", "1"

        now_ist = datetime.datetime.now(IST)
        
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {ACCESS_TOKEN}'
        }

        all_raw_candles = []

        async with aiohttp.ClientSession() as session:
            if unit in ["minutes", "hours"]:
                # --- 🧠 SMART CHUNKING: FETCH 60 DAYS IN TWO 30-DAY BLOCKS TO AVOID API LIMITS ---
                date_ranges = [
                    (
                        (now_ist - datetime.timedelta(days=30)).strftime("%Y-%m-%d"),
                        now_ist.strftime("%Y-%m-%d")
                    ),
                    (
                        (now_ist - datetime.timedelta(days=60)).strftime("%Y-%m-%d"),
                        (now_ist - datetime.timedelta(days=31)).strftime("%Y-%m-%d")
                    )
                ]

                for from_d, to_d in date_ranges:
                    v3_url = f"https://api.upstox.com/v3/historical-candle/{instrument_key}/{unit}/{interval}?to_date={to_d}&from_date={from_d}"
                    async with session.get(v3_url, headers=headers) as resp:
                        if resp.status == 200:
                            res_data = await resp.json()
                            if res_data.get("status") == "success":
                                candles = res_data.get("data", {}).get("candles", [])
                                if candles:
                                    all_raw_candles.extend(candles)
            else:
                # --- 📅 FOR DAYS, WEEKS, MONTHS: FETCH FULL 60 DAYS DIRECTLY ---
                to_date = now_ist.strftime("%Y-%m-%d")
                from_date = (now_ist - datetime.timedelta(days=60)).strftime("%Y-%m-%d")
                
                v3_url = f"https://api.upstox.com/v3/historical-candle/{instrument_key}/{unit}/{interval}?to_date={to_date}&from_date={from_date}"
                async with session.get(v3_url, headers=headers) as resp:
                    if resp.status == 200:
                        res_data = await resp.json()
                        if res_data.get("status") == "success":
                            all_raw_candles = res_data.get("data", {}).get("candles", [])

        if all_raw_candles:
            seen_times = set()
            unique_candles = []

            for c in all_raw_candles:
                timestamp = c[0]
                if timestamp not in seen_times:
                    seen_times.add(timestamp)
                    unique_candles.append(c)

            unique_candles.sort(key=lambda x: x[0])

            formatted_candles = []
            for c in unique_candles:
                t_raw = c[0]
                try:
                    if isinstance(t_raw, (int, float)):
                        t_val = int(t_raw)
                    elif isinstance(t_raw, str):
                        dt = datetime.datetime.fromisoformat(t_raw.replace('Z', '+00:00'))
                        t_val = int(dt.timestamp())
                    else:
                        t_val = int(datetime.datetime.now().timestamp())
                except Exception:
                    t_val = int(datetime.datetime.now().timestamp())

                formatted_candles.append({
                    "time": t_val,
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4])
                })

            return web.json_response({"status": True, "message": "SUCCESS", "data": formatted_candles})

        return web.json_response({"status": False, "message": "Real historical data unavailable from Upstox v3", "data": []})

    except Exception as e:
        logger.error(f"Exception in fetch_chart_data: {e}")
        return web.json_response({"status": False, "message": str(e), "data": []}, status=500)

app.router.add_get('/', home_route)
app.router.add_post('/api/get_chart_data', fetch_chart_data)

# --- 🔄 BACKGROUND TASKS ---
async def start_background_tasks(app):
    global MAIN_EVENT_LOOP
    MAIN_EVENT_LOOP = asyncio.get_running_loop()
    asyncio.create_task(start_upstox_websocket_streamer())
    logger.info("✅ Upstox WebSocket Streamer Background Task Started.")

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
