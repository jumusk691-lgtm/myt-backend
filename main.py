import asyncio
import json
import logging
import datetime
import os
import pytz
import socketio
import aiohttp

# --- Upstox Official SDK ---
import upstox_client
from upstox_client.rest import ApiException

# --- 🕒 TIMEZONE & LOGGING SETUP ---
IST = pytz.timezone('Asia/Kolkata')

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MUNH_TITAN_REAL_DATA")

# --- 🔑 UPSTOX CREDENTIALS ---
API_KEY = "eba0a80f-c907-42fa-a926-6672a120254d"
API_SECRET = os.getenv("UPSTOX_API_SECRET", "cg0pdqyg8t")

# 1-YEAR ANALYTICS ACCESS TOKEN
NEW_ANALYTICS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI2MkFIN0siLCJqdGkiOiI2YTdhMTJlZjk1YjgyYzEzZjc5OWEyMmIiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlzRXh0ZW5kZWQiOnRydWUsImlhdCI6MTc4NjM4NTEzNSwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxODE3OTM1MjAwfQ.0z7HMMUZUwJ6mRkzY3EUE1bB36_i1c7M-6yiNc8clgs"

ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", NEW_ANALYTICS_TOKEN).strip()

configuration = upstox_client.Configuration()
configuration.access_token = ACCESS_TOKEN

# --- 🚀 REAL DATA STORAGE (NO DEMO DATA) ---
LTP_CACHE = {}                 
SUBSCRIBED_TOKENS = set(["NSE_INDEX|Nifty 50", "BSE_INDEX|SENSEX"])
MAIN_EVENT_LOOP = None
streamer = None

# Socket.IO & Aiohttp Setup
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# --- 🌐 FETCH REAL LTP FROM UPSTOX REST API ---
async def fetch_real_ltps_via_rest(tokens_list):
    """
    Fetches 100% real LTPs directly from Upstox REST API 
    so that no demo or dummy data ever appears in the app.
    """
    if not tokens_list:
        return
    
    try:
        formatted_keys = ",".join([t.replace(":", "|") for t in tokens_list])
        url = f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={formatted_keys}"
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {ACCESS_TOKEN}'
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    res_json = await resp.json()
                    if res_json.get("status") == "success":
                        data = res_json.get("data", {})
                        for inst_key, details in data.items():
                            ltp = details.get("last_price")
                            if ltp is not None:
                                price_str = f"{float(ltp):.2f}"
                                LTP_CACHE[inst_key] = price_str
                                
                                # Broadcast real tick to clients
                                await sio.emit("live_data", {"instrument_key": inst_key, "ltp": price_str})
                                
                                # Handle Index aliases for Nifty / Sensex headers
                                if "Nifty 50" in inst_key:
                                    LTP_CACHE["NIFTY"] = price_str
                                    await sio.emit("live_data", {"instrument_key": "NIFTY", "ltp": price_str})
                                elif "SENSEX" in inst_key:
                                    LTP_CACHE["SENSEX"] = price_str
                                    await sio.emit("live_data", {"instrument_key": "SENSEX", "ltp": price_str})
        logger.info(f"📊 Real LTPs fetched via REST for: {tokens_list}")
    except Exception as e:
        logger.error(f"❌ Error fetching real LTP via REST: {e}")

# Helper function to broadcast live price tick from WebSocket
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

# --- 🔄 UPSTOX OFFICIAL WEBSOCKET STREAMER ---
async def start_upstox_websocket_streamer():
    global streamer
    logger.info("⚡ Upstox MarketDataStreamerV3 WebSocket Connecting...")
    
    def on_open():
        logger.info("🟢 Upstox WebSocket Connected Successfully!")
        if SUBSCRIBED_TOKENS:
            try:
                streamer.subscribe(list(SUBSCRIBED_TOKENS), "ltpc")
                logger.info(f"📡 Subscribed Tokens: {list(SUBSCRIBED_TOKENS)}")
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

# --- 🌐 SOCKET.IO HANDLERS ---
@sio.event
async def connect(sid, environ):
    logger.info(f"📱 Android Client Connected: {sid}")
    # Fetch real initial data immediately on connection
    if SUBSCRIBED_TOKENS:
        await fetch_real_ltps_via_rest(list(SUBSCRIBED_TOKENS))
    
    if LTP_CACHE:
        await sio.emit('initial_ltps', LTP_CACHE, room=sid)

async def handle_subscription(sid, data):
    try:
        if isinstance(data, str):
            data = json.loads(data)
            
        tokens_input = data.get("instrumentKeys") or data.get("tokens", [])
        new_tokens_to_fetch = []
        
        for item in tokens_input:
            if isinstance(item, dict):
                token = str(item.get("token") or item.get("instrument_key", "")).strip()
            else:
                token = str(item).strip()
                
            if token:
                norm_token = token.replace(":", "|")
                SUBSCRIBED_TOKENS.add(norm_token)
                new_tokens_to_fetch.append(norm_token)
                try:
                    if streamer:
                        streamer.subscribe([norm_token], "ltpc")
                except Exception as sub_err:
                    logger.error(f"❌ Dynamic subscription error for {norm_token}: {sub_err}")
                
        # Fetch real LTP via REST immediately for newly subscribed tokens
        if new_tokens_to_fetch:
            await fetch_real_ltps_via_rest(new_tokens_to_fetch)
            
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
        "message": "MUNH Titan Real-Data Upstox Backend Service is Live!",
        "version": "1.0.4"
    })

async def fetch_chart_data(request: web.Request):
    try:
        d = await request.json()
        instrument_key = str(d.get('token', '') or d.get('instrument_key', '')).strip()
        raw_interval = str(d.get('interval', "5minute")).strip().upper()

        if not instrument_key:
            return web.json_response({"status": False, "message": "Missing instrument_key", "data": []}, status=400)

        instrument_key = instrument_key.replace(":", "|")

        if raw_interval in ["DAY", "ONE_DAY", "1D"]:
            unit = "day"
        elif raw_interval in ["WEEK", "1W", "1WEEK"]:
            unit = "week"
        elif raw_interval in ["MONTH", "1MON", "1MONTH"]:
            unit = "month"
        else:
            unit = "1minute" 

        MINUTES_MAP = {
            "3MINUTE": 3, "THREE_MINUTE": 3, "3M": 3, "3minute": 3,
            "5MINUTE": 5, "FIVE_MINUTE": 5, "5M": 5, "5minute": 5,
            "10MINUTE": 10, "TEN_MINUTE": 10, "10M": 10, "10minute": 10,
            "15MINUTE": 15, "FIFTEEN_MINUTE": 15, "15M": 15, "15minute": 15,
            "30MINUTE": 30, "THIRTY_MINUTE": 30, "30M": 30, "30minute": 30,
            "60MINUTE": 60, "SIXTY_MINUTE": 60, "1HOUR": 60, "1H": 60, "60M": 60, "60minute": 60
        }
        target_minutes = MINUTES_MAP.get(raw_interval, 1)

        to_date = datetime.datetime.now(IST).strftime("%Y-%m-%d")
        from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=60)).strftime("%Y-%m-%d")

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {ACCESS_TOKEN}'
        }

        all_raw_candles = []

        async with aiohttp.ClientSession() as session:
            # 1️⃣ REAL INTRA-DAY CANDLES FROM UPSTOX
            intraday_url = f"https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/{unit}"
            async with session.get(intraday_url, headers=headers) as resp_intra:
                if resp_intra.status == 200:
                    res_intra = await resp_intra.json()
                    if res_intra.get("status") == "success":
                        intra_candles = res_intra.get("data", {}).get("candles", [])
                        all_raw_candles.extend(intra_candles)

            # 2️⃣ REAL HISTORICAL CANDLES FROM UPSTOX
            hist_url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/{unit}/{to_date}/{from_date}"
            async with session.get(hist_url, headers=headers) as resp_hist:
                if resp_hist.status == 200:
                    res_hist = await resp_hist.json()
                    if res_hist.get("status") == "success":
                        hist_candles = res_hist.get("data", {}).get("candles", [])
                        all_raw_candles.extend(hist_candles)

        if all_raw_candles:
            seen_times = set()
            unique_candles = []

            for c in all_raw_candles:
                timestamp = c[0]
                if timestamp not in seen_times:
                    seen_times.add(timestamp)
                    unique_candles.append(c)

            unique_candles.sort(key=lambda x: x[0])

            # --- 🛠️ RESAMPLE REAL CANDLES FOR HIGHER TIMEFRAMES ---
            if unit == "1minute" and target_minutes > 1:
                resampled = []
                current_agg = None
                
                for c in unique_candles:
                    t_str = c[0]
                    try:
                        dt = datetime.datetime.fromisoformat(str(t_str).replace('Z', '+00:00'))
                    except ValueError:
                        try:
                            dt = datetime.datetime.strptime(str(t_str)[:19], "%Y-%m-%dT%H:%M:%S")
                        except ValueError:
                            resampled.append(c)
                            continue
                    
                    mins_from_open = (dt.hour * 60 + dt.minute) - (9 * 60 + 15)
                    if mins_from_open < 0:
                        mins_from_open = 0
                        
                    block_idx = mins_from_open // target_minutes
                    block_key = (dt.date(), block_idx)
                    
                    if current_agg is None or current_agg['key'] != block_key:
                        if current_agg is not None:
                            resampled.append([
                                current_agg['time'],
                                current_agg['open'],
                                current_agg['high'],
                                current_agg['low'],
                                current_agg['close']
                            ])
                        current_agg = {
                            'key': block_key,
                            'time': t_str,
                            'open': float(c[1]),
                            'high': float(c[2]),
                            'low': float(c[3]),
                            'close': float(c[4])
                        }
                    else:
                        current_agg['high'] = max(current_agg['high'], float(c[2]))
                        current_agg['low'] = min(current_agg['low'], float(c[3]))
                        current_agg['close'] = float(c[4])
                
                if current_agg is not None:
                    resampled.append([
                        current_agg['time'],
                        current_agg['open'],
                        current_agg['high'],
                        current_agg['low'],
                        current_agg['close']
                    ])
                
                unique_candles = resampled

            # --- 🕒 CONVERT REAL TIMESTAMPS TO EPOCH SECONDS ---
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
                        t_val = int(time.time())
                except Exception:
                    t_val = int(time.time())

                formatted_candles.append({
                    "time": t_val,
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4])
                })

            return web.json_response({"status": True, "message": "SUCCESS", "data": formatted_candles})

        return web.json_response({"status": False, "message": "Real historical data unavailable from Upstox", "data": []})

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
    logger.info("✅ Upstox Real-Data Backend Service Initialized.")

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
