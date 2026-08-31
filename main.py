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

# --- 🚀 REAL WEBSOCKET DATA & LIVE CANDLE STORAGE ---
LTP_CACHE = {}                 
LIVE_CANDLES_CACHE = {}        # Real-time candle builder cache from websocket ticks
SUBSCRIBED_TOKENS = set(["NSE_INDEX|Nifty 50", "BSE_INDEX|SENSEX", "MCX_FO|495213", "MCX_FO|563946"])
CONNECTED_CLIENTS = set()
MAIN_EVENT_LOOP = None
streamer = None

# Aiohttp App Setup
app = web.Application()

# --- 🕒 MARKET STATUS CHECKER (ON / OFF) ---
def is_market_open() -> bool:
    now = datetime.datetime.now(IST)
    # Weekend check (Saturday = 5, Sunday = 6)
    if now.weekday() >= 5:
        return False
    
    market_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_end = now.replace(hour=23, minute=30, second=0, microsecond=0) # MCX stays open till late, general check
    
    return market_start <= now <= market_end

# --- 📡 BROADCAST LIVE TICK & REAL-TIME CANDLE TO NATIVE WEBSOCKET CLIENTS ---
async def broadcast_tick(token: str, price: float):
    price_str = f"{price:.2f}"
    LTP_CACHE[token] = price_str
    
    # Real-time 1-minute Candle Aggregation from WebSocket Ticks
    now_dt = datetime.datetime.now(IST)
    current_minute_key = now_dt.strftime("%Y-%m-%d %H:%M")
    
    if token not in LIVE_CANDLES_CACHE:
        LIVE_CANDLES_CACHE[token] = {}
        
    if current_minute_key not in LIVE_CANDLES_CACHE[token]:
        LIVE_CANDLES_CACHE[token][current_minute_key] = {
            "time": int(now_dt.timestamp()),
            "open": price,
            "high": price,
            "low": price,
            "close": price
        }
    else:
        candle = LIVE_CANDLES_CACHE[token][current_minute_key]
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price

    current_candle = LIVE_CANDLES_CACHE[token][current_minute_key]

    payload = {
        "instrument_key": token,
        "ltp": price_str,
        "live_candle": {
            "time": current_candle["time"],
            "open": f"{current_candle['open']:.2f}",
            "high": f"{current_candle['high']:.2f}",
            "low": f"{current_candle['low']:.2f}",
            "close": f"{current_candle['close']:.2f}"
        }
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
                                    streamer.subscribe([norm_token], "ltpc")
                                    logger.info(f"📡 Dynamically Subscribed via Upstox WS: {norm_token}")
                            except Exception as sub_err:
                                # Handled gracefully if streamer connection is still initializing
                                logger.info(f"ℹ️ Subscription queued/deferred for {norm_token} until WS open.")
                    
                    # If streamer is open, ensure all tokens are synced
                    if streamer and new_tokens:
                        try:
                            streamer.subscribe(list(SUBSCRIBED_TOKENS), "ltpc")
                        except Exception:
                            pass

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

# --- 🌐 REST HTTP API ENDPOINTS (STATUS & CHART HISTORICAL DATA) ---
async def home_route(request: web.Request):
    market_status = is_market_open()
    
    if market_status:
        active_mode = "100% Pure WebSocket Mode (Market is OPEN)"
    else:
        active_mode = "REST API v2 / Historical Mode (Market is CLOSED)"

    return web.json_response({
        "status": True,
        "message": "MUNH Titan Upstox Streamer Service is Live!",
        "market_is_open": market_status,
        "active_stream_mode": active_mode,
        "subscribed_tokens_count": len(SUBSCRIBED_TOKENS),
        "connected_android_clients": len(CONNECTED_CLIENTS),
        "version": "1.3.1"
    })

async def fetch_chart_data(request: web.Request):
    try:
        d = await request.json()
        instrument_key = str(d.get('token', '') or d.get('instrument_key', '')).strip()
        raw_interval = str(d.get('interval', "FIVE_MINUTE")).strip().upper()

        if not instrument_key:
            return web.json_response({"status": False, "message": "Missing instrument_key", "data": []}, status=400)

        instrument_key = instrument_key.replace(":", "|")

        if raw_interval in ["DAY", "ONE_DAY", "1D"]:
            unit = "day"
            target_minutes = 1440
        elif raw_interval in ["THIRTY_MINUTE", "30M", "30MIN", "30minute"]:
            unit = "30minute"
            target_minutes = 30
        else:
            unit = "1minute"
            MINUTES_MAP = {
                "ONE_MINUTE": 1, "1M": 1, "1MIN": 1, "1minute": 1,
                "THREE_MINUTE": 3, "3M": 3, "3MIN": 3, "3minute": 3,
                "FIVE_MINUTE": 5, "5M": 5, "5MIN": 5, "5minute": 5,
                "TEN_MINUTE": 10, "10M": 10, "10MIN": 10, "10minute": 10,
                "FIFTEEN_MINUTE": 15, "15M": 15, "15MIN": 15, "15minute": 15,
                "ONE_HOUR": 60, "1H": 60, "60M": 60, "60minute": 60
            }
            target_minutes = MINUTES_MAP.get(raw_interval, 5)

        to_date = datetime.datetime.now(IST).strftime("%Y-%m-%d")
        from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {ACCESS_TOKEN}'
        }

        all_raw_candles = []

        async with aiohttp.ClientSession() as session:
            hist_url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/{unit}/{to_date}/{from_date}"
            async with session.get(hist_url, headers=headers) as resp_hist:
                if resp_hist.status == 200:
                    res_hist = await resp_hist.json()
                    if res_hist.get("status") == "success":
                        candles = res_hist.get("data", {}).get("candles", [])
                        all_raw_candles.extend(candles)

        if all_raw_candles:
            seen_times = set()
            unique_candles = []
            for c in all_raw_candles:
                timestamp = c[0]
                if timestamp not in seen_times:
                    seen_times.add(timestamp)
                    unique_candles.append(c)

            unique_candles.sort(key=lambda x: x[0])

            if unit == "1minute" and target_minutes > 1:
                resampled = []
                current_agg = None
                
                for c in unique_candles:
                    t_str = c[0]
                    try:
                        if isinstance(t_str, str):
                            dt = datetime.datetime.fromisoformat(t_str.replace('Z', '+00:00'))
                        else:
                            dt = datetime.datetime.fromtimestamp(int(t_str), IST)
                    except Exception:
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

            # Append live building candles from websocket cache if available
            if instrument_key in LIVE_CANDLES_CACHE:
                for k_min, c_val in LIVE_CANDLES_CACHE[instrument_key].items():
                    formatted_candles.append({
                        "time": c_val["time"],
                        "open": c_val["open"],
                        "high": c_val["high"],
                        "low": c_val["low"],
                        "close": c_val["close"]
                    })

            return web.json_response({"status": True, "message": "SUCCESS", "data": formatted_candles})

        return web.json_response({"status": False, "message": "Historical data unavailable from Upstox", "data": []})

    except Exception as e:
        logger.error(f"Exception in fetch_chart_data: {e}")
        return web.json_response({"status": False, "message": str(e), "data": []}, status=500)

app.router.add_get('/', home_route)
app.router.add_get('/ws', websocket_handler)
app.router.add_post('/api/get_chart_data', fetch_chart_data)

# --- 🔄 BACKGROUND TASKS ---
async def start_background_tasks(app):
    global MAIN_EVENT_LOOP
    MAIN_EVENT_LOOP = asyncio.get_running_loop()
    
    asyncio.create_task(start_upstox_websocket_streamer())
    logger.info("✅ Upstox Background Task Initialized.")

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
