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
upstox_ws_client = None

# Socket.IO & Aiohttp Setup
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# Helper function to broadcast live price tick to clients
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

# --- 🌐 UPSTOX WEBSOCKET STREAM CONNECTOR ---
def start_upstox_websocket():
    try:
        import upstox_client
        from upstox_client.feeder import UpstoxFeeder

        def on_open():
            logger.info("✅ Upstox Market Feed WebSocket Connected Successfully!")
            if SUBSCRIBED_TOKENS:
                upstox_ws_client.subscribe(list(SUBSCRIBED_TOKENS), mode="full")

        def on_message(message):
            try:
                # Parse incoming market feed message
                if isinstance(message, dict):
                    feeds = message.get("feeds", {})
                    for instrument_key, data in feeds.items():
                        ltp = data.get("ff", {}).get("marketFF", {}).get("ltp")
                        if ltp:
                            asyncio.run_coroutine_threadsafe(broadcast_tick(instrument_key, float(ltp)), main_loop)
            except Exception as e:
                logger.error(f"❌ WS Message Parse Error: {e}")

        def on_error(error):
            logger.error(f"❌ Upstox WS Error: {error}")

        def on_close(code, reason):
            logger.warning(f"⚠️ Upstox WS Closed: {reason} (Code: {code})")

        global upstox_ws_client
        upstox_ws_client = UpstoxFeeder(
            access_token=ACCESS_TOKEN,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        upstox_ws_client.connect()
    except Exception as e:
        logger.error(f"❌ Failed to start Upstox WebSocket: {e}")

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
        new_tokens = []
        for item in tokens_input:
            if isinstance(item, dict):
                token = str(item.get("token") or item.get("instrument_key", "")).strip()
            else:
                token = str(item).strip()
                
            if token:
                token = token.replace(":", "|")
                SUBSCRIBED_TOKENS.add(token)
                new_tokens.append(token)
                
        # Dynamically subscribe new tokens to Upstox WS if active
        global upstox_ws_client
        if upstox_ws_client and new_tokens:
            try:
                upstox_ws_client.subscribe(new_tokens, mode="full")
            except Exception:
                pass
                
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
        "message": "MUNH Titan Upstox Backend Service is Live & Running!",
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
        from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=7)).strftime("%Y-%m-%d")

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {ACCESS_TOKEN}'
        }

        all_raw_candles = []

        async with aiohttp.ClientSession() as session:
            intraday_url = f"https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/{unit}"
            async with session.get(intraday_url, headers=headers) as resp_intra:
                if resp_intra.status == 200:
                    res_intra = await resp_intra.json()
                    if res_intra.get("status") == "success":
                        intra_candles = res_intra.get("data", {}).get("candles", [])
                        all_raw_candles.extend(intra_candles)

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

            if unit == "1minute" and target_minutes > 1:
                resampled = []
                current_agg = None
                
                for c in unique_candles:
                    t_str = c[0]
                    try:
                        dt = datetime.datetime.fromisoformat(t_str)
                    except ValueError:
                        try:
                            dt = datetime.datetime.strptime(t_str[:19], "%Y-%m-%dT%H:%M:%S")
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

            formatted_candles = [
                {
                    "time": c[0],
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4])
                }
                for c in unique_candles
            ]

            return web.json_response({"status": True, "message": "SUCCESS", "data": formatted_candles})

        return web.json_response({"status": False, "message": "Historical data unavailable", "data": []})

    except Exception as e:
        logger.error(f"Exception in fetch_chart_data: {e}")
        return web.json_response({"status": False, "message": str(e), "data": []}, status=500)

app.router.add_get('/', home_route)
app.router.add_post('/api/get_chart_data', fetch_chart_data)

# --- 🔄 BACKGROUND TASKS ---
main_loop = None

async def start_background_tasks(app):
    global main_loop
    main_loop = asyncio.get_running_loop()
    # Start Upstox WebSocket in a separate background thread to avoid blocking aiohttp event loop
    threading.Thread(target=start_upstox_websocket, daemon=True).start()
    logger.info("✅ Upstox Backend Service Initialized with Official WebSocket Feed.")

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
