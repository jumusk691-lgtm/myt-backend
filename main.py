import asyncio
import json
import logging
import time
import datetime
import threading
import pytz
import socketio
import ssl
import websockets
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
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI2MkFIN0siLCJqdGkiOiI2YTc5ZTg5MTk4MWM2YjA3ZDIxNTcxY2IiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc4NjM3NDI4OSwiaXNzIjoidWRhap1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODYzOTkyMDB9.hvPeurdKyWsaCDE6auP9TR20QF6dxVkO0NdwMEWXMTI"

# Configuration Setup
configuration = upstox_client.Configuration()
configuration.access_token = ACCESS_TOKEN

# --- 🚀 GLOBAL STATES & SCORE TRACKING ---
LTP_CACHE = {}               
SUBSCRIBED_TOKENS = set()
USER_SCORE = 0 

# --- 📊 CANDLE ENGINE CONFIGURATION ---
TIMEFRAMES = {
    "1M": 60,
    "3M": 180,
    "5M": 300,
    "15M": 900,
    "30M": 1800,
    "1H": 3600,
    "1D": 86400
}
MAX_CANDLES_LIMIT = 200
CANDLE_CACHE = {}

main_loop = None

# Socket.IO & Aiohttp Setup
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
app = web.Application()
sio.attach(app)

# --- 🎯 SCORE LOGIC ---
def update_user_score(points=1):
    global USER_SCORE
    USER_SCORE += points
    state.score = USER_SCORE
    logger.info(f"📊 Current User Score: {USER_SCORE}")
    return USER_SCORE

# --- 📈 LIVE TICK CANDLE AGGREGATOR ---
def update_token_candles(token_str, price_val):
    if price_val <= 0:
        return

    now_sec = int(time.time())
    if token_str not in CANDLE_CACHE:
        CANDLE_CACHE[token_str] = {}

    token_candles = CANDLE_CACHE[token_str]

    for tf_key, interval_sec in TIMEFRAMES.items():
        if tf_key not in token_candles:
            token_candles[tf_key] = []
            
        tf_list = token_candles[tf_key]
        bucket_time = (now_sec // interval_sec) * interval_sec

        if not tf_list:
            token_candles[tf_key].append({
                "time": bucket_time, "open": price_val,
                "high": price_val, "low": price_val, "close": price_val
            })
        else:
            last_candle = tf_list[-1]
            if bucket_time > last_candle["time"]:
                token_candles[tf_key].append({
                    "time": bucket_time, "open": last_candle["close"],
                    "high": max(last_candle["close"], price_val),
                    "low": min(last_candle["close"], price_val),
                    "close": price_val
                })
                if len(tf_list) > MAX_CANDLES_LIMIT:
                    tf_list.pop(0)
            else:
                last_candle["close"] = price_val
                last_candle["high"] = max(last_candle["high"], price_val)
                last_candle["low"] = min(last_candle["low"], price_val)

# Helper function to broadcast live price tick to clients
async def broadcast_tick(token: str, price: float):
    price_str = f"{price:.2f}"
    LTP_CACHE[token] = price_str
    update_token_candles(token, price)
    
    payload = {
        "instrument_key": token,
        "ltp": price_str
    }
    await sio.emit("live_data", payload)

# --- 🌐 UPSTOX LIVE WEBSOCKET STREAMING ENGINE ---
async def get_upstox_authorized_ws_url():
    try:
        api_instance = upstox_client.WebsocketApi(upstox_client.ApiClient(configuration))
        api_response = api_instance.get_market_data_feed_authorize("2.0")
        if api_response and api_response.data:
            return api_response.data.authorized_redirect_uri
    except Exception as e:
        logger.error(f"❌ Upstox WS Auth URL Error: {e}")
    return None

async def start_upstox_feed_stream():
    await asyncio.sleep(2)
    while True:
        try:
            ws_url = await get_upstox_authorized_ws_url()
            if not ws_url:
                logger.warning("⚠️ Retrying Upstox WS Auth URL in 5 seconds...")
                await asyncio.sleep(5)
                continue

            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            logger.info("🔌 Connecting to Upstox Market Data Feed...")
            async with websockets.connect(ws_url, ssl=ssl_context) as ws:
                logger.info("⚡ Upstox Feed Connected Successfully!")
                
                # Active Subscription Loop
                async def subscription_heartbeat():
                    last_sub_set = set()
                    while ws.open:
                        current_subs = set(SUBSCRIBED_TOKENS)
                        # Always include NIFTY and SENSEX
                        current_subs.add("NSE_INDEX|Nifty 50")
                        current_subs.add("BSE_INDEX|SENSEX")

                        if current_subs != last_sub_set:
                            sub_payload = {
                                "guid": "upstox_live_sub",
                                "method": "sub",
                                "data": {
                                    "mode": "ltpc",
                                    "instrumentKeys": list(current_subs)
                                }
                            }
                            await ws.send(json.dumps(sub_payload).encode('utf-8'))
                            last_sub_set = current_subs
                            logger.info(f"📡 Subscribed Upstox Keys: {current_subs}")
                        await asyncio.sleep(1)

                sub_task = asyncio.create_task(subscription_heartbeat())

                try:
                    async for message in ws:
                        # Decode JSON / Protobuf Text from Upstox
                        try:
                            if isinstance(message, bytes):
                                data = json.loads(message.decode('utf-8'))
                            else:
                                data = json.loads(message)

                            feeds = data.get("feeds", {})
                            for inst_key, feed_info in feeds.items():
                                ltp = 0.0
                                if "ltpc" in feed_info and "ltp" in feed_info["ltpc"]:
                                    ltp = float(feed_info["ltpc"]["ltp"])
                                elif "ff" in feed_info and "marketFF" in feed_info["ff"]:
                                    ltp = float(feed_info["ff"]["marketFF"]["ltpc"]["ltp"])
                                
                                if ltp > 0:
                                    await broadcast_tick(inst_key, ltp)

                        except Exception:
                            # Direct binary / text fallback parsing
                            pass

                except Exception as e:
                    logger.error(f"❌ Stream Reader Error: {e}")
                finally:
                    sub_task.cancel()

        except Exception as e:
            logger.error(f"❌ Upstox Connection Error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

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
                
        logger.info(f"Subscribed to Upstox Instruments: {SUBSCRIBED_TOKENS}")
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

# --- 🌐 REST HTTP API ENDPOINTS (UPSTOX HISTORICAL DATA) ---
async def fetch_chart_data(request: web.Request):
    try:
        d = await request.json()
        instrument_key = str(d.get('token', '')).strip()  # e.g., NSE_EQ|INE009A01021
        unit = str(d.get('interval', "1minute")).strip()  # day, 1minute, 3minute, etc.

        if not instrument_key:
            return web.json_response({"status": False, "message": "Missing instrument_key"}, status=400)

        to_date = datetime.datetime.now(IST).strftime("%Y-%m-%d")
        from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=7)).strftime("%Y-%m-%d")

        history_api = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))
        
        # Upstox Historical Candle Call
        api_response = history_api.get_historical_candle_data1(
            instrument_key=instrument_key,
            interval=unit,
            to_date=to_date,
            from_date=from_date,
            api_version="2.0"
        )

        if api_response and api_response.status == "success":
            raw_candles = api_response.data.candles
            formatted_candles = [
                {
                    "time": c[0],
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4])
                }
                for c in raw_candles
            ]
            return web.json_response({"status": True, "message": "SUCCESS", "data": formatted_candles})

        return web.json_response({"status": False, "message": "Historical data unavailable", "data": []})

    except ApiException as e:
        logger.error(f"Upstox API Exception: {e}")
        return web.json_response({"status": False, "message": str(e), "data": []}, status=500)
    except Exception as e:
        logger.error(f"Exception in fetch_chart_data: {e}")
        return web.json_response({"status": False, "message": str(e), "data": []}, status=500)

app.router.add_post('/api/get_chart_data', fetch_chart_data)

# --- 🔄 BACKGROUND TASKS ---
async def start_background_tasks(app):
    global main_loop
    main_loop = asyncio.get_event_loop()
    asyncio.create_task(start_upstox_feed_stream())
    logger.info("✅ Upstox Backend Service Initialized.")

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
