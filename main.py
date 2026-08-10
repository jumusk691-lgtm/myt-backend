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

# Helper function to broadcast live price tick to clients
async def broadcast_tick(token: str, price: float):
    price_str = f"{price:.2f}"
    
    # Store both formats (: and |) in cache for reliable lookups
    pipe_key = token.replace(":", "|")
    colon_key = token.replace("|", ":")
    
    LTP_CACHE[token] = price_str
    LTP_CACHE[pipe_key] = price_str
    LTP_CACHE[colon_key] = price_str
    
    # Send both instrument_key formats to prevent mismatch on Android UI
    payload = {
        "instrument_key": pipe_key,
        "token": pipe_key,
        "ltp": price_str
    }
    await sio.emit("live_data", payload)

    # Standardize Index aliases for Android Client mapping
    if pipe_key in ("NSE_INDEX|Nifty 50", "NSE_INDEX:Nifty 50"):
        LTP_CACHE["NIFTY"] = price_str
        await sio.emit("live_data", {"instrument_key": "NIFTY", "token": "NIFTY", "ltp": price_str})
    elif pipe_key in ("BSE_INDEX|SENSEX", "BSE_INDEX:SENSEX"):
        LTP_CACHE["SENSEX"] = price_str
        await sio.emit("live_data", {"instrument_key": "SENSEX", "token": "SENSEX", "ltp": price_str})

# --- 🔄 FAST LTP POLLING ENGINE (Reliable & Bypass Protobuf Parsing) ---
async def start_upstox_ltp_poller():
    logger.info("⚡ Upstox Live LTP Fast Poller Started!")
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                current_subs = list(SUBSCRIBED_TOKENS)
                if not current_subs:
                    current_subs = ["NSE_INDEX|Nifty 50", "BSE_INDEX|SENSEX"]

                # Convert pipe format to colon format for Upstox HTTP API standards
                formatted_keys = [k.replace("|", ":") for k in current_subs]
                keys_param = ",".join(formatted_keys)
                url = f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={keys_param}"

                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        res_data = await resp.json()
                        if res_data.get("status") == "success" and "data" in res_data:
                            data_map = res_data["data"]
                            for key_alias, detail in data_map.items():
                                last_price = float(detail.get("last_price", 0.0))
                                if last_price > 0:
                                    await broadcast_tick(key_alias, last_price)
                    else:
                        err_txt = await resp.text()
                        logger.error(f"❌ Upstox Quote API HTTP {resp.status}: {err_txt}")

            except Exception as e:
                logger.error(f"❌ Error in Upstox Poller: {e}")

            await asyncio.sleep(1.0) # 1-sec fast update tick

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
                
        logger.info(f"Subscribed Upstox Keys: {SUBSCRIBED_TOKENS}")
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
        unit = str(d.get('interval', "1minute")).strip()

        if not instrument_key:
            return web.json_response({"status": False, "message": "Missing instrument_key"}, status=400)

        # Ensure correct delimiter format for Upstox History API
        formatted_key = instrument_key.replace("|", ":")

        to_date = datetime.datetime.now(IST).strftime("%Y-%m-%d")
        from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=7)).strftime("%Y-%m-%d")

        history_api = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))
        
        api_response = history_api.get_historical_candle_data1(
            instrument_key=formatted_key,
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

app.router.add_get('/', home_route)
app.router.add_post('/api/get_chart_data', fetch_chart_data)

# --- 🔄 BACKGROUND TASKS ---
async def start_background_tasks(app):
    asyncio.create_task(start_upstox_ltp_poller())
    logger.info("✅ Upstox Backend Service Initialized with Poller Engine.")

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
