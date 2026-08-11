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

# --- 🔄 FAST LTP POLLING ENGINE ---
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

                # Upstox LTP endpoint supports comma separated keys
                keys_param = ",".join(current_subs)
                url = f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={keys_param}"

                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        res_data = await resp.json()
                        if res_data.get("status") == "success" and "data" in res_data:
                            data_map = res_data["data"]
                            for key_alias, detail in data_map.items():
                                inst_key = detail.get("instrument_token") or key_alias.replace(":", "|")
                                last_price = float(detail.get("last_price", 0.0))
                                if last_price > 0:
                                    await broadcast_tick(inst_key, last_price)
                                    if ":" in key_alias:
                                        pipe_key = key_alias.replace(":", "|")
                                        await broadcast_tick(pipe_key, last_price)
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
        
        # 🔍 Read token/instrument_key flexibly
        instrument_key = str(d.get('token', '') or d.get('instrument_key', '') or d.get('symbol', '')).strip()
        
        # 🔍 Read interval/timeframe/resolution flexibly
        raw_interval = str(d.get('interval', '') or d.get('timeframe', '') or d.get('resolution', '') or '5minute').strip()

        if not instrument_key:
            return web.json_response({"status": False, "message": "Missing instrument_key"}, status=400)

        # Pipe formatting for Upstox Key
        instrument_key = instrument_key.replace(":", "|")

        # Map to valid Upstox V2 Supported Candle Units
        INTERVAL_MAP = {
            "1": "1minute", "1M": "1minute", "1MIN": "1minute", "1MINUTE": "1minute",
            "3": "3minute", "3M": "3minute", "3MIN": "3minute", "3MINUTE": "3minute",
            "5": "5minute", "5M": "5minute", "5MIN": "5minute", "5MINUTE": "5minute",
            "10": "5minute", "10M": "5minute", "10MIN": "5minute", "10MINUTE": "5minute",
            "15": "15minute", "15M": "15minute", "15MIN": "15minute", "15MINUTE": "15minute",
            "30": "30minute", "30M": "30minute", "30MIN": "30minute", "30MINUTE": "30minute",
            "60": "30minute", "60M": "30minute", "1H": "30minute", "1HOUR": "30minute", "60MINUTE": "30minute",
            "D": "day", "1D": "day", "DAY": "day", "ONEDAY": "day"
        }

        unit = INTERVAL_MAP.get(raw_interval.upper(), "5minute")

        now_ist = datetime.datetime.now(IST)
        to_date = now_ist.strftime("%Y-%m-%d")
        from_date = (now_ist - datetime.timedelta(days=7)).strftime("%Y-%m-%d")

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {ACCESS_TOKEN}'
        }

        all_raw_candles = []

        async with aiohttp.ClientSession() as session:
            # 1️⃣ FETCH INTRADAY CANDLES (Today's live candles)
            intraday_url = f"https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/{unit}"
            async with session.get(intraday_url, headers=headers) as resp_intra:
                if resp_intra.status == 200:
                    res_intra = await resp_intra.json()
                    if res_intra.get("status") == "success":
                        intra_candles = res_intra.get("data", {}).get("candles", [])
                        all_raw_candles.extend(intra_candles)

            # 2️⃣ FETCH HISTORICAL CANDLES (Past days)
            hist_url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/{unit}/{to_date}/{from_date}"
            async with session.get(hist_url, headers=headers) as resp_hist:
                if resp_hist.status == 200:
                    res_hist = await resp_hist.json()
                    if res_hist.get("status") == "success":
                        hist_candles = res_hist.get("data", {}).get("candles", [])
                        all_raw_candles.extend(hist_candles)

        if all_raw_candles:
            # Unique mapping by Timestamp
            candle_dict = {}
            for c in all_raw_candles:
                t_str = c[0]
                if t_str not in candle_dict:
                    candle_dict[t_str] = {
                        "time": t_str,
                        "open": float(c[1]),
                        "high": float(c[2]),
                        "low": float(c[3]),
                        "close": float(c[4])
                    }

            # Sort chronologically by ISO String time
            formatted_candles = sorted(candle_dict.values(), key=lambda x: x["time"])

            return web.json_response({"status": True, "message": "SUCCESS", "data": formatted_candles})

        return web.json_response({"status": False, "message": "Historical data unavailable", "data": []})

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
