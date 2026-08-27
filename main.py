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

# Helper function to broadcast live price tick to clients in both formats
async def broadcast_tick(token: str, price: float):
    price_str = f"{price:.2f}"
    LTP_CACHE[token] = price_str
    
    # Broadcast standard key
    await sio.emit("live_data", {"instrument_key": token, "ltp": price_str})
    
    # Also broadcast alternative format (pipe <-> colon) to ensure app catches it
    if "|" in token:
        alt_token = token.replace("|", ":")
        LTP_CACHE[alt_token] = price_str
        await sio.emit("live_data", {"instrument_key": alt_token, "ltp": price_str})
    elif ":" in token:
        alt_token = token.replace(":", "|")
        LTP_CACHE[alt_token] = price_str
        await sio.emit("live_data", {"instrument_key": alt_token, "ltp": price_str})

    if token in ["NSE_INDEX|Nifty 50", "NSE_INDEX:Nifty 50"]:
        LTP_CACHE["NIFTY"] = price_str
        await sio.emit("live_data", {"instrument_key": "NIFTY", "ltp": price_str})
    elif token in ["BSE_INDEX|SENSEX", "BSE_INDEX:SENSEX"]:
        LTP_CACHE["SENSEX"] = price_str
        await sio.emit("live_data", {"instrument_key": "SENSEX", "ltp": price_str})

# --- 🔄 FAST LTP POLLING ENGINE ---
async def start_upstox_ltp_poller():
    logger.info("⚡ Upstox Live LTP Fast Poller Started!")
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

    concurrency_limiter = asyncio.Semaphore(5)

    async def fetch_chunk(session, chunk_tokens):
        # Upstox API accepts comma-separated keys (can use colon or pipe, let's normalize to comma with colon/pipe as required)
        formatted_tokens = [t.replace("|", ":") for t in chunk_tokens]
        keys_param = ",".join(formatted_tokens)
        url = f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={keys_param}"
        
        async with concurrency_limiter:
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        res_data = await resp.json()
                        if res_data.get("status") == "success" and "data" in res_data:
                            data_map = res_data["data"]
                            for key_alias, detail in data_map.items():
                                inst_key = detail.get("instrument_token") or key_alias
                                last_price = float(detail.get("last_price", 0.0))
                                if last_price > 0:
                                    await broadcast_tick(inst_key, last_price)
                    elif resp.status == 429:
                        await asyncio.sleep(3.0) 
            except Exception as e:
                logger.error(f"❌ Error in chunk fetch: {e}")

    async with aiohttp.ClientSession() as session:
        while True:
            start_time = time.time()
            try:
                current_subs = list(SUBSCRIBED_TOKENS)
                if not current_subs:
                    current_subs = ["NSE_INDEX|Nifty 50", "BSE_INDEX|SENSEX"]

                CHUNK_SIZE = 50 
                chunks = [current_subs[i:i + CHUNK_SIZE] for i in range(0, len(current_subs), CHUNK_SIZE)]

                tasks = [fetch_chunk(session, chunk) for chunk in chunks]
                await asyncio.gather(*tasks)

            except Exception as e:
                logger.error(f"❌ Error in Upstox Master Poller: {e}")

            elapsed = time.time() - start_time
            sleep_time = max(1.0 - elapsed, 0.1)
            await asyncio.sleep(sleep_time)

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
                SUBSCRIBED_TOKENS.add(token.replace("|", ":"))
                SUBSCRIBED_TOKENS.add(token.replace(":", "|"))
        
        # Instantly push whatever cached prices we have for these tokens to the client
        if LTP_CACHE:
            await sio.emit('initial_ltps', LTP_CACHE, room=sid)
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
            unit = "minute" 

        MINUTES_MAP = {
            "3MINUTE": 3, "THREE_MINUTE": 3, "3M": 3, "3minute": 3,
            "5MINUTE": 5, "FIVE_MINUTE": 5, "5M": 5, "5minute": 5,
            "10MINUTE": 10, "TEN_MINUTE": 10, "10M": 10, "10minute": 10,
            "15MINUTE": 15, "FIFTEEN_MINUTE": 15, "15M": 15, "15minute": 15,
            "30MINUTE": 30, "THIRTY_MINUTE": 30, "30M": 30, "30minute": 30,
            "60MINUTE": 60, "SIXTY_MINUTE": 60, "1HOUR": 60, "1H": 60, "60M": 60, "60minute": 60
        }
        target_minutes = MINUTES_MAP.get(raw_interval, 1)

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {ACCESS_TOKEN}'
        }

        all_raw_candles = []

        async with aiohttp.ClientSession() as session:
            if unit in ["day", "week", "month"]:
                to_date = datetime.datetime.now(IST).strftime("%Y-%m-%d")
                from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=60)).strftime("%Y-%m-%d")
                hist_url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/{unit}/{to_date}/{from_date}"
                async with session.get(hist_url, headers=headers) as resp_hist:
                    if resp_hist.status == 200:
                        res_hist = await resp_hist.json()
                        if res_hist.get("status") == "success":
                            all_raw_candles.extend(res_hist.get("data", {}).get("candles", []))
            else:
                end_date = datetime.datetime.now(IST)
                start_date = end_date - datetime.timedelta(days=60)
                
                current_chunk_end = end_date
                while current_chunk_end > start_date:
                    current_chunk_start = max(start_date, current_chunk_end - datetime.timedelta(days=7))
                    
                    t_str = current_chunk_end.strftime("%Y-%m-%d")
                    f_str = current_chunk_start.strftime("%Y-%m-%d")
                    
                    hist_url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/1minute/{t_str}/{f_str}"
                    async with session.get(hist_url, headers=headers) as resp_chunk:
                        if resp_chunk.status == 200:
                            res_chunk = await resp_chunk.json()
                            if res_chunk.get("status") == "success":
                                chunk_candles = res_chunk.get("data", {}).get("candles", [])
                                all_raw_candles.extend(chunk_candles)
                    
                    current_chunk_end = current_chunk_start - datetime.timedelta(days=1)
                    await asyncio.sleep(0.05)

        if all_raw_candles:
            seen_times = set()
            unique_candles = []

            for c in all_raw_candles:
                timestamp = c[0]
                if timestamp not in seen_times:
                    seen_times.add(timestamp)
                    unique_candles.append(c)

            unique_candles.sort(key=lambda x: x[0])

            if unit == "minute" and target_minutes > 1:
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

async def start_background_tasks(app):
    asyncio.create_task(start_upstox_ltp_poller())
    logger.info("✅ Upstox Backend Service Initialized.")

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=10000)
