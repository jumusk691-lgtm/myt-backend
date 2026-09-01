import asyncio
import json
import logging
import datetime
import os
import pytz
import urllib.parse
import aiohttp
import pyotp
from aiohttp import web
from playwright.async_api import async_playwright

# --- Upstox Official SDK ---
import upstox_client
from upstox_client.rest import ApiException

# --- 🕒 TIMEZONE & LOGGING SETUP ---
IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MUNH_TITAN_WEBSOCKET")

# --- 🔑 UPSTOX CREDENTIALS ---
API_KEY = os.getenv("UPSTOX_API_KEY", "eba0a80f-c907-42fa-a926-6672a120254d")
API_SECRET = os.getenv("UPSTOX_API_SECRET", "cg0pdqyg8t")
REDIRECT_URI = os.getenv("UPSTOX_REDIRECT_URI", "https://myt-backend-1.onrender.com/callback")

MOBILE_NO = os.getenv("UPSTOX_MOBILE_NO", "7735493540")
PIN = os.getenv("UPSTOX_PIN", "865895")
TOTP_KEY = os.getenv("UPSTOX_TOTP_KEY", "TOB3BGAEHGQADCIBT64GE4UT3Q7UX3BB")

ACCESS_TOKEN = ""

configuration = upstox_client.Configuration()

# --- 🚀 DATA CACHE ---
LTP_CACHE = {}                 
LIVE_CANDLES_CACHE = {}        
LAST_BROADCAST_TIME = {}       
SUBSCRIBED_TOKENS = set(["NSE_INDEX|Nifty 50", "BSE_INDEX|SENSEX", "MCX_FO|495213", "MCX_FO|563946"])
CONNECTED_CLIENTS = set()
MAIN_EVENT_LOOP = None
streamer = None

app = web.Application()

# --- 🤖 100% FULLY AUTOMATED LOGIN VIA PLAYWRIGHT ---
async def auto_login_and_get_token():
    global ACCESS_TOKEN, configuration
    logger.info("🤖 Starting 100% Fully Automated Upstox Login Flow...")
    
    auth_url = (
        f"https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code&client_id={API_KEY}&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
    )

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-infobars"
                ]
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={'width': 1366, 'height': 768}
            )
            
            # Hide webdriver flag to bypass Upstox Anti-Bot
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            page = await context.new_page()
            await page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})
            await page.goto(auth_url, wait_until="networkidle", timeout=60000)
            await asyncio.sleep(3)

            # 1. Fill Mobile Number
            await page.wait_for_selector("input[type='tel'], #mobileNum, input[name='mobileNumber']", timeout=30000)
            await page.fill("input[type='tel'], #mobileNum, input[name='mobileNumber']", MOBILE_NO)
            await page.click("button:has-text('Get OTP'), button[type='submit']")

            # 2. Fill TOTP
            await asyncio.sleep(2)
            totp_code = pyotp.TOTP(TOTP_KEY).now()
            logger.info(f"🔑 Generated TOTP: {totp_code}")
            
            await page.wait_for_selector("input[type='text'], #otpNum, input[name='otp']", timeout=30000)
            await page.fill("input[type='text'], #otpNum, input[name='otp']", totp_code)
            await page.click("button:has-text('Continue'), button[type='submit']")

            # 3. Fill PIN
            await asyncio.sleep(2)
            await page.wait_for_selector("input[type='password'], #pinCode, input[name='pin']", timeout=30000)
            await page.fill("input[type='password'], #pinCode, input[name='pin']", PIN)
            await page.click("button:has-text('Continue'), button[type='submit']")

            # 4. Wait for redirect code
            await page.wait_for_url(f"*{REDIRECT_URI}*", timeout=40000)
            final_url = page.url
            await browser.close()

            parsed = urllib.parse.urlparse(final_url)
            code = urllib.parse.parse_qs(parsed.query).get('code', [None])[0]

            if not code:
                logger.error("❌ Failed to capture Auth Code from Redirect URL")
                return False

            # 5. Exchange Auth Code for Access Token
            token_url = "https://api.upstox.com/v2/login/authorization/token"
            headers = {"accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
            payload = {
                "code": code,
                "client_id": API_KEY,
                "client_secret": API_SECRET,
                "redirect_uri": REDIRECT_URI,
                "grant_type": "authorization_code"
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(token_url, headers=headers, data=payload) as resp:
                    res = await resp.json()
                    if resp.status == 200 and "access_token" in res:
                        ACCESS_TOKEN = res["access_token"]
                        configuration.access_token = ACCESS_TOKEN
                        logger.info("🎉 SUCCESS: Access Token Automatically Generated & Updated!")
                        return True
                    else:
                        logger.error(f"❌ Token Exchange Failed: {res}")
                        return False

    except Exception as e:
        logger.error(f"❌ Auto-Login Exception: {e}")
        return False

def is_market_open() -> bool:
    now = datetime.datetime.now(IST)
    if now.weekday() >= 5:
        return False
    market_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_end = now.replace(hour=23, minute=30, second=0, microsecond=0)
    return market_start <= now <= market_end

async def broadcast_tick(token: str, price: float):
    price_str = f"{price:.2f}"
    LTP_CACHE[token] = price_str
    
    now_dt = datetime.datetime.now(IST)
    now_ts = now_dt.timestamp()
    current_minute_key = now_dt.strftime("%Y-%m-%d %H:%M")
    
    if token not in LIVE_CANDLES_CACHE:
        LIVE_CANDLES_CACHE[token] = {}
        
    if current_minute_key not in LIVE_CANDLES_CACHE[token]:
        LIVE_CANDLES_CACHE[token][current_minute_key] = {
            "time": int(now_dt.timestamp()),
            "open": price, "high": price, "low": price, "close": price
        }
    else:
        candle = LIVE_CANDLES_CACHE[token][current_minute_key]
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price

    if "Nifty 50" in token:
        LTP_CACHE["NIFTY"] = price_str
    elif "SENSEX" in token:
        LTP_CACHE["SENSEX"] = price_str

    if now_ts - LAST_BROADCAST_TIME.get(token, 0) < 1.0:
        return
        
    LAST_BROADCAST_TIME[token] = now_ts
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

    if CONNECTED_CLIENTS:
        message_str = json.dumps(payload)
        disconnected = set()
        for ws in CONNECTED_CLIENTS:
            try:
                await ws.send_str(message_str)
            except Exception:
                disconnected.add(ws)
        for ws in disconnected:
            CONNECTED_CLIENTS.discard(ws)

async def start_upstox_websocket_streamer():
    global streamer
    logger.info("⚡ Upstox MarketDataStreamerV3 WebSocket Connecting...")
    
    def on_open():
        logger.info("🟢 Upstox WebSocket Connected Successfully via SDK!")
        if SUBSCRIBED_TOKENS:
            try:
                streamer.subscribe(list(SUBSCRIBED_TOKENS), "ltpc")
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

async def websocket_handler(request: web.Request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    CONNECTED_CLIENTS.add(ws)

    if LTP_CACHE:
        try:
            await ws.send_str(json.dumps({"type": "initial_ltps", "data": LTP_CACHE}))
        except Exception:
            pass

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(msg.data)
                tokens_input = data.get("instrumentKeys") or data.get("tokens", [])
                for item in tokens_input:
                    token = str(item.get("token") or item if isinstance(item, dict) else item).strip().replace(":", "|")
                    if token:
                        SUBSCRIBED_TOKENS.add(token)
                        if streamer:
                            try:
                                streamer.subscribe([token], "ltpc")
                            except Exception:
                                pass
    finally:
        CONNECTED_CLIENTS.discard(ws)
    return ws

async def home_route(request: web.Request):
    return web.json_response({
        "status": True,
        "message": "MUNH Titan Upstox Streamer Service Live!",
        "access_token_active": bool(ACCESS_TOKEN),
        "market_is_open": is_market_open(),
        "subscribed_tokens_count": len(SUBSCRIBED_TOKENS),
        "connected_clients": len(CONNECTED_CLIENTS),
        "version": "2.0.0-fully-automated"
    })

async def fetch_chart_data(request: web.Request):
    try:
        d = await request.json()
        instrument_key = str(d.get('token', '') or d.get('instrument_key', '')).strip().replace(":", "|")
        if not instrument_key:
            return web.json_response({"status": False, "message": "Missing instrument_key"}, status=400)

        raw_interval = str(d.get('interval', "FIVE_MINUTE")).strip().upper()
        unit = "day" if raw_interval in ["DAY", "ONE_DAY", "1D"] else ("30minute" if raw_interval in ["THIRTY_MINUTE", "30M"] else "1minute")

        to_date = datetime.datetime.now(IST).strftime("%Y-%m-%d")
        from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

        headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}
        all_raw_candles = []

        async with aiohttp.ClientSession() as session:
            intra_url = f"https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/{unit}"
            async with session.get(intra_url, headers=headers) as resp:
                if resp.status == 200:
                    res = await resp.json()
                    all_raw_candles.extend(res.get("data", {}).get("candles", []))

            hist_url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/{unit}/{to_date}/{from_date}"
            async with session.get(hist_url, headers=headers) as resp:
                if resp.status == 200:
                    res = await resp.json()
                    all_raw_candles.extend(res.get("data", {}).get("candles", []))

        formatted_candles = []
        for c in all_raw_candles:
            formatted_candles.append({"time": c[0], "open": float(c[1]), "high": float(c[2]), "low": float(c[3]), "close": float(c[4])})

        return web.json_response({"status": True, "message": "SUCCESS", "data": formatted_candles})

    except Exception as e:
        return web.json_response({"status": False, "message": str(e)}, status=500)

app.router.add_get('/', home_route)
app.router.add_get('/ws', websocket_handler)
app.router.add_post('/api/get_chart_data', fetch_chart_data)

# --- 🔄 AUTOMATED STARTUP FLOW ---
async def run_login_flow():
    success = await auto_login_and_get_token()
    if success:
        asyncio.create_task(start_upstox_websocket_streamer())
    else:
        logger.error("❌ Auto-login failed on startup.")

async def start_background_tasks(app):
    global MAIN_EVENT_LOOP
    MAIN_EVENT_LOOP = asyncio.get_running_loop()
    # Execute non-blocking login task
    asyncio.create_task(run_login_flow())

app.on_startup.append(start_background_tasks)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    web.run_app(app, host="0.0.0.0", port=port)
