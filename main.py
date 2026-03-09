import os, pyotp, datetime, pytz, requests, sqlite3, tempfile, json, asyncio
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from supabase import create_client
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager
import redis.asyncio as aioredis  # Async Redis use karein

# --- 1. CONFIG ---
API_KEY = "85HE4VA1"
CLIENT_CODE = "S52638556"
PWD = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"
IST = pytz.timezone('Asia/Kolkata')

SUPABASE_URL = "https://tnrhlvibaeiwhlrxdxnm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRucmhsdmliYWVpd2hscnhkeG5tIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY0NzQ0NywiZXhwIjoyMDg4MjIzNDQ3fQ.epYmt7sxhZRhEQWoj0doCHAbfOTHOjSurBbLss5a4Pk"
BUCKET_NAME = "Myt"

REDIS_URL = "rediss://default:ATjsAAIncDFiOTVlY2RlNDI1ODk0MDI4YmRiMmE2Yzg0Y2RkM2RkZHAxMTQ1NzI@quick-narwhal-14572.upstash.io:6379"

# Global Objects
redis_client = None
sws = None
is_ws_ready = False

# --- 2. TICK ENGINE (REDIS ASYNC PUBLISH) ---
def on_data(wsapp, msg):
    if isinstance(msg, dict) and 'token' in msg:
        token = str(msg.get('token'))
        ltp = float(msg.get('last_traded_price', 0)) / 100
        if ltp > 0:
            tick_data = {
                "t": token,
                "p": "{:.2f}".format(ltp),
                "c": "{:.2f}".format(float(msg.get('close', 0)) / 100) if 'close' in msg else "0.00"
            }
            # Use a helper to run async publish from sync callback
            asyncio.run_coroutine_threadsafe(
                redis_client.publish("live_ticks", json.dumps(tick_data)), 
                loop
            )

# --- 3. LIFESPAN & WEBSOCKET ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, loop
    loop = asyncio.get_running_loop()
    # Initialize Async Redis
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    asyncio.create_task(manage_connection_loop())
    yield
    await redis_client.close()

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("📱 Android Connected")
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("live_ticks")
    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                await websocket.send_text(message["data"])
    except WebSocketDisconnect:
        print("📱 Android Disconnected")
    finally:
        await pubsub.unsubscribe("live_ticks")

# --- 4. ANGEL CONNECTION MANAGER ---
async def manage_connection_loop():
    global sws, is_ws_ready
    while True:
        now = datetime.datetime.now(IST)
        if 8 <= now.hour < 24:
            if not is_ws_ready:
                try:
                    smart_api = SmartConnect(api_key=API_KEY)
                    session = smart_api.generateSession(CLIENT_CODE, PWD, pyotp.TOTP(TOTP_STR).now())
                    if session.get('status'):
                        sws = SmartWebSocketV2(session['data']['jwtToken'], API_KEY, CLIENT_CODE, session['data']['feedToken'])
                        sws.on_data = on_data
                        sws.on_open = lambda ws: print("🟢 Angel Live") or setattr(sws, 'is_ready', True)
                        
                        import threading
                        threading.Thread(target=sws.connect, daemon=True).start()
                        is_ws_ready = True
                        
                        # Initial Subscribe
                        await asyncio.sleep(5)
                        tokens = [{"exchangeType": 1, "tokens": ["26000", "26009"]}, {"exchangeType": 5, "tokens": ["234316", "234313"]}]
                        sws.subscribe("init", 1, tokens)
                except Exception as e:
                    print(f"Conn Error: {e}")
        await asyncio.sleep(60)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
