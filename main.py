import json
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# Note: Yahan app, sm, state, logger, master_db, etc. pehle se defined hone chahiye.
# Agar nahi hain, toh unke imports zaroori hain.

# ==============================================================================
# --- 2. BINARY EMIT FUNCTION ---
# ==============================================================================

# To Binary (APK Expectation)
async def emit_binary_batch(event, data, room=None):
    try:
        # Data ko binary mein convert kar raha hai
        binary_payload = json.dumps(data).encode('utf-8')
        if room:
            await sm.emit(event, binary_payload, to=room)
        else:
            await sm.emit(event, binary_payload)
    except Exception as e:
        logger.error(f"❌ [Binary Emit Error]: {e}")

# Global mapping injection
socket_manager.emit_binary_batch = emit_binary_batch

# ==============================================================================
# --- 3. DIRECT TOKEN SYNC (API ENDPOINT) ---
# ==============================================================================

@app.post('/api/add_token')
async def add_new_token_direct(request: Request):
    """APK side HealthCheck/Re-sync endpoint"""
    try:
        data = await request.json()
        token = str(data.get('token'))
        exch = int(data.get('exch', 1))
        
        if not token:
            return JSONResponse(content={"status": "error", "msg": "No token"}, status_code=400)

        state.subscribed_tokens_set.add(token)
        state.token_metadata[token] = exch
        
        if state.sws and state.is_ws_ready:
            state.sws.subscribe("myt_direct", 1, [{"exchangeType": exch, "tokens": [token]}])
            logger.info(f"✅ [Direct Sync] Token {token} Active")
            return {"status": "success", "token": token}
            
        return JSONResponse(content={"status": "pending", "msg": "WS Not Ready"}, status_code=202)
    except Exception as e:
        logger.error(f"❌ [API Error]: {e}")
        return JSONResponse(content={"status": "error", "msg": str(e)}, status_code=500)

# ==============================================================================
# --- 4. LIFECYCLE (STARTUP) ---
# ==============================================================================

@app.on_event("startup")
async def startup_event():
    """Backend start hote hi background tasks shuru karega"""
    try:
        # Step 1: Master DB Sync
        logger.info("📡 [Master DB] Initializing...")
        master_db.sync_master_data()
        
        # Step 2: Background Tasks Launch
        asyncio.create_task(aggressive_memory_protector())
        
        if hasattr(recovery_manager, 'engine_lifecycle_manager'):
            # lifecycle_manager ko async loop mein chalana zaroori hai
            asyncio.create_task(asyncio.to_thread(recovery_manager.engine_lifecycle_manager))
            
        if hasattr(tick_engine, 'market_data_cleaner'):
            asyncio.create_task(asyncio.to_thread(tick_engine.market_data_cleaner))

        logger.info("🚀 [Munh V3 Titan] Server Ready")

    except Exception as fatal:
        logger.critical(f"💀 [Fatal Startup Error]: {fatal}")

# Back4App ya Northflank ke liye uvicorn startup command:
# uvicorn main:app --host 0.0.0.0 --port 8080
