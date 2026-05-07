# recovery_manager.py
import eventlet, gc
from brain import state, logger, API_KEY, CLIENT_CODE, socketio
from auth_manager import start_angel_session, verify_dns_resilience
from tick_engine import on_data_received
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ==============================================================================
# --- 1. TOKEN RE-SYNC (APK Match Logic) ---
# ==============================================================================
def re_subscribe_all_tokens():
    """
    Logic: Restart ke baad signal bhejta hai. 
    APK side pe 'server_restarted_sync_needed' sunne par re-sync trigger hoga.
    """
    if not state.sws or not state.is_ws_ready: return
    
    # --- BROADCAST SIGNAL ---
    def broadcast_sync():
        # APK ko 3 baar signal bhejenge taaki network fluctuation mein miss na ho
        for i in range(3): 
            logger.info(f"📡 [Recovery] Signaling APK to re-sync (Attempt {i+1})...")
            socketio.emit('server_restarted_sync_needed', {'status': 'ready'})
            eventlet.sleep(3) # 3s gap for APK stability

    eventlet.spawn(broadcast_sync)

    # RAM recovery logic
    active_tokens = list(getattr(state, 'subscribed_tokens_set', set()))
    if not active_tokens: 
        logger.info("ℹ️ [Recovery] RAM Empty. Waiting for APK Push via /api/add_token")
        return

    logger.info(f"🚀 [Direct Pipe] Resubscribing {len(active_tokens)} tokens from RAM...")
    
    # Segment wise batching for AngelOne stability
    seg_map = {1: [], 2: [], 3: [], 5: []}
    for t in active_tokens:
        s = state.token_metadata.get(t, 1)
        seg_map.get(s, seg_map[1]).append(t)
        
    for seg, tokens in seg_map.items():
        if not tokens: continue
        for i in range(0, len(tokens), 500):
            batch = tokens[i:i+500]
            try:
                state.sws.subscribe("myt_pipe", 1, [{"exchangeType": seg, "tokens": batch}])
                eventlet.sleep(0.2) # Rate limit safety
            except Exception as e:
                logger.error(f"❌ [Pipe Error]: {e}")

# ==============================================================================
# --- 2. ENGINE LIFECYCLE (The Guardian) ---
# ==============================================================================
def engine_lifecycle_manager():
    """
    Har 25s mein connection health check karta hai.
    """
    while True:
        try:
            if not getattr(state, 'is_ws_ready', False):
                if not verify_dns_resilience():
                    eventlet.sleep(15)
                    continue
                
                session_data = start_angel_session()
                if session_data:
                    # WebSocket V2 Initialization
                    state.sws = SmartWebSocketV2(
                        session_data['jwtToken'], API_KEY, CLIENT_CODE, session_data['feedToken']
                    )
                    
                    def on_open(ws):
                        state.is_ws_ready = True
                        logger.info("💎 [Pipe] Live & Connected!")
                        # 5s Wait: APK ko socket.io reconnect hone ka time do
                        eventlet.sleep(5) 
                        re_subscribe_all_tokens()

                    def on_close(ws, code, reason):
                        state.is_ws_ready = False
                        logger.warning(f"🔌 [Pipe] Broken: {reason}")
                        gc.collect()

                    def on_error(ws, error):
                        state.is_ws_ready = False
                        logger.error(f"⚠️ [Pipe Error]: {error}")

                    # Callbacks alignment
                    state.sws.on_data = on_data_received
                    state.sws.on_open = on_open
                    state.sws.on_close = on_close
                    state.sws.on_error = on_error
                    
                    eventlet.spawn(state.sws.connect)
                    eventlet.sleep(5)
            
            # Monitoring Health
            token_count = len(getattr(state, 'subscribed_tokens_set', set()))
            logger.info(f"⚡ [Monitor] Active Tokens: {token_count} | Status: OK")
            gc.collect()
            
        except Exception as e:
            logger.error(f"⚠️ [Loop Error]: {e}")
            state.is_ws_ready = False
            
        eventlet.sleep(25)
