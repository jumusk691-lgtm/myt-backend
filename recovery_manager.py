# recovery_manager.py
import eventlet, gc
from brain import state, logger, API_KEY, CLIENT_CODE, socketio
from auth_manager import start_angel_session, verify_dns_resilience
from tick_engine import on_data_received
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ==============================================================================
# --- 1. TOKEN RE-SYNC (Improved Signal) ---
# ==============================================================================
def re_subscribe_all_tokens():
    """
    Logic: Restart ke baad signal bhejta hai. 
    Added 3 retry attempts taaki APK signal miss na kare.
    """
    if not state.sws or not state.is_ws_ready: return
    
    # --- FIXED SIGNAL LOGIC ---
    # Signal ko 3 baar bhejenge 2-2 second ke gap mein taaki APK sun le
    def broadcast_sync():
        for i in range(3): 
            logger.info(f"📡 [Recovery] Signaling APK to re-sync (Attempt {i+1})...")
            socketio.emit('server_restarted_sync_needed', {'status': 'ready'})
            eventlet.sleep(2) 

    eventlet.spawn(broadcast_sync) # Background mein bhejta rahega

    # Baaki RAM wala logic as it is
    active_tokens = list(state.subscribed_tokens_set)
    if not active_tokens: 
        logger.info("ℹ️ [Recovery] RAM Empty. Waiting for APK Push via URL.")
        return

    logger.info(f"🚀 [Direct Pipe] Resubscribing {len(active_tokens)} tokens from RAM...")
    
    seg_map = {1: [], 2: [], 3: [], 5: []}
    for t in active_tokens:
        s = state.token_metadata.get(t, 1)
        seg_map[s].append(t)
        
    for seg, tokens in seg_map.items():
        if not tokens: continue
        for i in range(0, len(tokens), 500):
            batch = tokens[i:i+500]
            try:
                state.sws.subscribe("myt_pipe", 1, [{"exchangeType": seg, "tokens": batch}])
                eventlet.sleep(0.1) 
            except Exception as e:
                logger.error(f"❌ [Pipe Error]: {e}")

# ==============================================================================
# --- 2. ENGINE LIFECYCLE (The Guardian) ---
# ==============================================================================
def engine_lifecycle_manager():
    while True:
        try:
            if not state.is_ws_ready:
                if not verify_dns_resilience():
                    eventlet.sleep(15)
                    continue
                
                session_data = start_angel_session()
                if session_data:
                    state.sws = SmartWebSocketV2(
                        session_data['jwtToken'], API_KEY, CLIENT_CODE, session_data['feedToken']
                    )
                    
                    def on_open(ws):
                        state.is_ws_ready = True
                        logger.info("💎 [Pipe] Live & Connected!")
                        # 5 second wait taaki APK ke socket connect ho sakein
                        eventlet.sleep(5) 
                        re_subscribe_all_tokens()

                    # Fix Arguments for SmartApi WebSocket V2
                    def on_close(ws, code, reason):
                        state.is_ws_ready = False
                        logger.warning(f"🔌 [Pipe] Broken: {reason}")
                        gc.collect()

                    state.sws.on_data = on_data_received
                    state.sws.on_open = on_open
                    state.sws.on_close = on_close
                    
                    eventlet.spawn(state.sws.connect)
                    eventlet.sleep(5)
            
            gc.collect()
            
        except Exception as e:
            logger.error(f"⚠️ [Loop Error]: {e}")
            state.is_ws_ready = False
            
        eventlet.sleep(25)
