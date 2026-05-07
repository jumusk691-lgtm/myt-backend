# recovery_manager.py
import eventlet, gc
from brain import state, logger, API_KEY, CLIENT_CODE, socketio
from auth_manager import start_angel_session, verify_dns_resilience
from tick_engine import on_data_received
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ==============================================================================
# --- 1. TOKEN RE-SYNC (APK Trigger Logic) ---
# ==============================================================================
def re_subscribe_all_tokens():
    """
    Logic: Restart hone par APK ko signal bhejna taaki wo tokens re-send kare.
    Isse database ki dependency khatam ho jati hai.
    """
    if not state.sws or not state.is_ws_ready: return
    
    # 1. APK ko signal bhejo: "Bhai server restart hua hai, tokens refresh karo"
    logger.info("📡 [Recovery] Signaling APK to re-sync all active tokens...")
    socketio.emit('server_restarted_sync_needed', {'status': 'ready'})
    
    # 2. Jo tokens RAM mein bache hain unhe turant subscribe karo
    active_tokens = list(state.subscribed_tokens_set)
    if not active_tokens: 
        logger.info("ℹ️ [Recovery] RAM is empty. Waiting for APK to push tokens via URL.")
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
                # Direct Push to AngelOne Pipe
                state.sws.subscribe("myt_pipe", 1, [{"exchangeType": seg, "tokens": batch}])
                eventlet.sleep(0.1) # 429 Rate limit safety
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
                        # Restart ke baad turant APK se tokens mangwao
                        re_subscribe_all_tokens()

                    def on_close(ws, code, reason):
                        state.is_ws_ready = False
                        logger.warning(f"🔌 [Pipe] Broken: {reason} | Auto-Healing...")
                        gc.collect()

                    # Direct callbacks
                    state.sws.on_data = on_data_received
                    state.sws.on_open = on_open
                    state.sws.on_close = on_close
                    
                    eventlet.spawn(state.sws.connect)
                    eventlet.sleep(5)
            
            # Health check: RAM mein kitne tokens zinda hain
            token_count = len(state.subscribed_tokens_set)
            logger.info(f"⚡ [RAM Guard] Status: {token_count} Tokens | RAM: OK")
            
            gc.collect()
            
        except Exception as e:
            logger.error(f"⚠️ [Loop Error]: {e}")
            state.is_ws_ready = False
            
        eventlet.sleep(25)
