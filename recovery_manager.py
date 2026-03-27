# recovery_manager.py
import eventlet, pyotp, gc
from brain import state, logger, API_KEY, CLIENT_CODE, TOTP_STR, MPIN
from auth_manager import start_angel_session, verify_dns_resilience
from tick_engine import on_data_received
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ==============================================================================
# --- 1. RE-SUBSCRIBE LOGIC (Fast Re-Pipe) ---
# ==============================================================================
def re_subscribe_all_tokens():
    """
    Reconnect hone par saare active tokens ko AngelOne se 
    dobara mangwana taaki pipeline chalu ho jaye.
    """
    if not state.sws or not state.is_ws_ready: return
    
    # Snapshot of tokens to avoid runtime size change error
    active_tokens = list(state.subscribed_tokens_set)
    if not active_tokens: 
        logger.info("ℹ️ [Recovery] No tokens to resubscribe.")
        return

    logger.info(f"🔄 [Recovery] Resubscribing {len(active_tokens)} tokens to Pipe...")
    
    seg_map = {1: [], 2: [], 3: [], 5: []}
    for t in active_tokens:
        s = state.token_metadata.get(t, 1)
        seg_map[s].append(t)
        
    for seg, tokens in seg_map.items():
        if not tokens: continue
        # Split into 500 batches (Angel API Limit)
        for i in range(0, len(tokens), 500):
            batch = tokens[i:i+500]
            try:
                # Direct resubscribe to the stream
                state.sws.subscribe("myt_reconnect", 1, [{"exchangeType": seg, "tokens": batch}])
                eventlet.sleep(0.05) # Fast relay
            except Exception as e:
                logger.error(f"❌ [Resub Error]: {e}")

# ==============================================================================
# --- 2. ENGINE LIFECYCLE MANAGER (The Guardian) ---
# ==============================================================================
def engine_lifecycle_manager():
    """
    Main Recovery Loop: Har 25 sec mein check karta hai ki server 
    AngelOne se connected hai ya nahi.
    """
    while True:
        try:
            if not state.is_ws_ready:
                # Step A: DNS Check (Render Network Stability)
                if not verify_dns_resilience():
                    logger.warning("🌐 [Recovery] Waiting for Stable Network...")
                    eventlet.sleep(15)
                    continue
                
                # Step B: Session Refresh
                session_data = start_angel_session()
                if session_data:
                    # Initialize New WebSocket Instance
                    state.sws = SmartWebSocketV2(
                        session_data['jwtToken'], API_KEY, CLIENT_CODE, session_data['feedToken']
                    )
                    
                    def on_open(ws):
                        state.is_ws_ready = True
                        logger.info("💎 [WS] Connected & Pipeline Opened!")
                        # Reconnect ke baad purane saare symbols ka data fir se chalu karo
                        re_subscribe_all_tokens()

                    def on_close(ws, code, reason):
                        state.is_ws_ready = False
                        logger.warning(f"🔌 [WS] Disconnected: {reason} | Auto-Healing in 25s...")
                        gc.collect() # RAM Clean on disconnect

                    # Connect the pipe
                    state.sws.on_data = on_data_received
                    state.sws.on_open = on_open
                    state.sws.on_close = on_close
                    
                    # Run connection in background thread
                    eventlet.spawn(state.sws.connect)
                    eventlet.sleep(5)
                    
            # Heartbeat check for memory
            gc.collect()
            
        except Exception as e:
            logger.error(f"⚠️ [Recovery Loop Error]: {e}")
            state.is_ws_ready = False
            
        eventlet.sleep(25)
