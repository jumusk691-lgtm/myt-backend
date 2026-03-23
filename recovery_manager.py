# recovery_manager.py
# File 6: Handles Auto-Reconnection and WebSocket Lifecycle

from brain import state, logger, eventlet, API_KEY, CLIENT_CODE, TOTP_STR, MPIN
from auth_manager import start_angel_session, verify_dns_resilience
from tick_engine import on_data_received
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp

# ==============================================================================
# --- 1. THE AUTO-RESUBSCRIBE LOGIC (Batch Mode) ---
# ==============================================================================
def re_subscribe_all_tokens():
    """Reconnect hone ke baad saare purane tokens ko batch mein bhejta hai"""
    if not state.sws or not state.is_ws_ready: return
    
    logger.info(f"🔄 [Recovery] Resubscribing {len(state.subscribed_tokens_set)} tokens...")
    
    # Segment wise sorting
    seg_map = {1: [], 2: [], 3: [], 5: []}
    for t in state.subscribed_tokens_set:
        s = state.token_metadata.get(t, 1)
        seg_map[s].append(t)
        
    # 500-500 ke batches mein subscribe karo
    for seg, tokens in seg_map.items():
        if not tokens: continue
        for i in range(0, len(tokens), 500):
            batch = tokens[i:i+500]
            try:
                state.sws.subscribe(f"recovery_{seg}_{i}", 1, [{"exchangeType": seg, "tokens": batch}])
                eventlet.sleep(0.1) 
            except Exception as e:
                logger.error(f"❌ [Recovery] Sub Error: {e}")

# ==============================================================================
# --- 2. THE MAIN LIFECYCLE ENGINE ---
# ==============================================================================
def engine_lifecycle_manager():
    """Har 25 second mein check karta hai ki system live hai ya nahi"""
    while True:
        try:
            if not state.is_ws_ready:
                if not verify_dns_resilience():
                    eventlet.sleep(15)
                    continue
                
                logger.info("📡 [Lifecycle] Connection lost. Attempting Auto-Heal...")
                
                # Naya Session Generate karo
                session_data = start_angel_session()
                
                if session_data:
                    state.sws = SmartWebSocketV2(
                        session_data['jwtToken'], API_KEY, CLIENT_CODE, session_data['feedToken']
                    )
                    
                    # Callbacks setup
                    def on_open(ws):
                        state.is_ws_ready = True
                        state.reconnect_count += 1
                        logger.info(f"💎 [WS] Connected! Session No: {state.reconnect_count}")
                        re_subscribe_all_tokens() # Purane tokens wapas lao

                    def on_close(ws, code, reason):
                        state.is_ws_ready = False
                        logger.warning(f"🔌 [WS] Closed: {reason}")

                    state.sws.on_data = on_data_received # File 4 se joda
                    state.sws.on_open = on_open
                    state.sws.on_close = on_close
                    
                    # Threading ke bina connect karo (Run in background)
                    eventlet.spawn(state.sws.connect)
                    eventlet.sleep(5)
            
        except Exception as e:
            logger.error(f"⚠️ [Recovery] Critical Failure: {e}")
            state.is_ws_ready = False
            
        eventlet.sleep(25) # Har 25 sec mein health check
