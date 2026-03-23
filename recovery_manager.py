# recovery_manager.py
import eventlet, pyotp
from brain import state, logger, API_KEY, CLIENT_CODE, TOTP_STR, MPIN
from auth_manager import start_angel_session, verify_dns_resilience
from tick_engine import on_data_received
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

def re_subscribe_all_tokens():
    """Reconnect hone par saare purane tokens ka batch resubscribe karna"""
    if not state.sws or not state.is_ws_ready: return
    logger.info(f"🔄 [Recovery] Resubscribing {len(state.subscribed_tokens_set)} tokens...")
    
    seg_map = {1: [], 2: [], 3: [], 5: []}
    for t in state.subscribed_tokens_set:
        s = state.token_metadata.get(t, 1)
        seg_map[s].append(t)
        
    for seg, tokens in seg_map.items():
        if not tokens: continue
        for i in range(0, len(tokens), 500):
            batch = tokens[i:i+500]
            try:
                state.sws.subscribe(f"recon_{seg}_{i}", 1, [{"exchangeType": seg, "tokens": batch}])
                eventlet.sleep(0.1)
            except: pass

def engine_lifecycle_manager():
    """Main Recovery Loop: Har 25 sec mein health check"""
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
                        logger.info("💎 [WS] Connected & Auto-Healed!")
                        re_subscribe_all_tokens()

                    def on_close(ws, code, reason):
                        state.is_ws_ready = False
                        logger.warning(f"🔌 [WS] Disconnected: {reason}")

                    state.sws.on_data = on_data_received
                    state.sws.on_open = on_open
                    state.sws.on_close = on_close
                    eventlet.spawn(state.sws.connect)
                    eventlet.sleep(5)
        except Exception as e:
            logger.error(f"⚠️ [Recovery Error]: {e}")
            state.is_ws_ready = False
        eventlet.sleep(25)
