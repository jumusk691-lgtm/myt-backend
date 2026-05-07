# recovery_manager.py
import eventlet, gc
from brain import state, logger, API_KEY, CLIENT_CODE, socketio
from auth_manager import start_angel_session, verify_dns_resilience
from tick_engine import on_data_received
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
# Database se sirf active tokens ki list uthane ke liye
from master_db import get_active_watchlist_tokens 

# ==============================================================================
# --- 1. TOKEN RE-SYNC (The Memory Fix) ---
# ==============================================================================
def re_subscribe_all_tokens():
    """
    Logic: Restart ke baad agar RAM khali hai, toh DB se sirf Tokens ki list uthao
    aur AngelOne ko 'Direct Push' ke liye bolo.
    """
    if not state.sws or not state.is_ws_ready: return
    
    # 1. RAM check karo, agar 0 tokens hain toh DB se list bharo
    if not state.subscribed_tokens_set:
        logger.info("📂 [Memory] RAM khali hai. Supabase se tokens ki list mangwa raha hoon...")
        db_tokens = get_active_watchlist_tokens() 
        if db_tokens:
            state.subscribed_tokens_set.update(db_tokens)
            logger.info(f"✅ [Memory] {len(db_tokens)} Tokens yaad aa gaye!")

    active_tokens = list(state.subscribed_tokens_set)
    if not active_tokens: 
        logger.info("ℹ️ [Recovery] List abhi bhi khali hai. APK se add karna padega.")
        return

    # 2. Direct AngelOne Subscription
    logger.info(f"🚀 [Direct Pipe] Resubscribing {len(active_tokens)} tokens...")
    
    seg_map = {1: [], 2: [], 3: [], 5: []}
    for t in active_tokens:
        s = state.token_metadata.get(t, 1)
        seg_map[s].append(t)
        
    for seg, tokens in seg_map.items():
        if not tokens: continue
        for i in range(0, len(tokens), 500):
            batch = tokens[i:i+500]
            try:
                # Direct Hit to AngelOne
                state.sws.subscribe("myt_pipe", 1, [{"exchangeType": seg, "tokens": batch}])
                eventlet.sleep(0.1) # 429 Rate limit safety
            except Exception as e:
                logger.error(f"❌ [Pipe Error]: {e}")

# ==============================================================================
# --- 2. ENGINE LIFECYCLE (The Guardian) ---
# ==============================================================================
def engine_lifecycle_manager():
    """
    Har 25s mein connection check aur automatic recovery.
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
                        # Connect hote hi tokens mangwao
                        re_subscribe_all_tokens()

                    def on_close(ws, code, reason):
                        state.is_ws_ready = False
                        logger.warning(f"🔌 [Pipe] Broken: {reason}")
                        gc.collect()

                    # Direct on_data callback to tick_engine
                    state.sws.on_data = on_data_received
                    state.sws.on_open = on_open
                    state.sws.on_close = on_close
                    
                    eventlet.spawn(state.sws.connect)
                    eventlet.sleep(5)
            
            # Monitoring Health
            gc.collect()
            
        except Exception as e:
            logger.error(f"⚠️ [Loop Error]: {e}")
            state.is_ws_ready = False
            
        eventlet.sleep(25)
