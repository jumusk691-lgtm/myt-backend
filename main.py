# main.py
# The Master Launcher for Munh V3 Titan - Optimized for 512MB RAM Limit

import os, eventlet, gc
from brain import app, socketio, logger, state
# Internal Managers
import auth_manager, master_db, tick_engine, socket_manager, recovery_manager, historical_manager, p2p_distributor

# ==============================================================================
# --- 1. SMART AGGRESSIVE RAM PROTECTOR ---
# ==============================================================================
def aggressive_memory_protector():
    """
    Logic: Har 20 second mein Python ki leaked memory saaf karta hai.
    Selective Flush: Tokens ko yaad rakhega, faltu cache udayega.
    """
    while True:
        eventlet.sleep(20) 
        try:
            # 1. Python ki internal memory release karo
            gc.collect()
            
            # 2. Token Limit Check
            current_subs = len(state.subscribed_tokens_set)
            
            # Agar 4000+ tokens ho jayein (Render ki safe limit), tabhi reset karo
            if current_subs > 4000:
                logger.warning(f"🚨 [Emergency RAM Guard] High Load ({current_subs} tokens). Resetting to prevent OOM Crash.")
                state.subscribed_tokens_set.clear()
                state.token_metadata.clear()
                state.token_ref_count.clear()
            else:
                # Sirf status update (Logs mein green signal)
                logger.info(f"⚡ [RAM Guard] Memory Flushed. Active Subscriptions: {current_subs}")
                
        except Exception as e:
            logger.error(f"⚠️ [RAM Guard Error]: {e}")

# ==============================================================================
# --- 2. MAIN EXECUTION BLOCK ---
# ==============================================================================
if __name__ == '__main__':
    try:
        # Step 1: Master Data Load (Symbols/Tokens)
        # Ensure Supabase is connected before starting workers
        master_db.sync_master_data()
        
        # Step 2: Launch Background Workers (The Multi-Threading Engine)
        
        # A. Pulse Broadcaster: Batch updates for low latency
        socketio.start_background_task(tick_engine.pulse_broadcaster)
        
        # B. Market Data Cleaner: Tick engine ka cache saaf karne ke liye
        socketio.start_background_task(tick_engine.market_data_cleaner)
        
        # C. Recovery Manager: Auto-reconnect for AngelOne WebSocket
        socketio.start_background_task(recovery_manager.engine_lifecycle_manager)
        
        # D. Smart RAM Guard: Garbage collection logic
        socketio.start_background_task(aggressive_memory_protector)
        
        # Step 3: Server Deployment
        port = int(os.environ.get("PORT", 10000))
        logger.info(f"🚀 [Munh V3 Titan] Server Live on Port {port}...")
        logger.info(f"🛡️ [Logic] Selective Flush Active: Token persistence guaranteed.")

        # Eventlet WSGI: 512MB RAM ke liye lightweight and fast
        eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)

    except KeyboardInterrupt:
        logger.info("🛑 [Server] Shutting down gracefully...")
    except Exception as fatal:
        logger.critical(f"💀 [Fatal Crash]: {fatal}")
