# main.py
# The Master Launcher for Munh V3 Titan - Optimized for 512MB RAM

import os, eventlet, gc
from brain import app, socketio, logger, state
# Ensure all internal managers are imported correctly
import auth_manager, master_db, tick_engine, socket_manager, recovery_manager, historical_manager, p2p_distributor

# ==============================================================================
# --- 1. AGGRESSIVE RAM PROTECTOR (Smart Version) ---
# ==============================================================================
def aggressive_memory_protector():
    """
    Logic: Har 15 second mein Python ki internal memory saaf karta hai.
    Important: Hum tokens ko clear nahi karenge taaki purane users ka data chalta rahe.
    """
    while True:
        eventlet.sleep(15) # 15 second ka interval
        try:
            # 1. Python Garbage Collector: Jo memory 'leaked' hai use turant free karo
            gc.collect()
            
            # 2. State Limit: Agar tokens 5000 se upar jayein (Extreme Case), tabhi reset karo
            # 2000 bahut kam tha, isliye 5000 safe limit hai 512MB ke liye
            if len(state.subscribed_tokens_set) > 5000:
                state.subscribed_tokens_set.clear()
                logger.warning("🚨 [RAM Guard] Token limit exceeded. Emergency Reset performed.")
                
            logger.info(f"⚡ [RAM Guard] Memory Flushed. Active Subscriptions: {len(state.subscribed_tokens_set)}")
        except Exception as e:
            logger.error(f"⚠️ [RAM Guard Error]: {e}")

# ==============================================================================
# --- 2. MAIN EXECUTION BLOCK ---
# ==============================================================================
if __name__ == '__main__':
    # Step 1: Sync Master Data (Token list load karna)
    master_db.sync_master_data()
    
    # Step 2: Background Tasks Launch
    # A. Market Data Broadcaster (Live price push)
    socketio.start_background_task(tick_engine.pulse_broadcaster)
    
    # B. Market Data Cleaner (Temporary cache cleaning - Files 4 & 18)
    socketio.start_background_task(tick_engine.market_data_cleaner)
    
    # C. Recovery Manager (Connection maintenance)
    socketio.start_background_task(recovery_manager.engine_lifecycle_manager)
    
    # D. RAM Guard (Server protection)
    socketio.start_background_task(aggressive_memory_protector)
    
    # Step 3: Server Start
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 [Munh V3 Titan] Live on port {port} with Smart-Sync Protection...")
    
    try:
        # Eventlet WSGI server: Best for handling thousands of concurrent Socket connections
        eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
    except Exception as fatal:
        logger.critical(f"💀 [Server Crash]: {fatal}")
