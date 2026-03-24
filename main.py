# main.py
# The Master Launcher for Munh V3 Titan - Optimized for 512MB RAM Limit
# Logic: Smart Persistence - Never clear user tokens unless emergency.

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
    Hamesha yaad rakhega sabhi tokens ko, dubara subscribe nahi karega.
    """
    while True:
        eventlet.sleep(20) # 20 second interval for stability
        try:
            # 1. Garbage Collection: Python ki internal memory flush karo
            gc.collect()
            
            # 2. Smart Check: Hum 'subscribed_tokens_set' ko CLEAR NAHI KARENGE.
            # Sirf tab reset karenge jab 5000+ tokens ho jayein (Extreme Safety)
            current_subs = len(state.subscribed_tokens_set)
            
            if current_subs > 5000:
                # Emergency Reset: Jab RAM 512MB ke paar jaane lage
                state.subscribed_tokens_set.clear()
                logger.warning(f"🚨 [Emergency RAM Guard] Resetting {current_subs} tokens to prevent crash.")
            else:
                # Normal status update (Ye batayega ki price chalu hai)
                logger.info(f"⚡ [RAM Guard] Memory Flushed. Active Subscriptions: {current_subs}")
                
        except Exception as e:
            logger.error(f"⚠️ [RAM Guard Error]: {e}")

# ==============================================================================
# --- 2. MAIN EXECUTION BLOCK ---
# ==============================================================================
if __name__ == '__main__':
    # Step 1: Master Data Load (Symbols/Tokens)
    master_db.sync_master_data()
    
    # Step 2: Launch Background Workers
    # A. Pulse Broadcaster: Har 0.5s mein guchha (Batch) bhejta hai
    socketio.start_background_task(tick_engine.pulse_broadcaster)
    
    # B. Market Data Cleaner: Tick engine ka cache clear karne ke liye
    socketio.start_background_task(tick_engine.market_data_cleaner)
    
    # C. Recovery Manager: WebSocket connection zinda rakhne ke liye
    socketio.start_background_task(recovery_manager.engine_lifecycle_manager)
    
    # D. Smart RAM Guard: Memory leaks rokne ke liye
    socketio.start_background_task(aggressive_memory_protector)
    
    # Step 3: Server Deployment (Render/AWS/Local)
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 [Munh V3 Titan] Server Live on Port {port}...")
    logger.info(f"🛡️ [Logic] Smart Persistence Active: Purane tokens delete nahi honge.")

    try:
        # Eventlet WSGI: 512MB RAM ke liye sabse lightweight server
        eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
    except Exception as fatal:
        logger.critical(f"💀 [Server Crash]: {fatal}")
