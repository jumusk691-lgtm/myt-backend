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
    Logic: Har 60 second mein memory check karta hai.
    Persistent Logic: Tokens ko tabhi reset karega jab OOM (Out of Memory) ka khatra ho.
    """
    while True:
        # 20s bahut fast tha, 60s Render ke liye better hai taaki CPU throttle na ho
        eventlet.sleep(60) 
        try:
            # 1. Forceful Garbage Collection
            gc.collect()
            
            # 2. Subscription Persistence Check
            current_subs = len(state.subscribed_tokens_set)
            
            # Render 512MB RAM Limit: 3500 tokens is a safer threshold
            if current_subs > 3500:
                logger.warning(f"🚨 [Emergency RAM Guard] High Load ({current_subs} tokens). Cleaning inactive cache.")
                # Sab saaf karne ke bajaye, sirf cache saaf karo, tokens rehne do (Persistence)
                state.global_market_cache.clear() 
                gc.collect()
            
            # 3. Log Update: Isse pata chalega server zinda hai aur batching limit kya hai
            logger.info(f"⚡ [RAM Guard] Status: {current_subs} Tokens | Memory Optimized.")
                
        except Exception as e:
            logger.error(f"⚠️ [RAM Guard Error]: {e}")

# ==============================================================================
# --- 2. MAIN EXECUTION BLOCK ---
# ==============================================================================
if __name__ == '__main__':
    try:
        # Step 1: Master Data Load (Symbols/Tokens)
        # Isse pehle start karo taaki data fetch ho jaye
        logger.info("📡 [Master DB] Syncing data from Supabase...")
        master_db.sync_master_data()
        
        # Step 2: Launch Background Workers
        
        # A. Pulse Broadcaster: Yahi woh engine hai jo ab FULL BATCH bhejega (No Clear Logic)
        socketio.start_background_task(tick_engine.pulse_broadcaster)
        
        # B. Market Data Cleaner: OHLC management (Har 5 min mein cleaning)
        socketio.start_background_task(tick_engine.market_data_cleaner)
        
        # C. Recovery Manager: WebSocket auto-reconnect logic
        socketio.start_background_task(recovery_manager.engine_lifecycle_manager)
        
        # D. Smart RAM Guard: Isko last mein start karo
        socketio.start_background_task(aggressive_memory_protector)
        
        # Step 3: Server Deployment Settings
        port = int(os.environ.get("PORT", 10000))
        
        # IMPORTANT: Eventlet configuration for better concurrency on low RAM
        logger.info(f"🚀 [Munh V3 Titan] Live on Port {port} | Mode: Full Batch Persistence")
        
        # Eventlet WSGI deployment
        eventlet.wsgi.server(
            eventlet.listen(('0.0.0.0', port)), 
            app,
            log_output=False # Production mein logs kam karne ke liye (RAM bachegi)
        )

    except KeyboardInterrupt:
        logger.info("🛑 [Server] Manual shutdown initiated...")
    except Exception as fatal:
        logger.critical(f"💀 [Fatal Crash]: {fatal}")
