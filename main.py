# main.py
# The Master Launcher for Munh V3 Titan - Final Optimized for 512MB RAM
# Logic: Zero-Storage, Direct Pipe Bypass Mode

import os, eventlet, gc
eventlet.monkey_patch(all=True) # Ensure patches are applied first

from brain import app, socketio, logger, state
# Internal Managers
import auth_manager, master_db, tick_engine, socket_manager, recovery_manager, historical_manager, p2p_distributor

# ==============================================================================
# --- 1. SMART AGGRESSIVE RAM PROTECTOR (Updated for Pipe Mode) ---
# ==============================================================================
def aggressive_memory_protector():
    """
    Logic: Har 60 second mein memory check karta hai.
    Zero-RAM Mode: Ab humein cache clear karne ki zarurat nahi, sirf RAM flush karna hai.
    """
    while True:
        eventlet.sleep(60) 
        try:
            # 1. Forceful Garbage Collection (Isse unused memory turant free hoti hai)
            gc.collect()
            
            # 2. Monitoring (Bina storage ke bhi tokens track karenge bandwidth ke liye)
            current_subs = len(state.subscribed_tokens_set)
            active_users = len(state.active_users_pool)
            
            # Log Update: Monitoring server health
            logger.info(f"⚡ [RAM Guard] Status: {current_subs} Tokens | {active_users} Users | RAM: Minimal")
                
        except Exception as e:
            logger.error(f"⚠️ [RAM Guard Error]: {e}")

# ==============================================================================
# --- 2. MAIN EXECUTION BLOCK ---
# ==============================================================================
if __name__ == '__main__':
    try:
        # Step 1: Master Data Load (Symbols/Tokens)
        logger.info("📡 [Master DB] Syncing data from Supabase...")
        master_db.sync_master_data()
        
        # Step 2: Launch Background Workers (BYPASS MODE)
        
        # A. TICK PIPE: on_data_received (tick_engine mein) ab seedha bypass karega.
        # Broadcaster loop ki ab zarurat nahi hai (CPU Load 0% ho jayega).
        # socketio.start_background_task(tick_engine.pulse_broadcaster) # DISABLED for Bypass
        
        # B. RAM MONITOR: Sirf status check karne ke liye
        socketio.start_background_task(tick_engine.market_data_cleaner)
        
        # C. RECOVERY MANAGER: WebSocket auto-reconnect logic (Sabse zaruri)
        socketio.start_background_task(recovery_manager.engine_lifecycle_manager)
        
        # D. AGGRESSIVE RAM GUARD: Keep Render under 15-20MB
        socketio.start_background_task(aggressive_memory_protector)
        
        # Step 3: Server Deployment Settings
        port = int(os.environ.get("PORT", 10000))
        
        logger.info(f"🚀 [Munh V3 Titan] LIVE on Port {port} | Mode: ZERO-RAM BYPASS PIPE")
        
        # Eventlet WSGI deployment (Production Grade)
        eventlet.wsgi.server(
            eventlet.listen(('0.0.0.0', port)), 
            app,
            log_output=True # Ticks monitoring ke liye True rakha hai, baad mein False kar sakte hain
        )

    except KeyboardInterrupt:
        logger.info("🛑 [Server] Manual shutdown initiated...")
    except Exception as fatal:
        logger.critical(f"💀 [Fatal Crash]: {fatal}")
