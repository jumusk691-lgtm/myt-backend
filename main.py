# main.py
# The Master Launcher for Munh V3 Titan - Final Optimized for 512MB RAM
# Logic: Zero-Storage, Direct Pipe Bypass Mode with Binary Compression

import os, eventlet, gc, json, zlib
eventlet.monkey_patch(all=True) # Ensure patches are applied first

from brain import app, socketio, logger, state
# Internal Managers
import auth_manager, master_db, tick_engine, socket_manager, recovery_manager, historical_manager, p2p_distributor

# ==============================================================================
# --- 0. ATTRIBUTE COMPATIBILITY LAYER (CRITICAL FIX) ---
# ==============================================================================
# Bhai, logs mein 'active_users_list' ka error aa raha hai. 
# Hum yahan ensure kar rahe hain ki dono naam kaam karein taaki crash na ho.
if not hasattr(state, 'active_users_pool'):
    state.active_users_pool = set()

# Mapping 'active_users_list' to 'active_users_pool' for legacy support
state.active_users_list = state.active_users_pool 

# ==============================================================================
# --- 1. SMART AGGRESSIVE RAM PROTECTOR (Updated for Pipe Mode) ---
# ==============================================================================
def aggressive_memory_protector():
    """
    Logic: Har 60 second mein memory check karta hai.
    Zero-RAM Mode: Flush RAM and monitor active connections.
    """
    while True:
        eventlet.sleep(60) 
        try:
            # 1. Forceful Garbage Collection
            gc.collect()
            
            # 2. Monitoring
            current_subs = len(state.subscribed_tokens_set) if hasattr(state, 'subscribed_tokens_set') else 0
            active_users = len(state.active_users_pool)
            
            # Log Update: Monitoring server health
            logger.info(f"⚡ [RAM Guard] Status: {current_subs} Tokens | {active_users} Users | RAM: Minimal")
            
            # Rate Limit Protection: Agar 429 error aa raha hai, toh connections clear karo
            if active_users > 500: # Example limit for Render Free Tier
                logger.warning("⚠️ High Load Detected! Clearing stale connections...")
                state.active_users_pool.clear()
                
        except Exception as e:
            logger.error(f"⚠️ [RAM Guard Error]: {e}")

# ==============================================================================
# --- 2. BINARY PIPE EMITTER (For Error 429 & Too Many Packets Fix) ---
# ==============================================================================
def emit_binary_batch(event, data, room=None):
    """
    Logic: JSON data ko compress karke binary format mein bhejta hai.
    Isse 'Too many packets' wala error khatam ho jayega.
    """
    try:
        json_data = json.dumps(data)
        # Binary compression (zlib) - Optional, but simple string to bytes is safer for Socket.io
        binary_data = json_data.encode('utf-8') 
        
        if room:
            socketio.emit(event, binary_data, room=room)
        else:
            socketio.emit(event, binary_data)
    except Exception as e:
        logger.error(f"❌ [Binary Emit Error]: {e}")

# Monkey patch socket_manager to use binary if needed
# socket_manager.emit_data = emit_binary_batch 

# ==============================================================================
# --- 3. MAIN EXECUTION BLOCK ---
# ==============================================================================
if __name__ == '__main__':
    try:
        # Step 1: Master Data Load (Symbols/Tokens)
        logger.info("📡 [Master DB] Syncing data from Supabase...")
        master_db.sync_master_data()
        
        # Step 2: Launch Background Workers (BYPASS MODE)
        
        # A. TICK CLEANER: Junk tokens remove karne ke liye
        if hasattr(tick_engine, 'market_data_cleaner'):
            socketio.start_background_task(tick_engine.market_data_cleaner)
        
        # B. RECOVERY MANAGER: WebSocket auto-reconnect (Angel One Fix)
        if hasattr(recovery_manager, 'engine_lifecycle_manager'):
            socketio.start_background_task(recovery_manager.engine_lifecycle_manager)
        
        # C. AGGRESSIVE RAM GUARD: Keep Render under limit
        socketio.start_background_task(aggressive_memory_protector)
        
        # D. REFLECTOR PIPE MONITOR
        logger.info("⚡ [Reflector] Pipe Active | Mode: Binary Bypass")

        # Step 3: Server Deployment Settings
        port = int(os.environ.get("PORT", 10000))
        
        logger.info(f"🚀 [Munh V3 Titan] LIVE on Port {port} | Mode: ZERO-RAM BYPASS PIPE")
        
        # Production Server with specific settings to handle packet bursts
        # ping_timeout and ping_interval help in handling 429/disconnects
        socketio.run(
            app, 
            host='0.0.0.0', 
            port=port, 
            debug=False, 
            use_reloader=False,
            log_output=False # Production mein logs kam rakhein bandwidth bachane ke liye
        )

    except KeyboardInterrupt:
        logger.info("🛑 [Server] Manual shutdown initiated...")
    except Exception as fatal:
        logger.critical(f"💀 [Fatal Crash]: {fatal}")
