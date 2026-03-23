# main.py (Aggressive RAM Management)
import os, eventlet, gc
from brain import app, socketio, logger, state
import auth_manager, master_db, tick_engine, socket_manager, recovery_manager, historical_manager, p2p_manager

def aggressive_memory_protector():
    """Logic: Hard-Reset RAM every 10 seconds to stay under 512MB"""
    while True:
        eventlet.sleep(10) # 10 second ka loop
        try:
            # 1. Python ka Garbage Collector chalao
            gc.collect()
            
            # 2. State mein jo faltu lists ban rahi hain unhe limit karo
            if len(state.subscribed_tokens_set) > 2000:
                # Agar 2000 se zyada tokens ho jayein toh purane clear karo
                state.subscribed_tokens_set.clear()
                
            logger.info(f"⚡ [RAM Guard] Memory Flushed. Active Packets: {state.total_packets}")
        except:
            pass

if __name__ == '__main__':
    master_db.sync_master_data()
    
    # Sabhi background tasks start karo
    socketio.start_background_task(tick_engine.pulse_broadcaster)
    socketio.start_background_task(tick_engine.market_data_cleaner) # Naya Cleaner
    socketio.start_background_task(recovery_manager.engine_lifecycle_manager)
    socketio.start_background_task(aggressive_memory_protector) # Aggressive Guard
    
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 [Munh V3] Titan is Running with 512MB RAM Protection...")
    
    try:
        eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
    except Exception as fatal:
        logger.critical(f"💀 [Server Crash]: {fatal}")
