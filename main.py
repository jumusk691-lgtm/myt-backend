# main.py
import os, eventlet
from brain import app, socketio, logger, state
import auth_manager, master_db, tick_engine, socket_manager, recovery_manager, historical_manager

def memory_protector():
    """Logic 3 & 18: Cleans RAM to prevent Render crashes"""
    import gc
    while True:
        eventlet.sleep(600)
        gc.collect()
        logger.info(f"🧹 [System] RAM Optimized. Packets: {state.total_packets}")

if __name__ == '__main__':
    # 1. Sync Master Data (First priority)
    master_db.sync_master_data()
    
    # 2. Start Background Tasks
    socketio.start_background_task(tick_engine.pulse_broadcaster)
    socketio.start_background_task(recovery_manager.engine_lifecycle_manager)
    socketio.start_background_task(memory_protector)
    
    # 3. Launch Server
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 [Munh V3 Titan] Booting on port {port}...")
    
    try:
        eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
    except Exception as fatal:
        logger.critical(f"💀 [Server Crash]: {fatal}")
