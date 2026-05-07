# main.py
import os, eventlet, gc, json
eventlet.monkey_patch(all=True)

from brain import app, socketio, logger, state, request, jsonify
import auth_manager, master_db, tick_engine, socket_manager, recovery_manager

# ==============================================================================
# --- 1. ATTRIBUTE & MEMORY ALIGNMENT ---
# ==============================================================================
# APK ke 'active_users_pool' aur existing logic ko sync karne ke liye
state.active_users_pool = getattr(state, 'active_users_pool', set())
state.active_users_list = state.active_users_pool 

def aggressive_memory_protector():
    """Har 60s mein RAM aur stale connections saaf karta hai."""
    while True:
        eventlet.sleep(60)
        try:
            gc.collect()
            subs = len(getattr(state, 'subscribed_tokens_set', set()))
            users = len(state.active_users_pool)
            logger.info(f"⚡ [RAM Guard] Tokens: {subs} | Active Users: {users}")
            
            if users > 500: # Render Free Tier safety
                state.active_users_pool.clear()
        except Exception as e:
            logger.error(f"⚠️ [RAM Guard Error]: {e}")

# ==============================================================================
# --- 2. APK-MATCHED DATA EMITTER ---
# ==============================================================================
def emit_binary_batch(event, data, room=None):
    """
    APK ke 'live_update_batch' handler ke saath match karta hai.
    Data ko UTF-8 Binary mein convert karta hai.
    """
    try:
        # APK side par data is ByteArray check laga hai, isliye encode zaroori hai
        binary_payload = json.dumps(data).encode('utf-8')
        if room:
            socketio.emit(event, binary_payload, room=room)
        else:
            socketio.emit(event, binary_payload)
    except Exception as e:
        logger.error(f"❌ [Binary Emit Error]: {e}")

# Global mapping taaki tick_engine binary use kare
socket_manager.emit_binary_batch = emit_binary_batch

# ==============================================================================
# --- 3. DIRECT TOKEN SYNC (APK Support) ---
# ==============================================================================
@app.route('/api/add_token', methods=['POST'])
def add_new_token_direct():
    """
    Jab APK ka HealthCheck ya Re-sync trigger ho, tab ye endpoint use hoga.
    """
    try:
        data = request.json
        token = str(data.get('token'))
        exch = int(data.get('exch', 1))
        
        if not token:
            return jsonify({"status": "error", "msg": "No token"}), 400

        # RAM update
        state.subscribed_tokens_set.add(token)
        state.token_metadata[token] = exch
        
        # Immediate Subscription if Engine is Live
        if getattr(state, 'sws', None) and getattr(state, 'is_ws_ready', False):
            state.sws.subscribe("myt_direct", 1, [{"exchangeType": exch, "tokens": [token]}])
            logger.info(f"✅ [Direct Sync] Token {token} Active")
            return jsonify({"status": "success", "token": token})
            
        return jsonify({"status": "pending", "msg": "WS Not Ready, Token Saved"}), 202
    except Exception as e:
        logger.error(f"❌ [API Error]: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

# ==============================================================================
# --- 4. LAUNCHER ---
# ==============================================================================
if __name__ == '__main__':
    try:
        # Step 1: Initialize Database
        logger.info("📡 [Master DB] Syncing Scrip Master...")
        master_db.sync_master_data()
        
        # Step 2: Start Background Workers
        socketio.start_background_task(aggressive_memory_protector)
        
        if hasattr(recovery_manager, 'engine_lifecycle_manager'):
            socketio.start_background_task(recovery_manager.engine_lifecycle_manager)
            
        if hasattr(tick_engine, 'market_data_cleaner'):
            socketio.start_background_task(tick_engine.market_data_cleaner)

        # Step 3: Deployment
        port = int(os.environ.get("PORT", 10000))
        logger.info(f"🚀 [Munh V3 Titan] LIVE | Port: {port} | Binary Pipeline: ON")
        
        socketio.run(
            app, 
            host='0.0.0.0', 
            port=port, 
            debug=False, 
            use_reloader=False,
            log_output=False
        )

    except Exception as fatal:
        logger.critical(f"💀 [Fatal Crash]: {fatal}")
