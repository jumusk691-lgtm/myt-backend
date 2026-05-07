# main.py
import os, eventlet, gc, json
eventlet.monkey_patch(all=True)

from brain import app, socketio, logger, state
import auth_manager, master_db, tick_engine, socket_manager, recovery_manager

# ==============================================================================
# --- 1. COMPATIBILITY & MEMORY LAYER ---
# ==============================================================================
# Logs mein 'active_users_list' error na aaye isliye legacy support
state.active_users_pool = getattr(state, 'active_users_pool', set())
state.active_users_list = state.active_users_pool 

def aggressive_memory_protector():
    """Har 60s mein RAM saaf karta hai aur health log dikhata hai."""
    while True:
        eventlet.sleep(60)
        try:
            gc.collect()
            subs = len(getattr(state, 'subscribed_tokens_set', []))
            users = len(state.active_users_pool)
            logger.info(f"⚡ [Health] Tokens: {subs} | Users: {users} | RAM Cleaned")
            
            # Anti-Crash: Agar users bahut zyada badh jayein Render par
            if users > 1000:
                state.active_users_pool.clear()
        except Exception as e:
            logger.error(f"⚠️ [RAM Guard Error]: {e}")

# ==============================================================================
# --- 2. DATA EMITTER (Binary Compression) ---
# ==============================================================================
def emit_binary_batch(event, data, room=None):
    """'Too many packets' error fix karne ke liye binary emit."""
    try:
        binary_payload = json.dumps(data).encode('utf-8')
        if room:
            socketio.emit(event, binary_payload, room=room)
        else:
            socketio.emit(event, binary_payload)
    except Exception as e:
        logger.error(f"❌ [Emit Error]: {e}")

# ==============================================================================
# --- 3. RE-SYNC ENDPOINT (Direct Push URL) ---
# ==============================================================================
@app.route('/api/add_token', methods=['POST'])
def add_new_token_direct():
    """APK se direct token receive karke subscribe karta hai."""
    try:
        data = request.json
        token = str(data.get('token'))
        exch = int(data.get('exch', 1))
        
        if token:
            state.subscribed_tokens_set.add(token)
            state.token_metadata[token] = exch
            
            if state.sws and state.is_ws_ready:
                state.sws.subscribe("myt_direct", 1, [{"exchangeType": exch, "tokens": [token]}])
                return jsonify({"status": "success", "token": token})
        return jsonify({"status": "error", "msg": "Failed"}), 400
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 500

# ==============================================================================
# --- 4. EXECUTION ---
# ==============================================================================
if __name__ == '__main__':
    try:
        # Step 1: Master Sync
        logger.info("📡 [Master DB] Syncing from Supabase...")
        master_db.sync_master_data()
        
        # Step 2: Background Tasks Launch
        socketio.start_background_task(aggressive_memory_protector)
        
        if hasattr(recovery_manager, 'engine_lifecycle_manager'):
            socketio.start_background_task(recovery_manager.engine_lifecycle_manager)
            
        if hasattr(tick_engine, 'market_data_cleaner'):
            socketio.start_background_task(tick_engine.market_data_cleaner)

        # Step 3: Run Server
        port = int(os.environ.get("PORT", 10000))
        logger.info(f"🚀 [Munh V3 Titan] LIVE | Port: {port} | Zero-RAM Mode")
        
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
