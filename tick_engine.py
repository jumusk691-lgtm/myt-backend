from brain import state, logger, socketio, eventlet
import gc
import json

# ==============================================================================
# --- 1. SMART BATCH CONFIG ---
# ==============================================================================
def get_smart_batch_size(total_count):
    """
    Render stability ke liye batch size define karta hai.
    """
    if total_count >= 5000: return 500
    if total_count >= 1000: return 300
    if total_count >= 500: return 100
    return 25  # Default batch size for small watchlists

# Memory buffer for binary stream
tick_buffer = {}

# ==============================================================================
# --- 2. RAM & BUFFER GUARD ---
# ==============================================================================
def market_data_cleaner():
    """
    Har 60 seconds mein memory flush karta hai taaki RAM leak na ho.
    """
    while True:
        eventlet.sleep(60) 
        try:
            # Buffer cleanup agar threshold hit nahi hua
            tick_buffer.clear()
            gc.collect()
            
            # Safe attribute check to prevent 'HuntEngineState' error
            active_tokens = len(getattr(state, 'subscribed_tokens_set', set()))
            active_users = len(getattr(state, 'active_users_list', []))
            
            logger.info(f"⚡ [RAM Guard] Status: {active_tokens} Tokens | {active_users} Users | RAM: OK")
        except Exception as e:
            logger.error(f"⚠️ [Monitor Error]: {e}")

# ==============================================================================
# --- 3. PURE BINARY BATCH REFLECTOR ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """
    AngelOne -> Server -> Binary Buffer -> APK (Reflect Only)
    """
    try:
        token = msg.get('token')
        ltp_raw = msg.get('last_traded_price')
        
        if not token or ltp_raw is None:
            return

        token_str = str(token)
        
        # Unauthorized token filter
        subscribed = getattr(state, 'subscribed_tokens_set', set())
        if token_str not in subscribed:
            return
            
        # Price formatting (AngelOne factor 100)
        price = "{:.2f}".format(float(ltp_raw) / 100)
        
        # 1. ADD TO BUFFER (Wait for batch)
        tick_buffer[token_str] = {"p": price}
        
        # 2. BATCH SIZE LOGIC
        total_tokens = len(subscribed)
        limit = get_smart_batch_size(total_tokens)

        # 3. BINARY EMIT (Jab limit hit ho)
        if len(tick_buffer) >= limit:
            # 'binary=True' forces the payload into a binary frame for speed
            socketio.emit(
                'live_update_batch', 
                tick_buffer, 
                namespace='/', 
                binary=True 
            )
            
            # P2P Relay for Masters in Binary format
            socketio.emit(
                'master_tick_batch', 
                tick_buffer, 
                to='level_1_masters', 
                binary=True
            )
            
            # Flush immediately after emit
            tick_buffer.clear()

    except Exception:
        pass

# ==============================================================================
# --- 4. ENGINE START ---
# ==============================================================================
eventlet.spawn(market_data_cleaner)
logger.info("🚀 [Reflector] Pure Binary Batching Mode Active.")
