from brain import state, logger, socketio, eventlet
import gc
import json

# ==============================================================================
# --- 1. SMART BATCH CONFIG (Matching APK logic) ---
# ==============================================================================
def get_smart_batch_size(total_count):
    """
    Render bandwidth aur APK stability ke liye size.
    """
    if total_count >= 5000: return 500
    if total_count >= 1000: return 300
    if total_count >= 500: return 100
    return 15  # APK batch size 15 ke saath match kiya hai

# Memory buffer for binary stream
tick_buffer = {}

# ==============================================================================
# --- 2. RAM & BUFFER GUARD ---
# ==============================================================================
def market_data_cleaner():
    """
    Har 60s mein memory flush aur health monitoring.
    """
    while True:
        eventlet.sleep(60) 
        try:
            tick_buffer.clear()
            gc.collect()
            
            # Attribute safety check
            subs = getattr(state, 'subscribed_tokens_set', set())
            users = getattr(state, 'active_users_pool', set())
            
            logger.info(f"⚡ [RAM Guard] Status: {len(subs)} Tokens | {len(users)} Users")
        except Exception as e:
            logger.error(f"⚠️ [Monitor Error]: {e}")

# ==============================================================================
# --- 3. PURE BINARY BATCH REFLECTOR ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """
    AngelOne -> Server -> Binary Batch -> APK.
    APK handler: 'live_update_batch' (Binary Frame)
    """
    try:
        token = msg.get('token')
        ltp_raw = msg.get('last_traded_price')
        
        if not token or ltp_raw is None:
            return

        token_str = str(token)
        subscribed = getattr(state, 'subscribed_tokens_set', set())

        # Filter: Sirf wo tokens jo APK ne mangwaye hain
        if token_str not in subscribed:
            return
            
        # Price Formatting (AngelOne sends price * 100)
        price = "{:.2f}".format(float(ltp_raw) / 100)
        
        # 1. BUFFER UPDATE
        tick_buffer[token_str] = {"p": price}
        
        # 2. BATCH SIZE CHECK
        limit = get_smart_batch_size(len(subscribed))

        # 3. BINARY EMIT (Jab buffer limit tak pahunche)
        if len(tick_buffer) >= limit:
            # IMPORTANT: APK 'ByteArray' check karta hai, isliye UTF-8 encode zaroori hai
            binary_payload = json.dumps(tick_buffer).encode('utf-8')
            
            socketio.emit(
                'live_update_batch', 
                binary_payload, 
                binary=True 
            )
            
            # Flush
            tick_buffer.clear()

    except Exception:
        # High-frequency data mein logs avoid karein performance ke liye
        pass

# ==============================================================================
# --- 4. ENGINE START ---
# ==============================================================================
# Background worker for memory cleanup
eventlet.spawn(market_data_cleaner)
logger.info("🚀 [Reflector] Binary Batching Pipeline: READY")
