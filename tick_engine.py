from brain import state, logger, socketio, eventlet
import gc
import json

# ==============================================================================
# --- 1. DYNAMIC BATCH CONFIG (Automatic Logic) ---
# ==============================================================================
def get_smart_batch_size(total_count):
    """
    User ke total symbols ke hisaab se batch size decide karta hai.
    """
    if total_count >= 5000: return 500
    if total_count >= 1000: return 300
    if total_count >= 500: return 100
    return 20  # Stability ke liye low count (Render friendly)

# Global memory buffer
tick_buffer = {}

# ==============================================================================
# --- 2. RAM & RAM GUARD MONITOR (CRASH FIXED) ---
# ==============================================================================
def market_data_cleaner():
    """
    Render 512MB guard. Har 60 seconds mein RAM aur Buffer flush karta hai.
    """
    while True:
        eventlet.sleep(60) 
        try:
            tick_buffer.clear()
            gc.collect()
            
            # FIX: 'HuntEngineState' error hatane ke liye getattr use kiya hai
            active_tokens = len(getattr(state, 'subscribed_tokens_set', set()))
            active_users = len(getattr(state, 'active_users_list', []))
            
            logger.info(f"⚡ [RAM Guard] Status: {active_tokens} Tokens | {active_users} Users | RAM: Minimal")
        except Exception as e:
            logger.error(f"⚠️ [Monitor Error]: {e}")

# ==============================================================================
# --- 3. THE SMART REFLECTOR (High Speed + Safe Batching) ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """
    AngelOne -> Server -> APK (Direct Reflector).
    """
    try:
        token = msg.get('token')
        ltp_raw = msg.get('last_traded_price')
        
        if not token or ltp_raw is None:
            return

        token_str = str(token)
        
        # Check if anyone is watching this token
        subscribed = getattr(state, 'subscribed_tokens_set', set())
        if token_str not in subscribed:
            return
            
        # Price formatting (Divide by 100 for AngelOne)
        price = "{:.2f}".format(float(ltp_raw) / 100)
        payload = [token_str, price]

        # 1. DIRECT PUSH: Flicker speed ke liye single tick
        socketio.emit('live_tick', payload, to=token_str)

        # 2. SMART BATCHING: Ek saath heavy data na jaye
        tick_buffer[token_str] = {"p": price}
        
        total_tokens = len(subscribed)
        limit = get_smart_batch_size(total_tokens)

        if len(tick_buffer) >= limit:
            # Render ke liye 'live_update_batch' emit
            socketio.emit('live_update_batch', tick_buffer)
            tick_buffer.clear()

        # 3. P2P RELAY
        socketio.emit('master_tick', payload, to='level_1_masters')

    except Exception:
        pass # Maximum speed ke liye silent skip

# ==============================================================================
# --- 4. ENGINE INITIALIZATION ---
# ==============================================================================
# Background worker for RAM Management
eventlet.spawn(market_data_cleaner)
logger.info("✅ [Smart Engine] Reflector Pipe & RAM Guard Active.")
