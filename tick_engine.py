from brain import state, logger, socketio, eventlet
import gc
import json

# ==============================================================================
# --- 1. DYNAMIC BATCH CONFIG (Automatic Logic) ---
# ==============================================================================
# Ye logic user ke total symbols ke hisaab se batch size set karega
def get_smart_batch_size(total_count):
    if total_count >= 5000: return 500
    if total_count >= 1000: return 300
    if total_count >= 500: return 100
    return 50  # Default for small watchlists

# Global dictionary for batching (No persistence, just memory)
tick_buffer = {}

# ==============================================================================
# --- 2. RAM & RAM GUARD MONITOR ---
# ==============================================================================
def market_data_cleaner():
    """
    Render 512MB limit guard. 
    Har 60 seconds mein RAM aur Buffer flush karega.
    """
    while True:
        eventlet.sleep(60) 
        try:
            # Buffer cleanup to prevent memory leak
            tick_buffer.clear()
            gc.collect()
            
            active_users = len(state.active_users_list)
            active_tokens = len(state.subscribed_tokens_set)
            
            logger.info(f"⚡ [RAM Guard] Status: {active_tokens} Tokens | {active_users} Users | RAM: Minimal")
        except Exception as e:
            logger.error(f"⚠️ [Monitor Error]: {e}")

# ==============================================================================
# --- 3. THE SMART REFLECTOR (Event Driven + Batching) ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """
    AngelOne -> Server -> APK 
    Bypass logic with High-Speed Reflector.
    """
    try:
        token = msg.get('token')
        ltp_raw = msg.get('last_traded_price')
        
        if not token or ltp_raw is None:
            return

        token_str = str(token)
        # Security: Filter unauthorized tokens
        if token_str not in state.subscribed_tokens_set:
            return
            
        # Price Formatting
        price = "{:.2f}".format(float(ltp_raw) / 100)
        payload = [token_str, price]

        # 1. DIRECT PUSH (Single Tick for high-priority rooms)
        # Isse individual symbol ka flicker fast rehta hai
        socketio.emit('live_tick', payload, to=token_str)

        # 2. SMART BATCHING FOR APK (Memory Efficient)
        tick_buffer[token_str] = {"p": price}
        
        # Agar buffer ka size user-defined limit cross kare, toh batch emit karo
        total_tokens = len(state.subscribed_tokens_set)
        limit = get_smart_batch_size(total_tokens)

        if len(tick_buffer) >= limit:
            # Emit batch and clear buffer immediately
            socketio.emit('live_update_batch', tick_buffer)
            tick_buffer.clear()

        # 3. P2P RELAY
        socketio.emit('master_tick', payload, to='level_1_masters')

    except Exception:
        pass # Silent skip for maximum speed

# ==============================================================================
# --- 4. ENGINE INITIALIZATION ---
# ==============================================================================
# Background worker for RAM Management
eventlet.spawn(market_data_cleaner)
logger.info("✅ [Smart Engine] Reflector Pipe & RAM Guard Active.")
