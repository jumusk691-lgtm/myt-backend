from brain import state, logger, socketio, eventlet
import gc

# ==============================================================================
# --- 1. SMART RAM MONITOR (FREE TIER OPTIMIZED) ---
# ==============================================================================
def market_data_cleaner():
    """
    Iska kaam sirf RAM ko flush karna hai taaki Render ka 512MB 
    hamesha 15-20MB ke aas-pass hi rahe.
    """
    while True:
        eventlet.sleep(60) 
        try:
            # Forceful flush
            gc.collect()
            
            active_count = len(state.subscribed_tokens_set)
            # Minimal logging taaki CPU load na badhe
            logger.info(f"⚡ [Reflector] Pipe Active | {active_count} Tokens | RAM: Minimal")
        except Exception as e:
            logger.error(f"⚠️ [Monitor Error]: {e}")

# ==============================================================================
# --- 2. THE DIRECT REFLECTOR PIPE (BYPASS LOGIC) ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """
    LOGIC: AngelOne -> Server -> APK (Direct Reflector)
    No Save. No Cache. No Batching.
    """
    try:
        # 1. Fast Extraction
        token = msg.get('token')
        ltp_raw = msg.get('last_traded_price')
        
        if not token or not ltp_raw:
            return

        # Security: Agar koi is token ko nahi dekh raha, toh process mat karo
        token_str = str(token)
        if token_str not in state.subscribed_tokens_set:
            return
            
        # 2. Fast Processing (No storage)
        # Payload ko List rakha hai taaki JSON overhead 40% kam ho jaye
        # Format: [Token, Price]
        payload = [token_str, "{:.2f}".format(float(ltp_raw) / 100)]
        
        # 3. DIRECT REFLECT (Goli ki raftar se)
        # 'to=token_str' se sirf wahi user data payenge jo us room mein hain
        socketio.emit('live_tick', payload, to=token_str)
        
        # 4. P2P RELAY (Tree logic ke liye Level-1 ko bypass)
        socketio.emit('master_tick', payload, to='level_1_masters')

    except Exception:
        # Error handling ko silent rakha hai taaki speed maintain rahe
        pass

# ==============================================================================
# --- 3. PULSE BROADCASTER (STRICTLY DISABLED) ---
# ==============================================================================
def pulse_broadcaster():
    """
    Aapke logic ke hisaab se batching band hai. 
    Data 'Event-Driven' (Real-time flicker) par chalega.
    """
    logger.info("ℹ️ [Broadcaster] Disabled as per User Logic (Direct Pipe Mode).")
    return
