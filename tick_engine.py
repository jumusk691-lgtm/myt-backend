from brain import state, logger, socketio, eventlet
import gc

# ==============================================================================
# --- 1. SMART RAM MONITOR (Instead of Data Cleaner) ---
# ==============================================================================
def market_data_cleaner():
    """
    Ab humein data clean karne ki zarurat nahi hai kyunki hum save hi nahi kar rahe!
    Lekin ye function RAM monitor ki tarah kaam karega taaki 12MB limit bani rahe.
    """
    while True:
        eventlet.sleep(60) # Har minute check karo
        try:
            # Forceful Garbage Collection to keep RAM at ~12MB
            gc.collect()
            
            active_count = len(state.subscribed_tokens_set)
            if active_count > 0:
                logger.info(f"⚡ [Bypass Mode] Pipeline Active: {active_count} Tokens | RAM: Stable")
        except Exception as e:
            logger.error(f"⚠️ [Monitor Error]: {e}")

# ==============================================================================
# --- 2. THE BYPASS PIPELINE (Direct Data Receiver) ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """
    PURE BYPASS LOGIC: 
    Data Aaya -> Process Hua -> Seedha Bypass (Emit to Room).
    No Storage. No RAM Usage.
    """
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            
            # Security Check: Agar koi nahi dekh raha toh kyon bhejhein?
            if token not in state.subscribed_tokens_set:
                return

            ltp_raw = msg.get('last_traded_price', 0)
            if ltp_raw <= 0: return
            
            # Price Calculation
            ltp = float(ltp_raw) / 100
            
            # --- THE MAGIC PIPE ---
            # Hum poora batch nahi bhej rahe, sirf single tick bypass kar rahe hain.
            # 'to=token' ka matlab hai sirf wahi users receive karenge jo is token ke room mein hain.
            
            payload = {
                "t": token,
                "p": "{:.2f}".format(ltp)
            }
            
            # Direct Emit (Bypass)
            socketio.emit('live_tick', payload, to=token)
            
            # Agar Level-1 Master hai toh unhe bhi bhej do (P2P logic ke liye)
            socketio.emit('master_tick', payload, to='level_1_masters')

    except Exception as e:
        logger.error(f"⚠️ [Pipe Crash]: {e}")

# ==============================================================================
# --- 3. PULSE BROADCASTER (REMOVED / COMMENTED OUT) ---
# ==============================================================================
# Iski ab zarurat nahi hai. Pulse loop hi CPU aur RAM khaata hai. 
# Ab data 'Event-Driven' chalega (Jaise hi tick aayega, bypass ho jayega).
def pulse_broadcaster():
    logger.info("ℹ️ [Broadcaster] Disabled. System running on Direct Pipe Mode.")
    return
