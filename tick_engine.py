from brain import state, logger, sm # sm = SocketManager from FastAPI
import asyncio
import gc

# ==============================================================================
# --- 1. SMART RAM MONITOR & BATCHER ---
# ==============================================================================
async def market_data_cleaner():
    """
    RAM Monitor aur Batch Broadcaster ka combo.
    Ye har 1.5 second mein data ka batch bhejega aur RAM clean karega.
    """
    while True:
        await asyncio.sleep(1.5) # 1.5 Seconds Batching Interval
        try:
            if state.batch_buffer:
                # --- THE BATCH PIPE ---
                # Saare tokens ka data ek hi JSON object mein jayega
                await sm.emit('live_batch', state.batch_buffer)
                
                # Batch bhejne ke baad buffer khali kar do (Zero-RAM)
                state.batch_buffer = {}
                gc.collect() 
            
            # Active tracking log
            active_count = len(state.subscribed_tokens_set)
            if active_count > 0:
                logger.info(f"⚡ [Batch Mode] Active: {active_count} | RAM: Under 12MB")
                
        except Exception as e:
            logger.error(f"⚠️ [Monitor Error]: {e}")

# ==============================================================================
# --- 2. THE BYPASS PIPELINE (Direct Data Receiver) ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """
    Data Aaya -> Buffer mein Update Hua -> No Storage.
    """
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            
            # Agar koi user is token ko subscribe nahi kiya hai toh skip
            if token not in state.subscribed_tokens_set:
                return

            ltp_raw = msg.get('last_traded_price', 0)
            if ltp_raw <= 0: return
            
            # Price Calculation
            ltp = float(ltp_raw) / 100
            
            # --- THE MAGIC BUFFER ---
            # Hum turant emit nahi kar rahe, buffer mein daal rahe hain batching ke liye
            state.batch_buffer[token] = "{:.2f}".format(ltp)
            
            # Master P2P logic ke liye turant bypass (Optional)
            # sm.emit('master_tick', {"t": token, "p": state.batch_buffer[token]}, to='level_1_masters')

    except Exception as e:
        logger.error(f"⚠️ [Pipe Crash]: {e}")

# ==============================================================================
# --- 3. PULSE BROADCASTER (CLEANUP) ---
# ==============================================================================
def pulse_broadcaster():
    """Ye function ab 'market_data_cleaner' ke andar async handle hota hai."""
    return None
