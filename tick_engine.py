from brain import state, logger, socketio, eventlet

# ==============================================================================
# --- 1. SMART MARKET CLEANER ---
# ==============================================================================
def market_data_cleaner():
    while True:
        eventlet.sleep(300) # 5 minute tak badha do, 60 sec bahut jaldi hai
        try:
            if len(state.live_ohlc) > 3000:
                state.live_ohlc.clear()
                state.previous_price.clear()
                logger.info("🧹 [Cleaner] Cache cleared to save RAM.")
        except Exception as e:
            logger.error(f"⚠️ [Cleaner Error]: {e}")

# ==============================================================================
# --- 2. DIRECT DATA RECEIVER (AngelOne to Cache) ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """Data aate hi sirf Cache mein dalo, Emit mat karo (Speed Fix)"""
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            
            if token not in state.subscribed_tokens_set:
                return

            ltp_raw = msg.get('last_traded_price', 0)
            if ltp_raw <= 0: return
            
            ltp = float(ltp_raw) / 100
            
            # OHLC Calculation (Fastest way)
            if token not in state.live_ohlc:
                state.live_ohlc[token] = {"o": ltp, "h": ltp, "l": ltp}
            else:
                if ltp > state.live_ohlc[token]["h"]: state.live_ohlc[token]["h"] = ltp
                if ltp < state.live_ohlc[token]["l"]: state.live_ohlc[token]["l"] = ltp

            # Sirf Cache mein update karo (No Emit here!)
            # Format baad mein broadcaster mein karenge
            state.global_market_cache[token] = {
                "t": token,
                "p": ltp,
                "h": state.live_ohlc[token]["h"],
                "l": state.live_ohlc[token]["l"]
            }

    except Exception as e:
        logger.error(f"⚠️ [Tick Engine Error]: {e}")

# ==============================================================================
# --- 3. THE PULSE BROADCASTER (The Real Speed) ---
# ==============================================================================
def pulse_broadcaster():
    """Har 200ms mein data ko bundle karke feko (Streaming Feel)"""
    while True:
        try:
            if state.global_market_cache:
                # Snap and Clear
                snap = state.global_market_cache.copy()
                state.global_market_cache.clear()
                
                # Sabhi users ko ek saath batch bhej do
                # Android side par 'live_update_batch' listen karo
                socketio.emit('live_update_batch', snap)
                
                # Masters ke liye alag se (Agar zaroori ho)
                socketio.emit('master_tick_batch', snap, to='level_1_masters')

            eventlet.sleep(0.2) # 200ms is perfect for 'Live' feel without lag
        except Exception as e:
            logger.error(f"❌ [Broadcaster Error]: {e}")
            eventlet.sleep(1)
