from brain import state, logger, socketio, eventlet
import gc

# ==============================================================================
# --- 1. SMART MARKET CLEANER ---
# ==============================================================================
def market_data_cleaner():
    while True:
        eventlet.sleep(300) 
        try:
            # Sirf OHLC aur Comparisons saaf karo
            if len(state.live_ohlc) > 2000:
                state.live_ohlc.clear()
                state.previous_price.clear()
                gc.collect() 
                logger.info("🧹 [Safe Flush] Temporary OHLC cleared.")
        except Exception as e:
            logger.error(f"⚠️ [Cleaner Error]: {e}")

# ==============================================================================
# --- 2. DIRECT DATA RECEIVER ---
# ==============================================================================
def on_data_received(wsapp, msg):
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            
            # CRITICAL: Token check logic
            if token not in state.subscribed_tokens_set:
                return

            ltp_raw = msg.get('last_traded_price', 0)
            if ltp_raw <= 0: return
            
            ltp = float(ltp_raw) / 100
            
            # Fast OHLC
            if token not in state.live_ohlc:
                state.live_ohlc[token] = {"o": ltp, "h": ltp, "l": ltp}
            else:
                if ltp > state.live_ohlc[token]["h"]: state.live_ohlc[token]["h"] = ltp
                if ltp < state.live_ohlc[token]["l"]: state.live_ohlc[token]["l"] = ltp

            # Update Global Cache (Consistent Format)
            state.global_market_cache[token] = {
                "t": token,
                "p": "{:.2f}".format(ltp),
                "h": "{:.2f}".format(state.live_ohlc[token]["h"]),
                "l": "{:.2f}".format(state.live_ohlc[token]["l"])
            }

    except Exception as e:
        logger.error(f"⚠️ [Tick Engine Error]: {e}")

# ==============================================================================
# --- 3. THE PULSE BROADCASTER (Fixed Reference) ---
# ==============================================================================
def pulse_broadcaster():
    while True:
        try:
            # Agar data hai tabhi aage badho
            if state.global_market_cache:
                # 1. Snapshot lo aur clear karo (Bina reference tode)
                snap = state.global_market_cache.copy()
                state.global_market_cache.clear() 
                
                # 2. Android ko bhej do
                socketio.emit('live_update_batch', snap)
                
                # 3. Master ko bhej do
                socketio.emit('master_tick_batch', snap, to='level_1_masters')
                
                # Logger for debugging (Sirf development ke liye)
                # logger.info(f"📤 Sent {len(snap)} ticks")

            eventlet.sleep(0.4) # Render ke liye 0.4s (400ms) best hai
        except Exception as e:
            logger.error(f"❌ [Broadcaster Error]: {e}")
            eventlet.sleep(1)
