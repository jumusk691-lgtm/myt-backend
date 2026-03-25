from brain import state, logger, socketio, eventlet
import gc

# ==============================================================================
# --- 1. SMART MARKET CLEANER ---
# ==============================================================================
def market_data_cleaner():
    """
    Har 5 minute mein memory check karta hai. 
    Sirf un symbols ka data hatata hai jo ab watchlist mein nahi hain.
    """
    while True:
        eventlet.sleep(300) 
        try:
            # Subscribed tokens ka snapshot lo
            active_tokens = set(state.subscribed_tokens_set)
            
            # Agar OHLC data bahut badh gaya hai (2000+), toh cleaning karo
            if len(state.live_ohlc) > 2000:
                # Sirf active symbols ka data rakho, baaki saaf kardo
                state.live_ohlc = {k: v for k, v in state.live_ohlc.items() if k in active_tokens}
                state.previous_price = {k: v for k, v in state.previous_price.items() if k in active_tokens}
                
                gc.collect() 
                logger.info(f"🧹 [Smart Flush] Memory cleaned. Active: {len(active_tokens)}")
        except Exception as e:
            logger.error(f"⚠️ [Cleaner Error]: {e}")

# ==============================================================================
# --- 2. DIRECT DATA RECEIVER ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """
    Live ticks ko receive karke Global Cache mein update karta hai.
    """
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            
            # Agar user ne is token ko subscribe nahi kiya, toh ignore karo
            if token not in state.subscribed_tokens_set:
                return

            ltp_raw = msg.get('last_traded_price', 0)
            if ltp_raw <= 0: return
            
            # Price calculation (/100 as per AngelOne/Standard API)
            ltp = float(ltp_raw) / 100
            
            # 1. OHLC Update logic
            if token not in state.live_ohlc:
                state.live_ohlc[token] = {"o": ltp, "h": ltp, "l": ltp}
            else:
                if ltp > state.live_ohlc[token]["h"]: state.live_ohlc[token]["h"] = ltp
                if ltp < state.live_ohlc[token]["l"]: state.live_ohlc[token]["l"] = ltp

            # 2. Global Cache Update (Persistent Format)
            # YAHAN DATA CLEAR NAHI HOGA, UPDATE HOGA
            state.global_market_cache[token] = {
                "t": token,
                "p": "{:.2f}".format(ltp),
                "h": "{:.2f}".format(state.live_ohlc[token]["h"]),
                "l": "{:.2f}".format(state.live_ohlc[token]["l"])
            }

    except Exception as e:
        logger.error(f"⚠️ [Tick Engine Error]: {e}")

# ==============================================================================
# --- 3. THE PULSE BROADCASTER (No-Clear Batch Logic) ---
# ==============================================================================
def pulse_broadcaster():
    """
    Har pulse (500ms) par puri watchlist ka latest data ek sath bhejta hai.
    Isse scrolling ke waqt symbols 'offline' nahi honge.
    """
    while True:
        try:
            # Agar koi token subscribed hai, tabhi kaam karo
            active_tokens = state.subscribed_tokens_set
            
            if active_tokens:
                # 1. Pura Batch Taiyar Karo (Sirf active tokens ka latest price)
                # Hum 'clear()' nahi kar rahe, isliye hamesha last known price rahega
                snap = {
                    t: state.global_market_cache[t] 
                    for t in active_tokens 
                    if t in state.global_market_cache
                }
                
                if snap:
                    # 2. Android APK ko Pura Batch bhejo
                    socketio.emit('live_update_batch', snap)
                    
                    # 3. Master Dashboard ko bhejo
                    socketio.emit('master_tick_batch', snap, to='level_1_masters')
                    
                    # logger.info(f"📤 Sent Batch: {len(snap)} symbols")

            # Render stability ke liye 0.5s best hai
            eventlet.sleep(0.5) 
            
        except Exception as e:
            logger.error(f"❌ [Broadcaster Error]: {e}")
            eventlet.sleep(1)
