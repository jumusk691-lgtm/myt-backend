from brain import state, logger, socketio, eventlet
import gc

# ==============================================================================
# --- 1. SMART MARKET CLEANER (The Safe Flush) ---
# ==============================================================================
def market_data_cleaner():
    """Sirf Kachra saaf karega, Subscribed Tokens ko kabhi nahi chherega"""
    while True:
        eventlet.sleep(300) # 5 Minute interval
        try:
            # Check if RAM usage is high (approx by dict sizes)
            if len(state.live_ohlc) > 2000 or len(state.previous_price) > 2000:
                # SELECTIVE FLUSH:
                # Hum 'state.subscribed_tokens_set' ko clear NAHI kar rahe.
                state.live_ohlc.clear()
                state.previous_price.clear()
                
                # Python ko bolo force cleanup kare
                gc.collect() 
                logger.info(f"🧹 [Safe Flush] Memory freed. Active Tokens Preserved: {len(state.subscribed_tokens_set)}")
        except Exception as e:
            logger.error(f"⚠️ [Cleaner Error]: {e}")

# ==============================================================================
# --- 2. DIRECT DATA RECEIVER (Ultra Fast) ---
# ==============================================================================
def on_data_received(wsapp, msg):
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            
            # Security: Sirf wahi tokens jo user ne mange hain
            if token not in state.subscribed_tokens_set:
                return

            ltp_raw = msg.get('last_traded_price', 0)
            if ltp_raw <= 0: return
            
            ltp = float(ltp_raw) / 100
            
            # Fast OHLC (Bina formatting ke, formatting broadcaster mein hogi)
            if token not in state.live_ohlc:
                state.live_ohlc[token] = {"o": ltp, "h": ltp, "l": ltp}
            else:
                if ltp > state.live_ohlc[token]["h"]: state.live_ohlc[token]["h"] = ltp
                if ltp < state.live_ohlc[token]["l"]: state.live_ohlc[token]["l"] = ltp

            # Direct update into cache (No Copying here)
            state.global_market_cache[token] = {
                "t": token,
                "p": "{:.2f}".format(ltp),
                "h": "{:.2f}".format(state.live_ohlc[token]["h"]),
                "l": "{:.2f}".format(state.live_ohlc[token]["l"])
            }

    except Exception as e:
        logger.error(f"⚠️ [Tick Engine Error]: {e}")

# ==============================================================================
# --- 3. THE PULSE BROADCASTER (Zero-Lag Batching) ---
# ==============================================================================
def pulse_broadcaster():
    while True:
        try:
            if state.global_market_cache:
                # Atomic Snap: Cache ko khali karo aur data le lo bina RAM spike ke
                # 'popitem' ya direct swap use karna better hai fast processing ke liye
                batch_data = state.global_market_cache
                state.global_market_cache = {} # Naya empty dict assign karo (Fastest)
                
                # Broadcast to everyone
                socketio.emit('live_update_batch', batch_data)
                
                # Master node sync
                socketio.emit('master_tick_batch', batch_data, to='level_1_masters')

            eventlet.sleep(0.25) # 250ms is the sweet spot for Render
        except Exception as e:
            logger.error(f"❌ [Broadcaster Error]: {e}")
            eventlet.sleep(1)
