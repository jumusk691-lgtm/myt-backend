# tick_engine.py
# File 4: Direct Stream + Smart Memory Persistence (Price Freeze Fix)

from brain import state, logger, socketio, eventlet

# ==============================================================================
# --- 1. SMART MARKET CLEANER (Memory Guard) ---
# ==============================================================================
def market_data_cleaner():
    """
    Logic 18: Har 60 second mein purana kachra saaf karta hai.
    Fix: Hum 'global_market_cache' ko yahan se clear NAHI karenge, 
    wo kaam sirf Broadcaster karega taaki data miss na ho.
    """
    while True:
        eventlet.sleep(60) # Interval badha diya taaki performance bani rahe
        try:
            # 1. OHLC Reset: Sirf tabhi saaf karo jab 2000+ stocks ho jayein
            if len(state.live_ohlc) > 2000:
                state.live_ohlc.clear()
                logger.info("🧹 [Smart Cleaner] OHLC Cache Reset (Limit Exceeded).")
                
            # 2. Previous Price Reset: Comparison logic ke liye memory free karna
            if len(state.previous_price) > 2000:
                state.previous_price.clear()
            
            # Note: subscribed_tokens_set ko kabhi touch nahi karna hai.

        except Exception as e:
            logger.error(f"⚠️ [Cleaner Error]: {e}")

# ==============================================================================
# --- 2. DIRECT DATA RECEIVER (AngelOne to Socket) ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """Jaise hi AngelOne se tick aaye, turant direct rooms mein push karo"""
    try:
        # Check if message is a valid tick
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            
            # Validation: Kya ye token hamari active list mein hai?
            if token not in state.subscribed_tokens_set:
                return

            ltp_raw = msg.get('last_traded_price', 0)
            if ltp_raw <= 0: return
            
            # Price Conversion (Paise to Rupee)
            ltp = float(ltp_raw) / 100
            state.total_packets += 1 # Packet counting for logs
            
            # Live OHLC Update (In-Memory)
            if token not in state.live_ohlc:
                state.live_ohlc[token] = {"o": ltp, "h": ltp, "l": ltp, "c": ltp}
            else:
                state.live_ohlc[token]["h"] = max(state.live_ohlc[token]["h"], ltp)
                state.live_ohlc[token]["l"] = min(state.live_ohlc[token]["l"], ltp)

            # High-Speed Data Packet
            data_packet = {
                "t": token,
                "p": "{:.2f}".format(ltp),
                "h": "{:.2f}".format(state.live_ohlc[token]["h"]),
                "l": "{:.2f}".format(state.live_ohlc[token]["l"]),
                "o": "{:.2f}".format(state.live_ohlc[token]["o"])
            }

            # --- [DIRECT INSTANT BROADCAST] ---
            # 1. Specific Token Room: Watchlist aur Chart ko turant update milega
            socketio.emit('live_update', data_packet, to=token)
            
            # 2. P2P Level 1: Masters ko data relay karne ke liye bhejo
            socketio.emit('master_tick', data_packet, to='level_1_masters')
            
            # 3. Cache for Batching: Broadcaster ke liye save karo
            state.global_market_cache[token] = data_packet

    except Exception as e:
        logger.error(f"⚠️ [Tick Engine Error]: {e}")

# ==============================================================================
# --- 3. THE PULSE BROADCASTER (Batch Sync) ---
# ==============================================================================
def pulse_broadcaster():
    """Har 0.5 second mein poori market ka snapshot ek saath bhejta hai"""
    while True:
        try:
            if state.global_market_cache:
                # 1. Snap lena: Taki clearing ke waqt naya data miss na ho
                snap = dict(state.global_market_cache)
                state.global_market_cache.clear() # Sirf yahi ek jagah clear hoga
                
                # 2. Batch Broadcast: Sabhi users ko ek saath sync karna
                socketio.emit('live_update_batch', snap)
                
            eventlet.sleep(0.5) 
        except Exception as e:
            logger.error(f"❌ [Broadcaster Error]: {e}")
            eventlet.sleep(1) # Error aane par 1 second ka gap
