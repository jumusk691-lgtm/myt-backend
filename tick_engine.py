# tick_engine.py
# File 4: Direct Stream + Smart Memory Persistence (Purane Tokens Fix)

from brain import state, logger, socketio, eventlet

# ==============================================================================
# --- 1. SMART MARKET CLEANER (Memory Guard) ---
# ==============================================================================
def market_data_cleaner():
    """
    Logic 18: Har 30 second mein sirf 'Kachra' saaf karta hai.
    Important: Hum 'subscribed_tokens_set' ko touch nahi karenge taaki price chalta rahe.
    """
    while True:
        eventlet.sleep(30) # 30 second gap is better for performance
        try:
            # 1. Batch Cache ko clear karo (Kyunki ye har 0.5s bhej diya jata hai)
            state.global_market_cache.clear()
            
            # 2. OHLC Reset: Agar 1000 se zyada stocks ka OHLC jama ho gaya hai
            # Sirf tabhi saaf karo jab RAM khatre mein ho
            if len(state.live_ohlc) > 1000:
                state.live_ohlc.clear()
                logger.info("🧹 [Smart Cleaner] OHLC Cache Reset to save RAM.")
                
            # 3. Previous Price Reset: Comparison logic ke liye limit
            if len(state.previous_price) > 2000:
                state.previous_price.clear()

        except Exception as e:
            logger.error(f"⚠️ [Cleaner Error]: {e}")

# ==============================================================================
# --- 2. DIRECT DATA RECEIVER (AngelOne to Socket) ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """Jaise hi AngelOne se tick aaye, turant bina ruke push karo"""
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            
            # Validation: Sirf unhi tokens ko process karo jo koi dekh raha hai
            if token not in state.subscribed_tokens_set:
                return

            ltp_raw = msg.get('last_traded_price', 0)
            if ltp_raw <= 0: return
            
            ltp = float(ltp_raw) / 100
            state.total_packets += 1
            
            # Live OHLC Formation (In-Memory Processing)
            if token not in state.live_ohlc:
                state.live_ohlc[token] = {"o": ltp, "h": ltp, "l": ltp, "c": ltp}
            else:
                state.live_ohlc[token]["h"] = max(state.live_ohlc[token]["h"], ltp)
                state.live_ohlc[token]["l"] = min(state.live_ohlc[token]["l"], ltp)

            # Data Packet Structure (Smallest size for 4G/5G speed)
            data_packet = {
                "t": token,
                "p": "{:.2f}".format(ltp),
                "h": "{:.2f}".format(state.live_ohlc[token]["h"]),
                "l": "{:.2f}".format(state.live_ohlc[token]["l"]),
                "o": "{:.2f}".format(state.live_ohlc[token]["o"])
            }

            # --- [DIRECT P2P BROADCAST] ---
            
            # A. LEVEL 1 MASTERS: 100 users ko direct (Shared P2P base)
            socketio.emit('master_tick', data_packet, to='level_1_masters')

            # B. INDIVIDUAL TOKEN ROOM: Chart aur watchlist ke liye fast update
            socketio.emit('live_update', data_packet, to=token)
            
            # C. BATCH CACHE: Next pulse ke liye save karo
            state.global_market_cache[token] = data_packet

    except Exception as e:
        logger.error(f"⚠️ [Tick Engine Error]: {e}")

# ==============================================================================
# --- 3. THE PULSE BROADCASTER (Backup Batch) ---
# ==============================================================================
def pulse_broadcaster():
    """Har 0.5 second mein saari symbols ek saath (Batch) push karta hai"""
    while True:
        try:
            if state.global_market_cache:
                # Snap lekar cache free karna zaroori hai
                snap = dict(state.global_market_cache)
                state.global_market_cache.clear()
                
                # SARI SYMBOLS EK SATH PUSH (Logic 13)
                socketio.emit('live_update_batch', snap)
                
            eventlet.sleep(0.5) 
        except Exception as e:
            logger.error(f"❌ [Broadcaster Error]: {e}")
            eventlet.sleep(1)
