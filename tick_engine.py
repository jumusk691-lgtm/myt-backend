# tick_engine.py
# File 4: Optimized for One-Shot Batch Push & Subscribed Filtering

from brain import state, logger, socketio, eventlet

# ==============================================================================
# --- 1. DATA RECEIVER (AngelOne to Server) ---
# ==============================================================================
def on_data_received(wsapp, msg):
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            
            # Sirf wahi data process karo jo kisi user ne subscribe kiya hai (Logic 13)
            if token not in state.subscribed_tokens_set:
                return

            ltp_raw = msg.get('last_traded_price', 0)
            if ltp_raw <= 0: return
            
            ltp = float(ltp_raw) / 100
            state.total_packets += 1
            
            # OHLC Formation
            if token not in state.live_ohlc:
                state.live_ohlc[token] = {"o": ltp, "h": ltp, "l": ltp, "c": ltp}
            else:
                state.live_ohlc[token]["h"] = max(state.live_ohlc[token]["h"], ltp)
                state.live_ohlc[token]["l"] = min(state.live_ohlc[token]["l"], ltp)
                state.live_ohlc[token]["c"] = ltp

            # Data Packet taiyar karna
            data_packet = {
                "t": token,
                "p": "{:.2f}".format(ltp),
                "h": "{:.2f}".format(state.live_ohlc[token]["h"]),
                "l": "{:.2f}".format(state.live_ohlc[token]["l"]),
                "o": "{:.2f}".format(state.live_ohlc[token]["o"])
            }
            
            # Global Cache mein save karo (Broadcaster ke liye)
            state.global_market_cache[token] = data_packet
            
    except Exception as e:
        logger.error(f"⚠️ [Tick Engine] Error: {e}")

# ==============================================================================
# --- 2. THE MASTER PUSH (Every 1 Second - Single Packet) ---
# ==============================================================================
def pulse_broadcaster():
    """Saare symbols ka price ek saath push karta hai"""
    logger.info("📡 [Broadcaster] Single-Push Heartbeat Started.")
    while True:
        try:
            if state.global_market_cache:
                # 1. Poore Cache ka snapshot lo
                full_batch = dict(state.global_market_cache)
                
                # 2. Cache ko turant clear karo
                state.global_market_cache.clear()
                
                # 3. EK BAR MEIN SARI SYMBOLS (Logic 13)
                # Ye ek hi JSON packet mein 100+ tokens bhej dega
                socketio.emit('live_update_batch', full_batch)
                
                # Agar koi user specific room mein hai (Chart ke liye), wahan bhi bhej do
                for token, data in full_batch.items():
                    socketio.emit('live_update', data, to=token)

            # 1 second ka gap (Aapne 'her sec' bola tha)
            eventlet.sleep(1) 
            
        except Exception as e:
            logger.error(f"❌ [Broadcaster] Critical Error: {e}")
            eventlet.sleep(1)
