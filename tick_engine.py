# tick_engine.py
# File 4: Handles Live WebSocket Ticks, OHLC Formation & Broadcasting

from brain import state, logger, socketio, eventlet, datetime, IST

# ==============================================================================
# --- 1. THE DATA RECEIVER (ANGELONE TO SERVER) ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """Processes incoming binary packets from AngelOne"""
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            ltp_raw = msg.get('last_traded_price', 0)
            
            if ltp_raw <= 0: return
            
            ltp = float(ltp_raw) / 100
            state.total_packets += 1
            
            # --- Logic: Live OHLC Formation ---
            if token not in state.live_ohlc:
                state.live_ohlc[token] = {"o": ltp, "h": ltp, "l": ltp, "c": ltp}
            else:
                state.live_ohlc[token]["h"] = max(state.live_ohlc[token]["h"], ltp)
                state.live_ohlc[token]["l"] = min(state.live_ohlc[token]["l"], ltp)
                state.live_ohlc[token]["c"] = ltp

            # Previous Price (for Color Change Logic in APK)
            old_val = state.previous_price.get(token, "{:.2f}".format(ltp))
            
            # Creating the Data Packet
            data_packet = {
                "t": token,
                "p": "{:.2f}".format(ltp),
                "lp": old_val,
                "h": "{:.2f}".format(state.live_ohlc[token]["h"]),
                "l": "{:.2f}".format(state.live_ohlc[token]["l"]),
                "o": "{:.2f}".format(state.live_ohlc[token]["o"])
            }
            
            # Percentage Change Logic
            if 'close' in msg and float(msg['close']) > 0:
                cp = float(msg['close']) / 100
                data_packet["pc"] = "{:.2f}".format(((ltp - cp) / cp) * 100)

            # Storing in Global Cache for Broadcaster
            state.global_market_cache[token] = data_packet
            state.previous_price[token] = "{:.2f}".format(ltp)
            
    except Exception as e:
        logger.error(f"⚠️ [Tick Engine] Packet Error: {e}")

# ==============================================================================
# --- 2. THE PULSE BROADCASTER (SERVER TO APK) ---
# ==============================================================================
def pulse_broadcaster():
    """Broadcasts accumulated ticks to all 10,000+ users every 0.5s"""
    logger.info("📡 [Broadcaster] Pulse Heartbeat Started.")
    while True:
        try:
            if state.global_market_cache:
                # Taking a snapshot and clearing the original cache
                snap = dict(state.global_market_cache)
                state.global_market_cache.clear()
                
                # Sending Batch Update (Fastest for 10k users)
                socketio.emit('live_update_batch', snap)
                
                # Optional: Room-specific update (if user is on a single chart)
                for token, data in snap.items():
                    socketio.emit('live_update', data, to=token)
            
            eventlet.sleep(state.heartbeat_gap) # 0.5 Second Delay
        except Exception as e:
            logger.error(f"❌ [Broadcaster] Critical Error: {e}")
            eventlet.sleep(1)
