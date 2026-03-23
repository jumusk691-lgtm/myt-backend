# tick_engine.py
# File 4: Direct Stream (No Delay) & RAM Auto-Cleaner for 512MB Limit

from brain import state, logger, socketio, eventlet

# ==============================================================================
# --- 1. THE AUTO-CLEANER (The RAM Guard) ---
# ==============================================================================
def market_data_cleaner():
    """Logic 18: Har 15 second mein RAM ko flush karna taaki Render crash na ho"""
    while True:
        eventlet.sleep(15) 
        try:
            # 1. Purane Batch Cache ko poora saaf karo
            state.global_market_cache.clear()
            
            # 2. Agar OHLC data 500 stocks se zyada ho jaye toh reset karo
            # Isse RAM kabhi 512MB cross nahi karegi
            if len(state.live_ohlc) > 500:
                state.live_ohlc.clear()
                logger.info("🧹 [RAM Guard] High-Memory OHLC Reset Done.")
                
            # 3. Purane prices ka record bhi limit mein rakho
            if len(state.previous_price) > 1000:
                state.previous_price.clear()

        except Exception as e:
            logger.error(f"⚠️ [Cleaner Error]: {e}")

# ==============================================================================
# --- 2. THE DIRECT PUSH RECEIVER (No Saving) ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """Jaise hi data aaye, bina save kiye turant bhej do"""
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            
            # Subscribed check (Security & Performance)
            if token not in state.subscribed_tokens_set:
                return

            ltp_raw = msg.get('last_traded_price', 0)
            if ltp_raw <= 0: return
            
            ltp = float(ltp_raw) / 100
            state.total_packets += 1
            
            # Live OHLC Formation (Update in memory)
            if token not in state.live_ohlc:
                state.live_ohlc[token] = {"o": ltp, "h": ltp, "l": ltp, "c": ltp}
            else:
                state.live_ohlc[token]["h"] = max(state.live_ohlc[token]["h"], ltp)
                state.live_ohlc[token]["l"] = min(state.live_ohlc[token]["l"], ltp)

            # Data Packet Taiyar Karo
            data_packet = {
                "t": token,
                "p": "{:.2f}".format(ltp),
                "h": "{:.2f}".format(state.live_ohlc[token]["h"]),
                "l": "{:.2f}".format(state.live_ohlc[token]["l"]),
                "o": "{:.2f}".format(state.live_ohlc[token]["o"])
            }

            # --- DIRECT P2P & ROOM PUSH ---
            # 1. Level 1 Masters ko bhej do (P2P Shared Logic)
            socketio.emit('master_tick', data_packet, to='level_1_masters')

            # 2. Individual Room (Direct Push - No Cache)
            socketio.emit('live_update', data_packet, to=token)
            
            # 3. Batching for backup
            state.global_market_cache[token] = data_packet

    except Exception as e:
        logger.error(f"⚠️ [Direct Engine Error]: {e}")

# ==============================================================================
# --- 3. THE BATCH BROADCASTER (Fast Heartbeat) ---
# ==============================================================================
def pulse_broadcaster():
    """Har 0.5 sec mein saari symbols ka guchha bhejta hai"""
    while True:
        try:
            if state.global_market_cache:
                snap = dict(state.global_market_cache)
                state.global_market_cache.clear()
                
                # SARI SYMBOLS EK SATH
                socketio.emit('live_update_batch', snap)
                
            eventlet.sleep(0.5) 
        except Exception as e:
            logger.error(f"❌ [Broadcaster Error]: {e}")
            eventlet.sleep(1)
