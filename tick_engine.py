# tick_engine.py
# File 4: Direct Stream (No Delay) & P2P Distribution Logic

from brain import state, logger, socketio, eventlet

# ==============================================================================
# --- 1. THE DIRECT PUSH RECEIVER (No Saving/No Waiting) ---
# ==============================================================================
def on_data_received(wsapp, msg):
    """Jaise hi data aaye, bina save kiye turant bhej do"""
    try:
        if isinstance(msg, dict) and 'token' in msg:
            token = str(msg.get('token'))
            
            # Logic: Sirf wahi process karo jo subscribed hai
            if token not in state.subscribed_tokens_set:
                return

            ltp_raw = msg.get('last_traded_price', 0)
            if ltp_raw <= 0: return
            
            ltp = float(ltp_raw) / 100
            state.total_packets += 1
            
            # Live OHLC Update (Memory mein high/low update karne ke liye)
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

            # --------------------------------------------------------------
            # --- SHARED P2P PUSH LOGIC (Level 1 to All) ---
            # --------------------------------------------------------------
            
            # 1. Direct Push to LEVEL 1 (Master Nodes - First 100 Users)
            # Inko sabse pehle data milega taaki ye aage relay kar sakein
            socketio.emit('master_tick', data_packet, to='level_1_masters')

            # 2. Direct Push to Token Room (For everyone watching this stock)
            # Bina save kiye 'Direct' push ho raha hai
            socketio.emit('live_update', data_packet, to=token)
            
            # 3. Batch Cache (Optional backup for very slow networks)
            state.global_market_cache[token] = data_packet

    except Exception as e:
        logger.error(f"⚠️ [Direct Engine Error]: {e}")

# ==============================================================================
# --- 2. THE BATCH BROADCASTER (Backup Support) ---
# ==============================================================================
def pulse_broadcaster():
    """Ye har 0.5 sec mein ek saath saari symbols ka guchha (Batch) bhejta hai"""
    logger.info("📡 [Broadcaster] Batch-Push Ready.")
    while True:
        try:
            if state.global_market_cache:
                # Snap le kar cache clear karo taaki memory save ho
                snap = dict(state.global_market_cache)
                state.global_market_cache.clear()
                
                # SARI SYMBOLS EK SATH (One-shot Push)
                socketio.emit('live_update_batch', snap)
                
            eventlet.sleep(0.5) # Fast heartbeat
        except Exception as e:
            logger.error(f"❌ [Broadcaster Error]: {e}")
            eventlet.sleep(1)
