# socket_manager.py
# File 5: Optimized Socket Connections with P2P Leveling & Auth Fix
# Logic: Smart Persistence + Universal Exchange Support

from brain import state, logger, socketio, request, join_room, leave_room, eventlet
import p2p_distributor

# ==============================================================================
# --- 1. CONNECTION HANDLER (The Entry Point) ---
# ==============================================================================
@socketio.on('connect')
def handle_connect(auth=None): # Fix: Added auth=None to prevent TypeError
    """Jab user connect ho, use Level assign karo aur room mein daalo"""
    try:
        sid = request.sid
        
        # P2P Leveling Logic
        level = p2p_distributor.assign_node_level(sid)
        state.user_levels[sid] = level 
        state.active_users_pool[sid] = True
        
        if level == "LEVEL_1":
            join_room("level_1_masters") 
            logger.info(f"💎 [Master Node] {sid} joined LEVEL_1")
        else:
            join_room("peer_nodes") 
            logger.info(f"👥 [Peer Node] {sid} joined {level}")

        return True
    except Exception as e:
        logger.error(f"❌ [Socket Connect Error]: {e}")
        return False

# ==============================================================================
# --- 2. SMART MULTI-EXCHANGE SUBSCRIPTION LOGIC ---
# ==============================================================================
@socketio.on('subscribe')
def handle_incoming_subscription(data):
    """
    NSE, BSE, MCX, NFO ke liye full automated logic.
    Logic: SBIN (Cash) aur Nifty (Futures) dono ko sahi etype dena.
    """
    watchlist = data.get('watchlist', [])
    if not watchlist: return

    sid = request.sid
    # Batching for AngelOne (1=NSE/BSE Cash, 2=NFO, 5=MCX)
    batch_registry = {1: [], 2: [], 5: []}

    for instrument in watchlist:
        token = str(instrument.get('token'))
        exch = str(instrument.get('exch', 'NSE')).upper()
        symbol = str(instrument.get('symbol', '')).upper()
        if not token: continue

        # --- ROOM ASSIGNMENT ---
        join_room(token)
        
        # --- SMART EXCHANGE DETECTION ---
        if token not in state.subscribed_tokens_set:
            # 1. MCX Detection (Gold/Crude)
            if "MCX" in exch:
                etype = 5
            # 2. NFO Detection (Nifty/BankNifty Futures)
            elif any(x in symbol for x in ["FUT", "CE", "PE"]) or "NFO" in exch:
                etype = 2
            # 3. NSE/BSE Cash (SBIN, Reliance)
            else:
                etype = 1
            
            state.token_metadata[token] = etype
            batch_registry[etype].append(token)
            state.subscribed_tokens_set.add(token)

    # AngelOne API Call (Sirf naye tokens ke liye)
    if state.is_ws_ready and state.sws:
        for etype, tokens in batch_registry.items():
            if not tokens: continue
            for i in range(0, len(tokens), 500):
                final_batch = tokens[i:i+500]
                state.sws.subscribe(f"sub_{etype}_{i}", 1, [{"exchangeType": etype, "tokens": final_batch}])
                eventlet.sleep(0.05) 
                logger.info(f"📡 [AngelOne] Subscribed {len(final_batch)} tokens for Etype: {etype}")

# ==============================================================================
# --- 3. DISCONNECT & UNSUBSCRIBE ---
# ==============================================================================
@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    try:
        if sid in state.active_users_pool: del state.active_users_pool[sid]
        if sid in state.user_levels: del state.user_levels[sid]
        logger.info(f"🔌 [Socket] User Disconnected: {sid}")
    except Exception as e:
        logger.error(f"⚠️ [Disconnect Error]: {e}")

@socketio.on('unsubscribe')
def handle_unsubscribe(data):
    tokens = data.get('tokens', [])
    for token in tokens:
        leave_room(token)
    logger.info(f"📉 [Unsub] User left {len(tokens)} rooms.")
