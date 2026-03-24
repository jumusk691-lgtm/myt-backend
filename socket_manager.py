# socket_manager.py
# File 5: Universal Multi-Exchange Socket (No-Polling / Unlimited Mode)
# Support: NSE Cash, NFO (Nifty/BankNifty), MCX (Gold/Crude), BSE/BFO

from brain import state, logger, socketio, request, join_room, leave_room, eventlet
import p2p_distributor

# ==============================================================================
# --- 1. CONNECTION HANDLER (Auth Fix & P2P) ---
# ==============================================================================
@socketio.on('connect')
def handle_connect(auth=None): 
    """User connect hote hi use Level assign karo taaki load balance rahe"""
    try:
        sid = request.sid
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
# --- 2. SMART MULTI-SEGMENT SUBSCRIPTION (Unlimited Logic) ---
# ==============================================================================
@socketio.on('subscribe')
def handle_incoming_subscription(data):
    """
    NSE, BSE, MCX, NFO ke liye full automated logic.
    Logic: Symbol name aur Exch se sahi Etype (1, 2, 3, 5) select karna.
    """
    watchlist = data.get('watchlist', [])
    if not watchlist: return

    sid = request.sid
    # Batching for AngelOne (1=NSE/BSE Cash, 2=NFO, 3=BFO, 5=MCX)
    batch_registry = {1: [], 2: [], 3: [], 5: []}

    for instrument in watchlist:
        token = str(instrument.get('token'))
        exch = str(instrument.get('exch', 'NSE')).upper()
        symbol = str(instrument.get('symbol', '')).upper()
        if not token or token == "None": continue

        # --- ROOM LOGIC ---
        # User ko token-specific room mein join karwao taaki use live ticks milein
        join_room(token)
        
        # --- SMART EXCHANGE DETECTION (CRITICAL) ---
        # Agar token pehle se live nahi hai, tabhi AngelOne ko request bhejo
        if token not in state.subscribed_tokens_set:
            # A. MCX (Commodity)
            if "MCX" in exch:
                etype = 5
            # B. NFO (Nifty/BankNifty Futures & Options)
            elif any(x in symbol for x in ["FUT", "CE", "PE"]) or "NFO" in exch:
                etype = 2
            # C. BFO (BSE Derivatives - Sensex/Bankex)
            elif "BFO" in exch or ("BSE" in exch and any(x in symbol for x in ["CE", "PE", "FUT"])):
                etype = 3
            # D. CASH (NSE/BSE Equity - SBIN, Reliance)
            else:
                etype = 1
            
            state.token_metadata[token] = etype
            batch_registry[etype].append(token)
            state.subscribed_tokens_set.add(token)

    # --- ANGELONE API BATCH EXECUTION ---
    if state.is_ws_ready and state.sws:
        for etype, tokens in batch_registry.items():
            if not tokens: continue
            
            # Unlimited scalability ke liye 500 ke batches (AngelOne Limit)
            for i in range(0, len(tokens), 500):
                final_batch = tokens[i:i+500]
                # 'myt' is your specific correlation ID
                state.sws.subscribe("myt_unlimited", 1, [{"exchangeType": etype, "tokens": final_batch}])
                
                # Chhota delay taaki API rate limit hit na ho
                eventlet.sleep(0.05) 
                logger.info(f"📡 [AngelOne] Live: {len(final_batch)} tokens for Etype: {etype}")

# ==============================================================================
# --- 3. DISCONNECT & UNSUBSCRIBE (Memory Safety) ---
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
    """Room leave karwao par cache memory mein rehne do doosre users ke liye"""
    tokens = data.get('tokens', [])
    for token in tokens:
        leave_room(token)
    logger.info(f"📉 [Unsub] User left {len(tokens)} rooms.")
