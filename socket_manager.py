# socket_manager.py
# File 5: Optimized Socket Connections with P2P Leveling & Auth Fix
# Logic: Smart Persistence - Never resubscribe or clear active tokens.

from brain import state, logger, socketio, request, join_room, leave_room, eventlet
import p2p_distributor

# ==============================================================================
# --- 1. CONNECTION HANDLER (The Entry Point) ---
# ==============================================================================
@socketio.on('connect')
def handle_connect(auth): 
    """Jab user connect ho, use Level assign karo aur room mein daalo"""
    try:
        sid = request.sid
        
        # P2P Leveling: User ko capacity ke hisaab se level dena
        level = p2p_distributor.assign_node_level(sid)
        state.user_levels[sid] = level 
        
        # Active pool mein add karo
        state.active_users_pool[sid] = True
        
        # Rooms Assignment logic
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
# --- 2. SMART SUBSCRIPTION LOGIC (AngelOne Bridge) ---
# ==============================================================================
@socketio.on('subscribe')
def handle_incoming_subscription(data):
    """
    Watchlist tokens ko process karna.
    Logic: Purane tokens ko yaad rakhega, naye ko add karega.
    """
    watchlist = data.get('watchlist', [])
    if not watchlist: return

    sid = request.sid
    # Batching for AngelOne API
    batch_registry = {1: [], 2: [], 3: [], 5: []}

    for instrument in watchlist:
        token = str(instrument.get('token'))
        exch = str(instrument.get('exch', 'NSE')).upper()
        if not token: continue

        # --- ROOM LOGIC ---
        # User ko uske specific token room mein hamesha join karwao
        join_room(token)
        
        # --- SMART CHECK: ALREADY SUBSCRIBED? ---
        # Agar token pehle se set mein hai, toh AngelOne ko dubara request mat bhejo
        if token not in state.subscribed_tokens_set:
            etype = 1 
            if "MCX" in exch: etype = 5
            elif any(x in exch for x in ["NFO", "FUT", "OPT"]): etype = 2
            
            state.token_metadata[token] = etype
            batch_registry[etype].append(token)
            
            # Yaad rakho ki ye token ab subscribed hai
            state.subscribed_tokens_set.add(token)

    # AngelOne API Call (Sirf naye tokens ke liye)
    if state.is_ws_ready and state.sws:
        for etype, tokens in batch_registry.items():
            if not tokens: continue
            # Split tokens into 500 batches for stability
            for i in range(0, len(tokens), 500):
                final_batch = tokens[i:i+500]
                state.sws.subscribe(f"sub_{etype}_{i}", 1, [{"exchangeType": etype, "tokens": final_batch}])
                eventlet.sleep(0.05) 
                logger.info(f"📡 [AngelOne] Subscribed {len(final_batch)} new tokens for Etype: {etype}")

# ==============================================================================
# --- 3. DISCONNECT & UNSUBSCRIBE (RAM Protection) ---
# ==============================================================================
@socketio.on('disconnect')
def handle_disconnect():
    """User disconnected: Sirf user-specific data saaf karo, tokens nahi!"""
    sid = request.sid
    try:
        if sid in state.active_users_pool:
            del state.active_users_pool[sid]
        
        if sid in state.user_levels:
            del state.user_levels[sid]
            
        logger.info(f"🔌 [Socket] User Disconnected: {sid}")
    except Exception as e:
        logger.error(f"⚠️ [Disconnect Error]: {e}")

@socketio.on('unsubscribe')
def handle_unsubscribe(data):
    """Jab user watchlist se symbol hataye, sirf uska room leave karwao"""
    tokens = data.get('tokens', [])
    for token in tokens:
        leave_room(token)
    # Note: state.subscribed_tokens_set se hum remove NAHI kar rahe hain 
    # taaki doosre users ko data milta rahe aur cache bana rahe.
    logger.info(f"📉 [Unsub] User left {len(tokens)} rooms.")
