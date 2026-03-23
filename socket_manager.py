# socket_manager.py
# File 5: Optimized Socket Connections with P2P Leveling & Auth Fix

from brain import state, logger, socketio, request, join_room, leave_room, eventlet
import p2p_distributor

# ==============================================================================
# --- 1. CONNECTION HANDLER (The Entry Point) ---
# ==============================================================================
@socketio.on('connect')
def handle_connect(auth): # [FIX] 'auth' argument added to prevent TypeError
    """Jab user connect ho, use Level assign karo aur room mein daalo"""
    try:
        sid = request.sid
        
        # P2P Leveling: User ko capacity ke hisaab se level dena
        # Isse Render par load kam hoga aur data shared distribute hoga
        level = p2p_distributor.assign_node_level(sid)
        state.user_levels[sid] = level 
        
        # Active pool mein add karo
        state.active_users_pool[sid] = True
        
        # Rooms Assignment
        if level == "LEVEL_1":
            join_room("level_1_masters") # Direct child of Render
            logger.info(f"💎 [Master Node] {sid} joined LEVEL_1")
        else:
            join_room("peer_nodes") # Shared child
            logger.info(f"👥 [Peer Node] {sid} joined {level}")

        return True
    except Exception as e:
        logger.error(f"❌ [Socket Connect Error]: {e}")
        return False

# ==============================================================================
# --- 2. SUBSCRIPTION LOGIC (AngelOne Bridge) ---
# ==============================================================================
@socketio.on('subscribe')
def handle_incoming_subscription(data):
    """Watchlist tokens ko AngelOne aur Rooms mein map karna"""
    watchlist = data.get('watchlist', [])
    if not watchlist: return

    sid = request.sid
    # Batching for AngelOne API (To avoid rate limits)
    batch_registry = {1: [], 2: [], 3: [], 5: []}

    for instrument in watchlist:
        token = str(instrument.get('token'))
        exch = str(instrument.get('exch', 'NSE')).upper()
        if not token: continue

        # --- SMART ROOM LOGIC ---
        # User ko uske token room mein daalo (Direct Push ke liye)
        join_room(token)
        
        # --- ANGELONE SUBSCRIPTION LOGIC ---
        # Sirf tabhi subscribe karo agar ye token pehle kisi ne nahi maanga
        if token not in state.subscribed_tokens_set:
            etype = 1 
            if "MCX" in exch: etype = 5
            elif any(x in exch for x in ["NFO", "FUT", "OPT"]): etype = 2
            
            state.token_metadata[token] = etype
            batch_registry[etype].append(token)
            state.subscribed_tokens_set.add(token)

    # AngelOne API Call (If WebSocket is ready)
    if state.is_ws_ready and state.sws:
        for etype, tokens in batch_registry.items():
            if not tokens: continue
            # Split tokens into 500 batches for API stability
            for i in range(0, len(tokens), 500):
                final_batch = tokens[i:i+500]
                state.sws.subscribe(f"sub_{etype}_{i}", 1, [{"exchangeType": etype, "tokens": final_batch}])
                eventlet.sleep(0.05) 

# ==============================================================================
# --- 3. DISCONNECT & UNSUBSCRIBE (RAM Saver) ---
# ==============================================================================
@socketio.on('disconnect')
def handle_disconnect():
    """User ke jaate hi uski memory clear karna (Logic 18)"""
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
    """User jab watchlist se token hataye"""
    tokens = data.get('tokens', [])
    for token in tokens:
        leave_room(token)
    logger.info(f"📉 Unsubscribed: {len(tokens)} tokens from SID: {request.sid}")
