from brain import state, logger, socketio, request, join_room, leave_room, eventlet
import p2p_distributor

# ==============================================================================
# --- 1. CONNECTION HANDLER (Stable Connection) ---
# ==============================================================================
@socketio.on('connect')
def handle_connect(auth=None): 
    try:
        sid = request.sid
        # Level assigning logic
        level = p2p_distributor.assign_node_level(sid)
        state.user_levels[sid] = level 
        state.active_users_pool[sid] = True
        
        # Initialize user subscription set if not exists
        if sid not in state.user_subscriptions:
            state.user_subscriptions[sid] = set()

        if level == "LEVEL_1":
            join_room("level_1_masters") 
            logger.info(f"💎 [Master] {sid} connected to LEVEL_1")
        else:
            join_room("peer_nodes") 
            logger.info(f"👥 [Peer] {sid} connected to {level}")

        return True
    except Exception as e:
        logger.error(f"❌ [Socket Connect Error]: {e}")
        return False

# ==============================================================================
# --- 2. SMART SUBSCRIPTION (Memory-Aware Logic) ---
# ==============================================================================
@socketio.on('subscribe')
def handle_incoming_subscription(data):
    watchlist = data.get('watchlist', [])
    if not watchlist: return

    sid = request.sid
    batch_registry = {1: [], 2: [], 3: [], 5: []}

    for instrument in watchlist:
        token = str(instrument.get('token'))
        exch = str(instrument.get('exch', 'NSE')).upper()
        symbol = str(instrument.get('symbol', '')).upper()
        
        if not token or token == "None": continue

        # 1. Room Join: User ko live updates milne shuru honge
        join_room(token)
        state.user_subscriptions[sid].add(token)

        # 2. Ref Count: Track users per token
        state.token_ref_count[token] = state.token_ref_count.get(token, 0) + 1
        
        # 3. New Token Check: Agar server ke paas ye token naya hai
        if token not in state.subscribed_tokens_set:
            if "MCX" in exch: etype = 5
            elif any(x in symbol for x in ["FUT", "CE", "PE"]) or "NFO" in exch: etype = 2
            elif "BFO" in exch or ("BSE" in exch and any(x in symbol for x in ["CE", "PE", "FUT"])): etype = 3
            else: etype = 1
            
            state.token_metadata[token] = etype
            batch_registry[etype].append(token)
            state.subscribed_tokens_set.add(token)

    # AngelOne API Call (Batching)
    if state.is_ws_ready and state.sws:
        for etype, tokens in batch_registry.items():
            if not tokens: continue
            # Split into batches of 500 (API Limit)
            for i in range(0, len(tokens), 500):
                final_batch = tokens[i:i+500]
                state.sws.subscribe("myt_unlimited", 1, [{"exchangeType": etype, "tokens": final_batch}])
                eventlet.sleep(0.05) 
                logger.info(f"📡 [AngelOne] Subscribed: {len(final_batch)} tokens for Etype: {etype}")

# ==============================================================================
# --- 3. AUTO-CLEANUP (Selective Flush Ready) ---
# ==============================================================================

def perform_unsubscribe_logic(sid, tokens):
    """
    Logic: User ko room se nikalo, par AngelOne se tabhi hatao 
    jab koi bhi dusra user use na dekh raha ho.
    """
    unsub_batch = {1: [], 2: [], 3: [], 5: []}

    for token in tokens:
        leave_room(token)
        if token in state.token_ref_count:
            state.token_ref_count[token] -= 1
            
            # Agar counts 0 ho gaye (No one watching)
            if state.token_ref_count[token] <= 0:
                etype = state.token_metadata.get(token, 1)
                unsub_batch[etype].append(token)
                
                # Cleaning internal tracking (NOT state.subscribed_tokens_set)
                # Hum subscribed_tokens_set ko tick_engine ke cleaner se manage karenge
                if token in state.token_metadata: del state.token_metadata[token]
                if token in state.token_ref_count: del state.token_ref_count[token]

    # AngelOne Unsubscribe (Bandwidth Saver)
    if state.is_ws_ready and state.sws:
        for etype, tokens in unsub_batch.items():
            if tokens:
                state.sws.unsubscribe("myt_unlimited", 1, [{"exchangeType": etype, "tokens": tokens}])

@socketio.on('unsubscribe')
def handle_unsubscribe(data):
    sid = request.sid
    tokens = data.get('tokens', [])
    perform_unsubscribe_logic(sid, tokens)
    for t in tokens: 
        if sid in state.user_subscriptions:
            state.user_subscriptions[sid].discard(t)

@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    try:
        # Jab user jaye, uske rooms khali karo par main list zinda rakho
        if sid in state.user_subscriptions:
            tokens = list(state.user_subscriptions[sid])
            for token in tokens:
                leave_room(token)
                if token in state.token_ref_count:
                    state.token_ref_count[token] -= 1
            # Hum sid ki subscription list delete nahi karenge, 
            # bas clean up karenge room membership
            del state.user_subscriptions[sid]
            
        if sid in state.active_users_pool: del state.active_users_pool[sid]
        if sid in state.user_levels: del state.user_levels[sid]
        logger.info(f"🔌 [Socket] Sid Cleaned: {sid}")
    except Exception as e:
        logger.error(f"⚠️ [Disconnect Error]: {e}")
