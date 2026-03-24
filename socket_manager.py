from brain import state, logger, socketio, request, join_room, leave_room, eventlet
import p2p_distributor

# ==============================================================================
# --- 1. CONNECTION HANDLER ---
# ==============================================================================
@socketio.on('connect')
def handle_connect(auth=None): 
    try:
        sid = request.sid
        level = p2p_distributor.assign_node_level(sid)
        state.user_levels[sid] = level 
        state.active_users_pool[sid] = True
        
        # User ki personal tracking list
        state.user_subscriptions[sid] = set()

        if level == "LEVEL_1":
            join_room("level_1_masters") 
            logger.info(f"💎 [Master] {sid} joined LEVEL_1")
        else:
            join_room("peer_nodes") 
            logger.info(f"👥 [Peer] {sid} joined {level}")

        return True
    except Exception as e:
        logger.error(f"❌ [Socket Connect Error]: {e}")
        return False

# ==============================================================================
# --- 2. SMART MULTI-SEGMENT SUBSCRIPTION (Zero-Waste Logic) ---
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

        # 1. User ko Room join karwao
        join_room(token)
        state.user_subscriptions[sid].add(token)

        # 2. Reference Counting: Track karo kitne users is token ko dekh rahe hain
        state.token_ref_count[token] = state.token_ref_count.get(token, 0) + 1
        
        # 3. Agar ye token server par PEHLI BAAR aaya hai, tabhi Angel ko bolo
        if token not in state.subscribed_tokens_set:
            if "MCX" in exch: etype = 5
            elif any(x in symbol for x in ["FUT", "CE", "PE"]) or "NFO" in exch: etype = 2
            elif "BFO" in exch or ("BSE" in exch and any(x in symbol for x in ["CE", "PE", "FUT"])): etype = 3
            else: etype = 1
            
            state.token_metadata[token] = etype
            batch_registry[etype].append(token)
            state.subscribed_tokens_set.add(token)

    # AngelOne API Subscription
    if state.is_ws_ready and state.sws:
        for etype, tokens in batch_registry.items():
            if not tokens: continue
            for i in range(0, len(tokens), 500):
                final_batch = tokens[i:i+500]
                state.sws.subscribe("myt_unlimited", 1, [{"exchangeType": etype, "tokens": final_batch}])
                eventlet.sleep(0.05) 
                logger.info(f"📡 [AngelOne] New Sub: {len(final_batch)} tokens (Etype: {etype})")

# ==============================================================================
# --- 3. AUTO-CLEANUP & DISCONNECT (Memory Protection) ---
# ==============================================================================

def perform_unsubscribe_logic(sid, tokens):
    """Token ka pehra kam karo, agar 0 ho jaye toh AngelOne se hata do"""
    unsub_batch = {1: [], 2: [], 3: [], 5: []}

    for token in tokens:
        leave_room(token)
        if token in state.token_ref_count:
            state.token_ref_count[token] -= 1
            
            # Agar koi bhi user ab ye token nahi dekh raha
            if state.token_ref_count[token] <= 0:
                etype = state.token_metadata.get(token, 1)
                unsub_batch[etype].append(token)
                
                # Cache se saaf karo
                if token in state.subscribed_tokens_set: state.subscribed_tokens_set.remove(token)
                if token in state.token_metadata: del state.token_metadata[token]
                if token in state.token_ref_count: del state.token_ref_count[token]

    # AngelOne ko bolo ab data mat bhejo (Saving bandwidth)
    if state.is_ws_ready and state.sws:
        for etype, tokens in unsub_batch.items():
            if tokens:
                state.sws.unsubscribe("myt_unlimited", 1, [{"exchangeType": etype, "tokens": tokens}])
                logger.info(f"📉 [AngelOne] Unsubscribed {len(tokens)} ghost tokens.")

@socketio.on('unsubscribe')
def handle_unsubscribe(data):
    sid = request.sid
    tokens = data.get('tokens', [])
    perform_unsubscribe_logic(sid, tokens)
    # User ki personal list se bhi hatao
    for t in tokens: state.user_subscriptions[sid].discard(t)

@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    try:
        # User ne jo jo subscribe kiya tha, sabka count minus karo
        if sid in state.user_subscriptions:
            perform_unsubscribe_logic(sid, list(state.user_subscriptions[sid]))
            del state.user_subscriptions[sid]
            
        if sid in state.active_users_pool: del state.active_users_pool[sid]
        if sid in state.user_levels: del state.user_levels[sid]
        logger.info(f"🔌 [Socket] User Cleanup Done: {sid}")
    except Exception as e:
        logger.error(f"⚠️ [Cleanup Error]: {e}")
