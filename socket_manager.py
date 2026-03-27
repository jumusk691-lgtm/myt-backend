from brain import state, logger, socketio, request, join_room, leave_room, eventlet
import p2p_distributor

# ==============================================================================
# --- 1. CONNECTION HANDLER (Lightweight) ---
# ==============================================================================
@socketio.on('connect')
def handle_connect(auth=None): 
    try:
        sid = request.sid
        # P2P Leveling: Isse server ka load nodes mein divide hota hai
        level = p2p_distributor.assign_node_level(sid)
        state.user_levels[sid] = level 
        state.active_users_pool[sid] = True
        
        # Zero-RAM: User ki list initialize karo par data save mat karo
        if sid not in state.user_subscriptions:
            state.user_subscriptions[sid] = set()

        # Room Join based on Level
        if level == "LEVEL_1":
            join_room("level_1_masters") 
        else:
            join_room("peer_nodes") 
            
        logger.info(f"🚀 [Socket] New {level} Connected: {sid}")
        return True
    except Exception as e:
        logger.error(f"❌ [Connect Error]: {e}")
        return False

# ==============================================================================
# --- 2. BYPASS SUBSCRIPTION (Room Focus) ---
# ==============================================================================
@socketio.on('subscribe')
def handle_incoming_subscription(data):
    watchlist = data.get('watchlist', [])
    if not watchlist: return

    sid = request.sid
    batch_registry = {1: [], 2: [], 3: [], 5: []}

    for instrument in watchlist:
        token = str(instrument.get('token'))
        if not token or token == "None": continue

        # 🔥 BYPASS LOGIC: User ko us token ke room mein daal do.
        # Ab jo bhi tick is token ka aayega, wo is room mein 'emit' hoga.
        join_room(token)
        
        if sid in state.user_subscriptions:
            state.user_subscriptions[sid].add(token)

        # Ref Count: Sirf ye dekhne ke liye ki kya humein Angel se unsub karna hai
        state.token_ref_count[token] = state.token_ref_count.get(token, 0) + 1
        
        # Agar ye token naya hai, toh AngelOne se subscribe karo
        if token not in state.subscribed_tokens_set:
            exch = str(instrument.get('exch', 'NSE')).upper()
            symbol = str(instrument.get('symbol', '')).upper()
            
            if "MCX" in exch: etype = 5
            elif any(x in symbol for x in ["FUT", "CE", "PE"]) or "NFO" in exch: etype = 2
            elif "BFO" in exch or ("BSE" in exch and any(x in symbol for x in ["CE", "PE", "FUT"])): etype = 3
            else: etype = 1
            
            state.token_metadata[token] = etype
            batch_registry[etype].append(token)
            state.subscribed_tokens_set.add(token)

    # AngelOne API Call (Optimized Batching)
    if state.is_ws_ready and state.sws:
        for etype, tokens in batch_registry.items():
            if not tokens: continue
            for i in range(0, len(tokens), 500):
                state.sws.subscribe("myt_unlimited", 1, [{"exchangeType": etype, "tokens": tokens[i:i+500]}])
                eventlet.sleep(0.02) # Fast Relay

# ==============================================================================
# --- 3. DISCONNECT & CLEANUP (RAM Reclaimer) ---
# ==============================================================================
@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    try:
        if sid in state.user_subscriptions:
            tokens = list(state.user_subscriptions[sid])
            for token in tokens:
                leave_room(token) # Room se nikalo
                if token in state.token_ref_count:
                    state.token_ref_count[token] -= 1
                    
                    # Agar koi nahi dekh raha, toh Angel se bhi hatao (Bandwidth Saver)
                    if state.token_ref_count[token] <= 0:
                        etype = state.token_metadata.get(token, 1)
                        if state.sws:
                            state.sws.unsubscribe("myt_unlimited", 1, [{"exchangeType": etype, "tokens": [token]}])
                        
                        # Memory Cleanup
                        state.subscribed_tokens_set.discard(token)
                        state.token_metadata.pop(token, None)
                        state.token_ref_count.pop(token, None)
            
            del state.user_subscriptions[sid]
            
        state.active_users_pool.pop(sid, None)
        state.user_levels.pop(sid, None)
        logger.info(f"🔌 [Socket] Cleaned SID: {sid}")
        gc.collect() # Force RAM release
    except Exception as e:
        logger.error(f"⚠️ [Disconnect Error]: {e}")
