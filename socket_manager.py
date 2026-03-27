from brain import state, logger, socketio, request, join_room, leave_room, eventlet
import p2p_distributor
import gc

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
        
        if sid not in state.user_subscriptions:
            state.user_subscriptions[sid] = set()

        if level == "LEVEL_1": join_room("level_1_masters") 
        else: join_room("peer_nodes") 
            
        logger.info(f"🚀 [Socket] New {level} Connected: {sid}")
        return True
    except Exception as e:
        logger.error(f"❌ [Connect Error]: {e}")
        return False

# ==============================================================================
# --- 2. BYPASS SUBSCRIPTION (1000 Symbols Optimized) ---
# ==============================================================================
@socketio.on('subscribe')
def handle_incoming_subscription(data):
    watchlist = data.get('watchlist', [])
    if not watchlist: return

    sid = request.sid
    batch_to_angel = {1: [], 2: [], 3: [], 5: []}
    
    # Fast filtering and room joining
    for instrument in watchlist:
        token = str(instrument.get('token'))
        if not token or token == "None": continue

        # 1. Join Room (Direct Pipe)
        join_room(token)
        
        # 2. Update state memory
        if sid in state.user_subscriptions:
            state.user_subscriptions[sid].add(token)

        # 3. Ref Count (Bandwidth optimization)
        state.token_ref_count[token] = state.token_ref_count.get(token, 0) + 1
        
        # 4. Check if we need to call AngelOne
        if token not in state.subscribed_tokens_set:
            exch = str(instrument.get('exch', 'NSE')).upper()
            symbol = str(instrument.get('symbol', '')).upper()
            
            # Logic for Segment Selection
            if "MCX" in exch: etype = 5
            elif any(x in symbol for x in ["FUT", "CE", "PE"]) or "NFO" in exch: etype = 2
            elif "BFO" in exch: etype = 3
            else: etype = 1
            
            state.token_metadata[token] = etype
            batch_to_angel[etype].append(token)
            state.subscribed_tokens_set.add(token)

    # 🚀 STEP 5: AngelOne Subscribe (Non-Blocking)
    def angel_trigger():
        if state.is_ws_ready and state.sws:
            for etype, tokens in batch_to_angel.items():
                if not tokens: continue
                # Split in 500 batches
                for i in range(0, len(tokens), 500):
                    chunk = tokens[i:i+500]
                    state.sws.subscribe("myt_unlimited", 1, [{"exchangeType": etype, "tokens": chunk}])
                    eventlet.sleep(0.05) # Delay for stability
    
    eventlet.spawn(angel_trigger)

# ==============================================================================
# --- 3. FETCH CURRENT LTP (The "Zero" Price Fix) ---
# ==============================================================================
@socketio.on('get_last_known_prices')
def handle_ltp_request():
    """
    Jab user app khole, wo ye call karega. Hum state se (jo tick_engine 
    save kar raha hai) turant prices bhej denge.
    """
    sid = request.sid
    if sid not in state.user_subscriptions: return
    
    # Iske liye tick_engine ko 'state.last_known_prices' update karni hogi
    # Hum ek batch reply bhejenge
    response = {}
    for token in state.user_subscriptions[sid]:
        price = state.token_metadata.get(token, {}).get('last_price', '0.00')
        response[token] = price
    
    socketio.emit('initial_prices', response, room=sid)

# ==============================================================================
# --- 4. DISCONNECT & CLEANUP ---
# ==============================================================================
@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    try:
        if sid in state.user_subscriptions:
            tokens = list(state.user_subscriptions[sid])
            for token in tokens:
                leave_room(token)
                if token in state.token_ref_count:
                    state.token_ref_count[token] -= 1
                    if state.token_ref_count[token] <= 0:
                        # Unsubscribe only if NO ONE is watching
                        etype = state.token_metadata.get(token, 1)
                        if state.sws:
                            state.sws.unsubscribe("myt_unlimited", 1, [{"exchangeType": etype, "tokens": [token]}])
                        state.subscribed_tokens_set.discard(token)
                        state.token_metadata.pop(token, None)
                        state.token_ref_count.pop(token, None)
            del state.user_subscriptions[sid]
        
        state.active_users_pool.pop(sid, None)
        state.user_levels.pop(sid, None)
        gc.collect() 
    except Exception as e:
        logger.error(f"⚠️ [Disconnect Error]: {e}")
