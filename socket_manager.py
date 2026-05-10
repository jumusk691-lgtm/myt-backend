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
# --- 2. BATCHING & BYPASS SUBSCRIPTION ---
# ==============================================================================
@socketio.on('subscribe')
def handle_incoming_subscription(data):
    watchlist = data.get('watchlist', [])
    if not watchlist: return

    sid = request.sid
    batch_to_angel = {1: [], 2: [], 3: [], 5: []}
    
    for instrument in watchlist:
        token = str(instrument.get('token'))
        if not token or token == "None": continue

        # --- BYPASS ROOM JOINING ---
        # Room joining asan rakhi hai taaki direct pipe bani rahe
        join_room(token)
        
        if sid in state.user_subscriptions:
            state.user_subscriptions[sid].add(token)

        state.token_ref_count[token] = state.token_ref_count.get(token, 0) + 1
        
        if token not in state.subscribed_tokens_set:
            exch = str(instrument.get('exch', 'NSE')).upper()
            symbol = str(instrument.get('symbol', '')).upper()
            
            # Segment Selection
            if "MCX" in exch: etype = 5
            elif any(x in symbol for x in ["FUT", "CE", "PE"]) or "NFO" in exch: etype = 2
            elif "BFO" in exch: etype = 3
            else: etype = 1
            
            state.token_metadata[token] = etype
            batch_to_angel[etype].append(token)
            state.subscribed_tokens_set.add(token)

    # AngelOne Subscribe (Batching used for stability)
    def angel_trigger():
        if state.is_ws_ready and state.sws:
            for etype, tokens in batch_to_angel.items():
                if not tokens: continue
                for i in range(0, len(tokens), 500):
                    chunk = tokens[i:i+500]
                    state.sws.subscribe("myt_unlimited", 1, [{"exchangeType": etype, "tokens": chunk}])
                    eventlet.sleep(0.05) 
    
    eventlet.spawn(angel_trigger)

# ==============================================================================
# --- 3. BATCHED LTP RESPONSE (Fix for Zero Price) ---
# ==============================================================================
@socketio.on('get_last_known_prices')
def handle_ltp_request():
    sid = request.sid
    if sid not in state.user_subscriptions: return
    
    # BATCHING: Ek hi object mein saare prices bhej rahe hain
    response = {}
    for token in state.user_subscriptions[sid]:
        # state.batch_buffer se latest price uthayega
        price = state.batch_buffer.get(token, '0.00')
        response[token] = price
    
    socketio.emit('live_batch', response, room=sid)

# ==============================================================================
# --- 4. DISCONNECT & CLEANUP (Zero-RAM) ---
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
                        etype = state.token_metadata.get(token, 1)
                        if state.sws:
                            state.sws.unsubscribe("myt_unlimited", 1, [{"exchangeType": etype, "tokens": [token]}])
                        state.subscribed_tokens_set.discard(token)
                        state.token_metadata.pop(token, None)
                        state.token_ref_count.pop(token, None)
                        # RAM se buffer bhi saaf karo
                        state.batch_buffer.pop(token, None)
            del state.user_subscriptions[sid]
        
        state.active_users_pool.pop(sid, None)
        state.user_levels.pop(sid, None)
        gc.collect() 
    except Exception as e:
        logger.error(f"⚠️ [Disconnect Error]: {e}")
