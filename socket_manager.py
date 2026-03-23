# socket_manager.py (Updated with P2P Leveling)
from brain import state, logger, socketio, request, join_room, leave_room, eventlet

@socketio.on('connect')
def handle_connect():
    # User ko uski capacity ke hisaab se Level dena
    user_count = len(state.active_users_pool)
    if user_count < 100:
        state.user_levels[request.sid] = "LEVEL_1" # Render's Direct Child
        join_room("master_nodes")
    else:
        state.user_levels[request.sid] = "LEVEL_2" # Peer Node
        
    state.active_users_pool[request.sid] = True
    logger.info(f"🔌 [Socket] {state.user_levels[request.sid]} Connected: {request.sid}")

@socketio.on('subscribe')
def handle_incoming_subscription(data):
    watchlist = data.get('watchlist', [])
    if not watchlist: return

    batch_registry = {1: [], 2: [], 3: [], 5: []}

    for instrument in watchlist:
        token = str(instrument.get('token'))
        exch = str(instrument.get('exch', 'NSE')).upper()
        if not token: continue

        # --- P2P SHARED LOGIC ---
        # User ko uske specific token room mein daalo
        join_room(token)
        
        # Agar ye Level 1 user hai, tabhi AngelOne ko request bhejo
        # Level 2 users ko data Level 1 waale pass karenge (via server relay)
        if state.user_levels.get(request.sid) == "LEVEL_1":
            etype = 1 
            if "MCX" in exch: etype = 5
            elif any(x in exch for x in ["NFO", "FUT", "OPT"]): etype = 2
            
            state.token_metadata[token] = etype
            if token not in state.subscribed_tokens_set:
                batch_registry[etype].append(token)
                state.subscribed_tokens_set.add(token)

    # AngelOne API Subscription
    if state.is_ws_ready and state.sws:
        for etype, tokens in batch_registry.items():
            if not tokens: continue
            for i in range(0, len(tokens), 500):
                final_batch = tokens[i:i+500]
                state.sws.subscribe(f"sub_{etype}_{i}", 1, [{"exchangeType": etype, "tokens": final_batch}])
                eventlet.sleep(0.05)

# --- NEW: UNSUBSCRIBE LOGIC (To save RAM) ---
@socketio.on('unsubscribe')
def handle_unsubscribe(data):
    tokens = data.get('tokens', [])
    for token in tokens:
        leave_room(token)
        # Yahan check kar sakte hain agar koi aur user us token ko nahi dekh raha toh AngelOne se stop kar dein
    logger.info(f"📉 Unsubscribed: {len(tokens)} tokens")
