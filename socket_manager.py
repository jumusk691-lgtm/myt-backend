# socket_manager.py
from brain import state, logger, socketio, request, join_room, eventlet

@socketio.on('connect')
def handle_connect():
    logger.info(f"🔌 [Socket] User Connected: {request.sid}")

@socketio.on('subscribe')
def handle_incoming_subscription(data):
    """Logic 13: Full Sync Batch Logic"""
    watchlist = data.get('watchlist', [])
    if not watchlist: return

    batch_registry = {1: [], 2: [], 3: [], 5: []}

    for instrument in watchlist:
        token = str(instrument.get('token'))
        exch = str(instrument.get('exch', 'NSE')).upper()
        if not token or token == "": continue

        join_room(token) # Targeted updates ke liye
        
        # Segment Logic
        etype = 1 
        if "MCX" in exch: etype = 5
        elif any(x in exch for x in ["NFO", "FUT", "OPT"]): etype = 2
        elif "BSE" in exch: etype = 3
        
        state.token_metadata[token] = etype
        if token not in state.subscribed_tokens_set:
            batch_registry[etype].append(token)
            state.subscribed_tokens_set.add(token)

    # AngelOne Subscribe Call
    if state.is_ws_ready and state.sws:
        for etype, tokens in batch_registry.items():
            if not tokens: continue
            for i in range(0, len(tokens), 500):
                final_batch = tokens[i:i+500]
                state.sws.subscribe(f"sub_{etype}_{i}", 1, [{"exchangeType": etype, "tokens": final_batch}])
                eventlet.sleep(0.05)
