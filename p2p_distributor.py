# p2p_distributor.py
# Logic: Tree-based data distribution (100 -> 1000 -> 10000 -> 100000)

from brain import state, socketio, logger

# ==============================================================================
# --- 1. NODE ASSIGNMENT (Tree Logic Fixed) ---
# ==============================================================================
def assign_node_level(user_id):
    """User ko uski capacity ke hisaab se Levels mein divide karna"""
    total_connected = len(state.active_users_pool)
    
    if total_connected <= 100:
        return "LEVEL_1"  # Render -> Level 1
    elif total_connected <= 1000:
        return "LEVEL_2"  # Level 1 -> Level 2
    elif total_connected <= 10000:
        return "LEVEL_3"  # Level 2 -> Level 3
    elif total_connected <= 50000:
        return "LEVEL_4"  # Level 3 -> Level 4
    else:
        return "LEVEL_5"  # Level 4 -> Level 5 (The Final Leaves)

# ==============================================================================
# --- 2. CHAIN BROADCAST LOGIC (The Relay) ---
# ==============================================================================
@socketio.on('broadcast_to_sub_node')
def handle_sub_node_broadcast(data):
    """
    Jab user ko data milta hai, uska APK isse wapas bhejta hai 
    taaki server use next level ke group ko 'Push' kar sake.
    """
    target_group = data.get('target_group') 
    payload = data.get('market_data')
    
    if target_group and payload:
        # Server bina load liye bas aage pass kar raha hai
        socketio.emit('live_update_relay', payload, to=target_group)

# ==============================================================================
# --- 3. MASTER EMIT (The Root) ---
# ==============================================================================
def master_node_emit(snap):
    """Ye function tick_engine.py direct call karega"""
    # Sirf 100 'Master Nodes' ko data jayega
    socketio.emit('master_tick', snap, to='level_1_masters')
