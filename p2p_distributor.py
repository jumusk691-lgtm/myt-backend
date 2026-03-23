# p2p_distributor.py
# Logic: Tree-based data distribution (100 -> 1000 -> 10000)

from brain import state, socketio, logger

# ==============================================================================
# --- 1. NODE ASSIGNMENT (Kaun data bhejega aur kaun lega) ---
# ==============================================================================
def assign_node_level(user_id):
    """User ko uski capacity ke hisaab se Level 1, 2 ya 3 mein daalna"""
    total_connected = len(state.active_users_pool)
    
    if total_connected <= 100:
        return "LEVEL_1"  # Ye Render se direct data lenge
    elif total_connected <= 1100:
        return "LEVEL_2"  # Ye Level 1 waalo se data lenge
    else:
        return "LEVEL_3"  # Ye Level 2 waalo se data lenge
    else:
        return "LEVEL_4"
    else:
        return "LEVEL_5"
# ==============================================================================
# --- 2. CHAIN BROADCAST LOGIC ---
# ==============================================================================
@socketio.on('broadcast_to_sub_node')
def handle_sub_node_broadcast(data):
    """
    Jab Level 1 user ko data milega, uska APK isse wapas server 
    par bhejega dusre users ke liye (Relay Race logic)
    """
    target_group = data.get('target_group') # e.g., 'group_A_level2'
    payload = data.get('market_data')
    
    # Server bas is data ko bina process kiye aage "Push" kar dega
    socketio.emit('live_update_relay', payload, to=target_group)

# ==============================================================================
# --- 3. MASTER EMIT (Only to Level 1) ---
# ==============================================================================
def master_node_emit(snap):
    """Ye function tick_engine.py mein use hoga"""
    # Sirf un 100 logon ko bhej raha hai jo 'Master_Nodes' room mein hain
    socketio.emit('master_tick', snap, to='level_1_masters')
