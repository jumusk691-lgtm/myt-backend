# p2p_distributor.py
# Logic: Tree-based data distribution (100 -> 1000 -> 10000 -> 100000)
# Final Fix: Attribute safety and Binary Relay optimization

from brain import state, socketio, logger
import json

# ==============================================================================
# --- 0. SAFETY LAYER ---
# ==============================================================================
def get_total_users():
    """Safety check for state attributes"""
    if hasattr(state, 'active_users_pool'):
        return len(state.active_users_pool)
    elif hasattr(state, 'active_users_list'):
        return len(state.active_users_list)
    return 0

# ==============================================================================
# --- 1. NODE ASSIGNMENT (Dynamic Scaling) ---
# ==============================================================================
def assign_node_level(user_id):
    """User ko uski capacity ke hisaab se Levels mein divide karna"""
    total_connected = get_total_users()
    
    if total_connected <= 100:
        return "LEVEL_1"  # Root Nodes (Direct from Render)
    elif total_connected <= 1000:
        return "LEVEL_2"  # Distribution Layer 1
    elif total_connected <= 10000:
        return "LEVEL_3"  # Distribution Layer 2
    elif total_connected <= 50000:
        return "LEVEL_4"  # Regional Distribution
    else:
        return "LEVEL_5"  # Edge Nodes (Final Leaves)

# ==============================================================================
# --- 2. CHAIN BROADCAST LOGIC (Binary & JSON Support) ---
# ==============================================================================
@socketio.on('broadcast_to_sub_node')
def handle_sub_node_broadcast(data):
    """
    Logic: Jab Level 1 user ko data milta hai, uska APK isse wapas bhejta hai.
    Server use 'live_update_batch' event ke saath next level ko relay karta hai.
    """
    try:
        # Check if data is binary (ByteArray) or Dictionary
        if isinstance(data, (bytes, bytearray)):
            # Agar data already binary hai (coming from APK), toh direct relay
            # Hum ise decode nahi karenge taaki CPU load na bade
            target_group = "level_2_nodes" # Example dynamic targeting logic
            socketio.emit('live_update_batch', data, to=target_group)
            return

        # Agar JSON data aaya hai
        target_group = data.get('target_group') 
        payload = data.get('market_data')
        
        if target_group and payload:
            # Server direct push karega next group ko
            # Hum wahi 'live_update_batch' event use kar rahe hain jo UserActivity sun raha hai
            socketio.emit('live_update_batch', payload, to=target_group)
            
    except Exception as e:
        logger.error(f"❌ [P2P Relay Error]: {e}")

# ==============================================================================
# --- 3. MASTER EMIT (The Root Trigger) ---
# ==============================================================================
def master_node_emit(snap):
    """
    Ye function tick_engine.py call karega.
    Logic: Pura load 1 lakh logo pe nahi, sirf 'level_1_masters' (first 100) pe dalega.
    """
    try:
        # Master emit hamesha batch event par jayega
        # Isse aapka UserActivity ka 'live_update_batch' trigger hoga
        socketio.emit('live_update_batch', snap, to='level_1_masters')
    except Exception as e:
        logger.error(f"❌ [Master Emit Error]: {e}")
