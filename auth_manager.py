# auth_manager.py
# File 2: Handles AngelOne Session, Login Logic & Connectivity Checks

import pyotp
from SmartApi import SmartConnect
from brain import state, logger, API_KEY, CLIENT_CODE, MPIN, TOTP_STR, socket

# ==============================================================================
# --- CONNECTIVITY LOGIC ---
# ==============================================================================
def verify_dns_resilience():
    """Checks if AngelOne API is reachable to prevent crash loop"""
    try:
        host = "apiconnect.angelone.in"
        socket.gethostbyname(host)
        state.dns_status = True
        return True
    except:
        state.dns_status = False
        logger.error("📡 [Auth] DNS Failure: Internet or AngelOne API unreachable.")
        return False

# ==============================================================================
# --- ANGELONE SESSION GENERATOR ---
# ==============================================================================
def start_angel_session():
    """Generates the master JWT and Feed tokens for the engine"""
    if not verify_dns_resilience():
        return None

    try:
        logger.info(f"🔐 [Auth] Initializing Session for Client: {CLIENT_CODE}")
        state.smart_api = SmartConnect(api_key=API_KEY)
        
        # Generating TOTP
        totp = pyotp.TOTP(TOTP_STR).now()
        
        # Login Execution
        session = state.smart_api.generateSession(CLIENT_CODE, MPIN, totp)
        
        if session and session.get('status'):
            logger.info("✅ [Auth] AngelOne Session Created Successfully.")
            return session['data'] # Isme jwtToken aur feedToken hota hai
        else:
            message = session.get('message', 'Unknown Error')
            logger.error(f"❌ [Auth] Session Failed: {message}")
            return None
            
    except Exception as e:
        logger.error(f"⚠️ [Auth] Critical Login Error: {e}")
        return None

# ==============================================================================
# --- USER REGISTRATION LOGIC (FUTURE SCOPE) ---
# ==============================================================================
def register_user_logic(username, password):
    """Placeholder for your APK's registration logic"""
    # Yahan hum Supabase ya SQLite mein user save karne ka logic daalenge
    logger.info(f"👤 [Auth] Registration request for: {username}")
    return True
