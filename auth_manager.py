import pyotp
import socket
import gc
from SmartApi import SmartConnect
from brain import state, logger, API_KEY, CLIENT_CODE, MPIN, TOTP_STR

# ==============================================================================
# --- 1. CONNECTIVITY LOGIC (DNS Resilience) ---
# ==============================================================================
def verify_dns_resilience():
    """Checks if AngelOne API is reachable to prevent crash loop"""
    try:
        host = "apiconnect.angelone.in"
        # DNS resolution check
        socket.gethostbyname(host)
        state.dns_status = True
        return True
    except Exception:
        state.dns_status = False
        logger.error("📡 [Auth] DNS Failure: Internet or AngelOne API unreachable.")
        return False

# ==============================================================================
# --- 2. ANGELONE SESSION GENERATOR (Zero-RAM Leak) ---
# ==============================================================================
def start_angel_session():
    """
    Generates the master JWT and Feed tokens.
    Optimization: Clears session cache after login to save RAM.
    """
    if not verify_dns_resilience():
        return None

    try:
        logger.info(f"🔐 [Auth] Initializing Session for Client: {CLIENT_CODE}")
        
        # Cloudflare Optimized: Local instance to save state memory
        smart_api = SmartConnect(api_key=API_KEY)
        
        # Generating TOTP
        totp = pyotp.TOTP(TOTP_STR).now()
        
        # Login Execution
        session = smart_api.generateSession(CLIENT_CODE, MPIN, totp)
        
        if session and session.get('status'):
            logger.info("✅ [Auth] AngelOne Session Created Successfully.")
            
            # Save session instance to state only if successful
            state.smart_api = smart_api
            
            # Data return karne ke baad manual cleanup
            session_data = session['data']
            del session # RAM Release
            gc.collect() 
            
            return session_data 
        else:
            message = session.get('message', 'Unknown Error')
            logger.error(f"❌ [Auth] Session Failed: {message}")
            return None
            
    except Exception as e:
        logger.error(f"⚠️ [Auth] Critical Login Error: {e}")
        return None

# ==============================================================================
# --- 3. REGISTRATION LOGIC ---
# ==============================================================================
def register_user_logic(username, password):
    """Placeholder for your APK's registration logic"""
    # Cloudflare par Supabase client 'brain.py' mein hai, wahi use hoga
    logger.info(f"👤 [Auth] Registration request for: {username}")
    return True
