from brain import state, logger, app, IST
from fastapi import Request, Response
import datetime
import sqlite3
import gc

# ==============================================================================
# --- 1. HISTORICAL DATA FETCHING (ZERO-RAM OPTIMIZED) ---
# ==============================================================================
@app.post('/api/get_chart_data')
async def fetch_chart_data(request: Request):
    """
    Fetches historical candles for APK Charts.
    Cloudflare Optimization: Manual GC and Local Variable cleanup.
    """
    try:
        d = await request.json()
        token = str(d.get('token'))
        exch = d.get('exch', 'NSE')
        
        # Interval Validation
        requested_interval = d.get('interval', "FIVE_MINUTE")
        interval = "FIFTEEN_MINUTE" if requested_interval == "FIFTEEN_MINUTE" else "FIVE_MINUTE"
        
        # Session check (State management from brain.py)
        if not state.smart_api:
            logger.error("❌ [History] SmartApi Session is NULL!")
            return {"status": False, "message": "API Session Expired"}

        # Time Calculation: Strictly Last 10 Days
        now = datetime.datetime.now(IST)
        to_date = now.strftime('%Y-%m-%d %H:%M')
        from_date = (now - datetime.timedelta(days=10)).strftime('%Y-%m-%d %H:%M')

        # AngelOne API Payload
        params = {
            "exchange": exch,
            "symboltoken": token,
            "interval": interval,
            "fromdate": from_date,
            "todate": to_date
        }
        
        logger.info(f"📊 [History] Fetching {interval} for Token: {token}")
        
        # API Call
        historic_data = state.smart_api.getCandleData(params)
        
        if historic_data and historic_data.get('status'):
            # --- SCORE LOGIC (Zero-RAM Bypass) ---
            state.score += 1
            
            result = {
                "status": True,
                "token": token,
                "interval": interval,
                "score": state.score,
                "data": historic_data.get('data', [])
            }
            
            # --- MEMORY CLEANUP ---
            del historic_data
            gc.collect() 
            
            return result
        else:
            msg = historic_data.get('message', 'No data from API')
            return {"status": False, "message": msg}

    except Exception as e:
        logger.error(f"❌ [History Error]: {e}")
        return {"status": False, "error": str(e)}

# ==============================================================================
# --- 2. EXPIRY LIST API (SQLite Optimized) ---
# ==============================================================================
@app.post('/api/expiry_list')
async def get_expiry(request: Request):
    """Returns list of expiry dates from SQLite master"""
    try:
        d = await request.json()
        name = d.get('name', '').upper()
        if not name: return {"expiries": [], "status": False}

        if not state.db_path:
             return {"expiries": [], "status": False, "msg": "DB not ready"}

        # Use context manager for auto-closing connection
        with sqlite3.connect(state.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT expiry 
                FROM symbols 
                WHERE name = ? AND expiry != '' 
                ORDER BY expiry ASC
            """, (name,))
            exps = [r[0] for r in cursor.fetchall()]
        
        # Connection automatically closes here
        return {
            "status": True,
            "name": name,
            "expiries": exps
        }
    except Exception as e:
        logger.error(f"❌ [Expiry API Error]: {e}")
        return {"expiries": [], "status": False}
