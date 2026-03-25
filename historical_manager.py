# historical_manager.py
# File 7: Handles Historical Candle Data (5min & 15min - 10 Day Window)

from brain import state, logger, app, IST
from flask import jsonify, request
import datetime
import sqlite3

# ==============================================================================
# --- 1. HISTORICAL DATA FETCHING LOGIC ---
# ==============================================================================
@app.route('/api/get_chart_data', methods=['POST'])
def fetch_chart_data():
    """
    Fetches historical candles for APK Charts.
    Strictly restricted to 5-minute and 15-minute intervals for 10 days.
    Includes logic for session-based score tracking.
    """
    try:
        d = request.json
        token = str(d.get('token'))
        exch = d.get('exch', 'NSE')
        
        # User 5 or 15 bhej sakta hai, default hum 5 rakhenge
        requested_interval = d.get('interval', "FIVE_MINUTE")
        
        # Validation: Sirf 5 aur 15 allow karenge
        if requested_interval == "FIFTEEN_MINUTE":
            interval = "FIFTEEN_MINUTE"
        else:
            interval = "FIVE_MINUTE"
        
        # Session check
        if not hasattr(state, 'smart_api') or not state.smart_api:
            logger.error("❌ [History] SmartApi Session is NULL!")
            return jsonify({"status": False, "message": "API Session Expired"})

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
        
        logger.info(f"📊 [History] Fetching {interval} (10 Days) for Token: {token}")
        historic_data = state.smart_api.getCandleData(params)
        
        if historic_data and historic_data.get('status'):
            # --- SCORE LOGIC ---
            # Har bar chart data load hone par score update hoga
            if not hasattr(state, 'score'):
                state.score = 0
            state.score += 1
            logger.info(f"📈 [Score] Chart accessed. Current Score: {state.score}")

            # AngelOne returns: [ ["time", O, H, L, C, V], ... ]
            return jsonify({
                "status": True,
                "token": token,
                "interval": interval,
                "score": state.score,
                "data": historic_data.get('data', [])
            })
        else:
            msg = historic_data.get('message', 'No data from API')
            logger.warning(f"⚠️ [History] API returned no data: {msg}")
            return jsonify({"status": False, "message": msg})

    except Exception as e:
        logger.error(f"❌ [History Error]: {e}")
        return jsonify({"status": False, "error": str(e)})

# ==============================================================================
# --- 2. EXPIRY LIST API ---
# ==============================================================================
@app.route('/api/expiry_list', methods=['POST'])
def get_expiry():
    """Returns list of expiry dates for Options trading from SQLite master"""
    try:
        name = request.json.get('name', '').upper()
        if not name: return jsonify({"expiries": [], "status": False})

        if not hasattr(state, 'db_path') or not state.db_path:
             return jsonify({"expiries": [], "status": False, "msg": "DB not ready"})

        conn = sqlite3.connect(state.db_path)
        cursor = conn.cursor()
        
        # Query: Unique expiries in ascending order
        cursor.execute("""
            SELECT DISTINCT expiry 
            FROM symbols 
            WHERE name = ? AND expiry != '' 
            ORDER BY expiry ASC
        """, (name,))
        
        exps = [r[0] for r in cursor.fetchall()]
        conn.close()
        
        return jsonify({
            "status": True,
            "name": name,
            "expiries": exps
        })
    except Exception as e:
        logger.error(f"❌ [Expiry API Error]: {e}")
        return jsonify({"expiries": [], "status": False})
