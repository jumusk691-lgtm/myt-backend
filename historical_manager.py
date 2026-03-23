# historical_manager.py
# File 7: Handles Historical Candle Data for Charts (90 Days)

from brain import state, logger, app, jsonify, request, datetime, IST

# ==============================================================================
# --- 1. HISTORICAL DATA FETCHING LOGIC ---
# ==============================================================================
@app.route('/get_90day_chart', methods=['POST'])
def fetch_chart_data():
    """Fetches historical candles for APK Charts"""
    try:
        d = request.json
        token = d.get('token')
        exch = d.get('exch', 'NSE')
        interval = d.get('interval', "ONE_MINUTE") # 1min, 5min, 15min etc.
        
        if not state.smart_api:
            logger.error("❌ [History] SmartApi Session not found!")
            return jsonify({"status": False, "message": "Session Expired"})

        # Time Calculation: Aaj se 90 din pehle tak ka data
        to_date = datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M')
        from_date = (datetime.datetime.now(IST) - datetime.timedelta(days=90)).strftime('%Y-%m-%d %H:%M')

        # AngelOne Historical API Call
        payload = {
            "exchange": exch,
            "symboltoken": token,
            "interval": interval,
            "fromdate": from_date,
            "todate": to_date
        }
        
        historic_data = state.smart_api.getCandleData(payload)
        
        if historic_data and historic_data.get('status'):
            logger.info(f"📊 [History] Data sent for Token: {token}")
            return jsonify(historic_data)
        else:
            return jsonify({"status": False, "message": "No data available"})

    except Exception as e:
        logger.error(f"⚠️ [History] Error: {e}")
        return jsonify({"status": False, "error": str(e)})

# ==============================================================================
# --- 2. EXPIRY LIST API (For Options Traders) ---
# ==============================================================================
@app.route('/api/expiry_list', methods=['POST'])
def get_expiry():
    """Helps APK show the list of upcoming Expiries for Options"""
    import sqlite3
    try:
        name = request.json.get('name')
        conn = sqlite3.connect(state.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT expiry FROM symbols WHERE name = ? AND expiry != '' ORDER BY expiry ASC", (name,))
        exps = [r[0] for r in cursor.fetchall()]
        conn.close()
        return jsonify({"expiries": exps})
    except: return jsonify([])
