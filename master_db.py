# master_db.py
# Handles Symbol Searching, Option Chain, and Master Data Sync
# Optimized: Syncs ONLY on Monday and Friday to save bandwidth/RAM

import sqlite3, tempfile, requests, os
from brain import state, logger, SUPABASE_URL, SUPABASE_KEY, BUCKET_NAME, create_client, IST, app, jsonify, request
from datetime import datetime

# ==============================================================================
# --- MASTER DATA DOWNLOAD & INDEXING ---
# ==============================================================================
def sync_master_data():
    """
    Logic: 
    1. Check if today is Monday (0) or Friday (4).
    2. If yes, download fresh data from AngelOne and update Supabase.
    3. If no, download the existing 'angel_master.db' from Supabase.
    """
    logger.info("🔄 [Master] Initializing Scrip Master Sync...")
    
    # Aaj ka din nikaalo (Monday=0, Tuesday=1 ... Friday=4 ... Sunday=6)
    today_day = datetime.now(IST).weekday()
    
    # Database path setup
    if not hasattr(state, 'db_path') or not state.db_path:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            state.db_path = tmp.name

    # Check: Kya aaj sync ka din hai? (Monday or Friday)
    is_sync_day = today_day in [0, 4]

    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        if is_sync_day:
            logger.info("📅 [Master] Today is Sync Day (Mon/Fri). Downloading fresh data from AngelOne...")
            master_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
            response = requests.get(master_url, timeout=60)
            
            if response.status_code == 200:
                json_payload = response.json()
                
                conn = sqlite3.connect(state.db_path)
                cursor = conn.cursor()
                cursor.execute("PRAGMA journal_mode=WAL") 
                cursor.execute("DROP TABLE IF EXISTS symbols")
                cursor.execute('''CREATE TABLE symbols (
                    token TEXT, symbol TEXT, name TEXT, expiry TEXT, 
                    strike TEXT, lotsize TEXT, instrumenttype TEXT, 
                    exch_seg TEXT, tick_size TEXT)''')
                
                records = [(str(i.get('token')), i.get('symbol'), i.get('name'), i.get('expiry'),
                            i.get('strike'), i.get('lotsize'), i.get('instrumenttype'),
                            i.get('exch_seg'), i.get('tick_size'))
                           for i in json_payload if i.get('token')]
                
                cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", records)
                cursor.execute("CREATE INDEX idx_token_fast ON symbols(token)")
                cursor.execute("CREATE INDEX idx_name_fast ON symbols(name)")
                conn.commit()
                conn.close()
                
                # Persistence: Supabase pe naya file upload karo
                with open(state.db_path, "rb") as f:
                    supabase.storage.from_(BUCKET_NAME).upload(
                        path="angel_master.db", 
                        file=f.read(),
                        file_options={"x-upsert": "true"}
                    )
                logger.info("✅ [Master] Fresh Sync & Cloud Backup Complete.")
        else:
            # Baaki din (Tue, Wed, Thu, Sat, Sun): Seedha Supabase se uthao
            logger.info("☁️ [Master] Not a Sync Day. Pulling existing DB from Supabase...")
            try:
                res = supabase.storage.from_(BUCKET_NAME).download("angel_master.db")
                with open(state.db_path, "wb") as f:
                    f.write(res)
                logger.info("✅ [Master] Existing DB loaded from Cloud Persistence.")
            except Exception as e:
                logger.error(f"⚠️ [Master] Supabase Pull Failed: {e}. Retrying full sync as fallback...")
                # Fallback: Agar cloud pe file na mile, toh sync kar lo
                return sync_master_data_forced()

        state.last_master_update = datetime.now(IST)
        return True

    except Exception as e:
        logger.error(f"❌ [Master] Sync Logic Failed: {e}")
        return False

def sync_master_data_forced():
    """Emergency fallback sync logic"""
    # ... (Aapka purana sync logic yahan repeat ho sakta hai emergency ke liye)
    pass

# ==============================================================================
# --- SEARCH & OPTION CHAIN API ROUTES ---
# ==============================================================================
@app.route('/api/search', methods=['POST'])
def handle_search():
    """Fast search for symbols (Used by APK Search Bar)"""
    try:
        query = request.json.get('query', '').upper()
        if not os.path.exists(state.db_path): return jsonify({"symbols": []})
        
        conn = sqlite3.connect(state.db_path)
        cursor = conn.cursor()
        # instrumenttype='' ensures we get main symbols like NIFTY, BANKNIFTY first
        cursor.execute("SELECT DISTINCT name FROM symbols WHERE name LIKE ? AND instrumenttype = '' LIMIT 15", (f'%{query}%',))
        results = [row[0] for row in cursor.fetchall()]
        conn.close()
        return jsonify({"symbols": results})
    except Exception as e: 
        logger.error(f"Search API Error: {e}")
        return jsonify({"symbols": []})

@app.route('/api/option_chain', methods=['POST'])
def get_chain():
    """Fetches Call/Put strikes for a specific expiry"""
    try:
        d = request.json
        name, expiry = d.get('name'), d.get('expiry')
        if not os.path.exists(state.db_path): return jsonify({"status": False})

        conn = sqlite3.connect(state.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT strike, token, symbol FROM symbols WHERE name = ? AND expiry = ? ORDER BY CAST(strike AS FLOAT) ASC", (name, expiry))
        rows = cursor.fetchall()
        conn.close()
        
        # Result mapping
        chain = [{"strike": r[0], "token": r[1], "symbol": r[2]} for r in rows]
        return jsonify({"status": True, "chain": chain})
    except Exception as e:
        logger.error(f"Option Chain Error: {e}")
        return jsonify({"status": False})
