import sqlite3, tempfile, requests, os
from brain import state, logger, SUPABASE_URL, SUPABASE_KEY, BUCKET_NAME, create_client, IST, app, jsonify, request
from datetime import datetime

# ==============================================================================
# --- MASTER DATA DOWNLOAD & INDEXING (ZERO-RAM OPTIMIZED) ---
# ==============================================================================
def sync_master_data():
    """
    Logic: Same (Mon/Fri Sync). 
    Update: Added 'Manual GC' and 'Connection Closing' to save RAM.
    """
    logger.info("🔄 [Master] Initializing Scrip Master Sync...")
    
    today_day = datetime.now(IST).weekday()
    
    # Path setup: Cloudflare environment ke liye temp path
    if not hasattr(state, 'db_path') or not state.db_path:
        tmp_dir = tempfile.gettempdir()
        state.db_path = os.path.join(tmp_dir, "angel_master.db")

    # Sync Day Check (Mon=0, Fri=4)
    is_sync_day = today_day in [0, 4]

    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        if is_sync_day:
            logger.info("📅 [Master] Today is Sync Day (Mon/Fri). Downloading fresh data...")
            master_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
            response = requests.get(master_url, timeout=60)
            
            if response.status_code == 200:
                json_payload = response.json()
                
                # RAM bachaane ke liye Database connection ko local rakha hai
                conn = sqlite3.connect(state.db_path)
                cursor = conn.cursor()
                cursor.execute("PRAGMA journal_mode=OFF") # Cloudflare ke liye OFF mode fast hai
                cursor.execute("PRAGMA synchronous=OFF")
                
                cursor.execute("DROP TABLE IF EXISTS symbols")
                cursor.execute('''CREATE TABLE symbols (
                    token TEXT, symbol TEXT, name TEXT, expiry TEXT, 
                    strike TEXT, lotsize TEXT, instrumenttype TEXT, 
                    exch_seg TEXT, tick_size TEXT)''')
                
                records = [(str(i.get('token')), i.get('symbol'), i.get('name'), i.get('expiry'),
                            i.get('strike'), i.get('lotsize'), i.get('instrumenttype'),
                            i.get('exch_seg'), i.get('tick_size'))
                           for i in json_payload if i.get('token')]
                
                # Batch insert (Speed optimized)
                cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", records)
                cursor.execute("CREATE INDEX idx_token_fast ON symbols(token)")
                cursor.execute("CREATE INDEX idx_name_fast ON symbols(name)")
                
                conn.commit()
                conn.close() # RAM turant free karo
                
                # Cloud Backup
                with open(state.db_path, "rb") as f:
                    supabase.storage.from_(BUCKET_NAME).upload(
                        path="angel_master.db", 
                        file=f.read(),
                        file_options={"x-upsert": "true"}
                    )
                logger.info("✅ [Master] Fresh Sync & Cloud Backup Complete.")
                del json_payload, records # Memory se hatao
        else:
            logger.info("☁️ [Master] Pulling existing DB from Supabase...")
            try:
                res = supabase.storage.from_(BUCKET_NAME).download("angel_master.db")
                with open(state.db_path, "wb") as f:
                    f.write(res)
                logger.info("✅ [Master] Existing DB loaded from Cloud.")
            except Exception as e:
                logger.error(f"⚠️ [Master] Fallback trigger: {e}")
                return sync_master_data_forced()

        state.last_master_update = datetime.now(IST)
        import gc
        gc.collect() # 12MB Limit check
        return True

    except Exception as e:
        logger.error(f"❌ [Master] Logic Failed: {e}")
        return False

def sync_master_data_forced():
    """Emergency sync logic implementation if needed"""
    return False

# ==============================================================================
# --- SEARCH & OPTION CHAIN API (BYPASS OPTIMIZED) ---
# ==============================================================================
@app.route('/api/search', methods=['POST'])
def handle_search():
    """Fast search for symbols (Zero-RAM Leak)"""
    try:
        query = request.json.get('query', '').upper()
        if not os.path.exists(state.db_path): return jsonify({"symbols": []})
        
        conn = sqlite3.connect(state.db_path)
        cursor = conn.cursor()
        # instrumenttype='' main indices ke liye fast bypass
        cursor.execute("SELECT DISTINCT name FROM symbols WHERE name LIKE ? AND instrumenttype = '' LIMIT 15", (f'%{query}%',))
        results = [row[0] for row in cursor.fetchall()]
        conn.close() # Connection hamesha close karein
        return jsonify({"symbols": results})
    except Exception as e: 
        return jsonify({"symbols": []})

@app.route('/api/option_chain', methods=['POST'])
def get_chain():
    """Fetches strikes with Batch-ready tokens"""
    try:
        d = request.json
        name, expiry = d.get('name'), d.get('expiry')
        if not os.path.exists(state.db_path): return jsonify({"status": False})

        conn = sqlite3.connect(state.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT strike, token, symbol FROM symbols WHERE name = ? AND expiry = ? ORDER BY CAST(strike AS FLOAT) ASC", (name, expiry))
        rows = cursor.fetchall()
        conn.close()
        
        chain = [{"strike": r[0], "token": r[1], "symbol": r[2]} for r in rows]
        return jsonify({"status": True, "chain": chain})
    except Exception as e:
        logger.error(f"Option Chain Error: {e}")
        return jsonify({"status": False})
