# master_db.py
# File 3: Handles Symbol Searching, Option Chain, and Master Data Sync

import sqlite3, tempfile, requests
from brain import state, logger, SUPABASE_URL, SUPABASE_KEY, BUCKET_NAME, create_client, IST, datetime, app, jsonify, request

# ==============================================================================
# --- MASTER DATA DOWNLOAD & INDEXING ---
# ==============================================================================
def sync_master_data():
    """Downloads 100k+ scrips and creates a high-speed SQLite index"""
    logger.info("🔄 [Master] Starting Scrip Master Sync...")
    try:
        master_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = requests.get(master_url, timeout=45)
        
        if response.status_code == 200:
            json_payload = response.json()
            
            # Temporary file for the database
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
                state.db_path = tmp.name
            
            conn = sqlite3.connect(state.db_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL") # High-speed mode
            cursor.execute("DROP TABLE IF EXISTS symbols")
            cursor.execute('''CREATE TABLE symbols (
                token TEXT, symbol TEXT, name TEXT, expiry TEXT, 
                strike TEXT, lotsize TEXT, instrumenttype TEXT, 
                exch_seg TEXT, tick_size TEXT)''')
            
            # Batch Insert for Speed
            records = [(str(i.get('token')), i.get('symbol'), i.get('name'), i.get('expiry'),
                        i.get('strike'), i.get('lotsize'), i.get('instrumenttype'),
                        i.get('exch_seg'), i.get('tick_size'))
                       for i in json_payload if i.get('token')]
            
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", records)
            
            # Creating Indexes (Search will be 100x faster)
            cursor.execute("CREATE INDEX idx_token_fast ON symbols(token)")
            cursor.execute("CREATE INDEX idx_name_fast ON symbols(name)")
            conn.commit()
            conn.close()
            
            # Uploading to Supabase for Persistence
            with open(state.db_path, "rb") as f:
                supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", 
                    file=f.read(),
                    file_options={"x-upsert": "true"}
                )
            
            state.last_master_update = datetime.datetime.now(IST)
            logger.info("✅ [Master] SQLite Sync & Cloud Backup Complete.")
            return True
    except Exception as e:
        logger.error(f"❌ [Master] Sync Failed: {e}")
        return False

# ==============================================================================
# --- SEARCH & OPTION CHAIN API ROUTES ---
# ==============================================================================
@app.route('/api/search', methods=['POST'])
def handle_search():
    """Fast search for symbols (Used by APK Search Bar)"""
    try:
        query = request.json.get('query', '').upper()
        conn = sqlite3.connect(state.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT name FROM symbols WHERE name LIKE ? AND instrumenttype = '' LIMIT 15", (f'%{query}%',))
        results = [row[0] for row in cursor.fetchall()]
        conn.close()
        return jsonify({"symbols": results})
    except: return jsonify([])

@app.route('/api/option_chain', methods=['POST'])
def get_chain():
    """Fetches Call/Put strikes for a specific expiry"""
    try:
        d = request.json
        name, expiry = d.get('name'), d.get('expiry')
        conn = sqlite3.connect(state.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT strike, token, symbol FROM symbols WHERE name = ? AND expiry = ? ORDER BY CAST(strike AS FLOAT) ASC", (name, expiry))
        rows = cursor.fetchall()
        conn.close()
        return jsonify({"chain": rows})
    except: return jsonify({"status": False})
