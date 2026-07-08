import sqlite3
import tempfile
import requests
import os
import gc
import uvicorn
from datetime import datetime
from fastapi import FastAPI
from supabase import create_client
import pyotp  # TOTP के लिए

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================
API_KEY = "Z80WG5Sg"
CLIENT_CODE = "S52638556"
MPIN = "0000"
TOTP_STR = "XFTXZ2445N4V2UMB7EWUCBDRMU"

SUPABASE_URL = "https://fnfynhgkdevxytxtfzrk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZuZnluaGdrZGV2eHl0eHRmenJrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc0OTAwMjgsImV4cCI6MjA5MzA2NjAyOH0.Tgr8kB6KGeAsAbXzH8a2wlLStqMFS3fnFPcowbL4Di8"
BUCKET_NAME = "myt"

# ==============================================================================
# --- FULL SYNC LOGIC (.db generation) ---
# ==============================================================================
def sync_master_data():
    print("🔄 [Master] Initializing Angel One Scrip Master Sync...")
    
    tmp_dir = tempfile.gettempdir()
    db_path = os.path.join(tmp_dir, "angel_master.db")

    try:
        master_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = requests.get(master_url, timeout=60)
        
        if response.status_code == 200:
            json_payload = response.json()
            
            if os.path.exists(db_path): os.remove(db_path)
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("PRAGMA journal_mode=OFF")
            cursor.execute("PRAGMA synchronous=OFF")
            
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
            print("✅ [Master] SQLite .db file generated locally.")

            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            with open(db_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", 
                    file=f.read(),
                    file_options={"x-upsert": "true", "content-type": "application/x-sqlite3"}
                )
            print("✅ [Master] Cloud Backup to Supabase Complete.")
            
            del json_payload, records
            gc.collect()
            return True
            
    except Exception as e:
        print(f"❌ [Master] Sync Failed: {e}")
        return False
    return False

# ==============================================================================
# --- RENDER/SERVER TRIGGER ENDPOINT ---
# ==============================================================================
app = FastAPI()

@app.get("/trigger-sync")
def trigger_sync():
    success = sync_master_data()
    return {"status": "success" if success else "failed"}

def get_totp():
    return pyotp.TOTP(TOTP_STR).now()

# ==============================================================================
# --- SERVER RUNNER (Render ke liye zaroori) ---
# ==============================================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
