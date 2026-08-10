import sqlite3
import tempfile
import requests
import os
import gc
import csv
import gzip
import io
from supabase import create_client

# ==============================================================================
# --- CONFIGURATION (Only Supabase Config Needed) ---
# ==============================================================================
SUPABASE_URL = "https://fnfynhgkdevxytxtfzrk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZuZnluaGdrZGV2eHl0eHRmenJrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc0OTAwMjgsImV4cCI6MjA5MzA2NjAyOH0.Tgr8kB6KGeAsAbXzH8a2wlLStqMFS3fnFPcowbL4Di8"
BUCKET_NAME = "myt"

# ==============================================================================
# --- FULL SYNC & OVERWRITE LOGIC (Upstox Data -> angel_master.db) ---
# ==============================================================================
def sync_master_data():
    print("🔄 [Upstox Master] Initializing Upstox Scrip Master Sync...")
    tmp_dir = tempfile.gettempdir()
    db_path = os.path.join(tmp_dir, "angel_master.db")

    try:
        # Upstox Public Master Contract Complete File URL (No Keys/Login Required)
        upstox_url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
        print("⬇️ [Upstox Master] Downloading complete instrument master file from Upstox...")
        response = requests.get(upstox_url, timeout=120)
        
        if response.status_code == 200:
            # Decompress Gzip data in memory
            decompressed_content = gzip.decompress(response.content).decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(decompressed_content))

            if os.path.exists(db_path):
                os.remove(db_path)

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=OFF")
            cursor.execute("PRAGMA synchronous=OFF")
            
            # Table schema directly aligned with DatabaseHelper.kt
            cursor.execute('''CREATE TABLE symbols (
                token TEXT, symbol TEXT, name TEXT, expiry TEXT, 
                strike TEXT, lotsize TEXT, instrumenttype TEXT, 
                exch_seg TEXT, tick_size TEXT)''')

            records = []
            
            for row in csv_reader:
                # Instrument Key serves as token in Upstox (e.g. NSE_EQ|INE002A01018 or NSE_FO|43812)
                token = row.get('instrument_key') or row.get('token') or ''
                if not token:
                    continue

                trading_symbol = row.get('tradingsymbol') or row.get('symbol') or ''
                name = row.get('name') or trading_symbol
                expiry = row.get('expiry') or ''
                strike = row.get('strike') or '0.0'
                lotsize = row.get('lot_size') or '1'
                instrumenttype = row.get('instrument_type') or ''
                segment = row.get('exchange') or ''
                tick_size = row.get('tick_size') or '0.05'

                # Standardize exch_seg mapping according to DatabaseHelper expectations
                # Segment values: NSE_EQ, NSE_FO, BSE_EQ, BSE_FO, MCX_FO
                exch_seg = segment
                if "|" in token:
                    exch_seg = token.split("|")[0]

                records.append((
                    str(token),
                    str(trading_symbol),
                    str(name),
                    str(expiry),
                    str(strike),
                    str(lotsize),
                    str(instrumenttype),
                    str(exch_seg),
                    str(tick_size)
                ))

            print(f"📦 [Upstox Master] Processing {len(records)} symbols...")
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", records)
            
            # Indexes required by DatabaseHelper.kt queries
            cursor.execute("CREATE INDEX idx_token_fast ON symbols(token)")
            cursor.execute("CREATE INDEX idx_name_fast ON symbols(name)")
            cursor.execute("CREATE INDEX idx_symbol_fast ON symbols(symbol)")
            cursor.execute("CREATE INDEX idx_exch_fast ON symbols(exch_seg)")

            conn.commit()
            conn.close()
            print("✅ [Upstox Master] SQLite angel_master.db generated with Upstox Data.")

            # Supabase Storage Upload / Overwrite Process
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            
            with open(db_path, "rb") as f:
                file_bytes = f.read()
                
            try:
                # 1st Attempt: Directly Overwrite / Upsert using file_options
                supabase.storage.from_(BUCKET_NAME).upload(
                    path="angel_master.db", 
                    file=file_bytes,
                    file_options={"upsert": "true", "content-type": "application/x-sqlite3"}
                )
                print("✅ [Master] Cloud Backup to Supabase Complete (Uploaded/Overwritten).")
            except Exception as upload_err:
                # 2nd Attempt: If upload fails due to existing file, use update method to overwrite
                print(f"⚠️ [Master] Upload notice ({upload_err}), attempting direct update/overwrite...")
                supabase.storage.from_(BUCKET_NAME).update(
                    path="angel_master.db",
                    file=file_bytes,
                    file_options={"content-type": "application/x-sqlite3"}
                )
                print("✅ [Master] Cloud File Overwritten via Update successfully.")

            del records, decompressed_content
            gc.collect()
            return True
        else:
            print(f"❌ [Master] Failed to fetch Upstox master file. HTTP Status: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ [Master] Sync Failed: {e}")
        return False


# ==============================================================================
# --- DIRECT EXECUTION ---
# ==============================================================================
if __name__ == "__main__":
    print("🚀 Running Script directly to download Upstox Data, generate DB, and overwrite angel_master.db on Supabase...")
    sync_master_data()
