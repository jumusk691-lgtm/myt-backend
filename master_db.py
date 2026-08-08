import sqlite3
import tempfile
import requests
import os
import gc
import csv
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from supabase import create_client

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================
SUPABASE_URL = "https://fnfynhgkdevxytxtfzrk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZuZnluaGdrZGV2eHl0eHRmenJrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc0OTAwMjgsImV4cCI6MjA5MzA2NjAyOH0.Tgr8kB6KGeAsAbXzH8a2wlLStqMFS3fnFPcowbL4Di8"
BUCKET_NAME = "myt"

# FYERS Symbol Master URLs
FYERS_NSE_CM_URL = "https://public.fyers.in/sym_mapping/nse_cm.csv"
FYERS_NSE_FO_URL = "https://public.fyers.in/sym_mapping/nse_fo.csv"
FYERS_MCX_URL = "https://public.fyers.in/sym_mapping/mcx_fo.csv"

# Render Web Service Port Binding Keep-Alive Server
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b"Master DB Sync Service Running.")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

def start_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    server.serve_forever()

def parse_fyers_csv(url, default_exch):
    records = []
    try:
        print(f"📥 Downloading Fyers Scrip Master from {default_exch}...")
        resp = requests.get(url, timeout=60)
        if resp.status_code == 200:
            lines = resp.text.splitlines()
            reader = csv.reader(lines)
            for row in reader:
                if len(row) >= 9:
                    # Safe Index Extraction for Fyers CSV Format
                    fy_token = row[0] if len(row) > 0 else ""
                    symbol = row[1] if len(row) > 1 else ""
                    name = symbol.split("-")[0] if "-" in symbol else symbol
                    lotsize = row[2] if len(row) > 2 else "1"
                    tick_size = row[3] if len(row) > 3 else "0.05"
                    instrumenttype = row[7] if len(row) > 7 else ""
                    strike = row[8] if len(row) > 8 else "0"
                    expiry = row[9] if len(row) > 9 else ""
                    exch_seg = default_exch

                    records.append((
                        str(fy_token), str(symbol), str(name), str(expiry),
                        str(strike), str(lotsize), str(instrumenttype),
                        str(exch_seg), str(tick_size)
                    ))
    except Exception as e:
        print(f"⚠️ Error parsing {default_exch} CSV: {e}")
    return records

def generate_and_upload_db():
    print("🔄 [Master] Initializing Fyers Scrip Master Sync...")
    
    tmp_dir = tempfile.gettempdir()
    db_path = os.path.join(tmp_dir, "angel_master.db")

    try:
        records = []
        records.extend(parse_fyers_csv(FYERS_NSE_CM_URL, "NSE"))
        records.extend(parse_fyers_csv(FYERS_NSE_FO_URL, "NFO"))
        records.extend(parse_fyers_csv(FYERS_MCX_URL, "MCX"))
        
        if len(records) > 0:
            if os.path.exists(db_path): 
                os.remove(db_path)
                
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("PRAGMA journal_mode=OFF")
            cursor.execute("PRAGMA synchronous=OFF")
            
            cursor.execute('''CREATE TABLE symbols (
                token TEXT, symbol TEXT, name TEXT, expiry TEXT, 
                strike TEXT, lotsize TEXT, instrumenttype TEXT, 
                exch_seg TEXT, tick_size TEXT)''')
            
            print(f"⚙️ Inserting {len(records)} records into SQLite database...")
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", records)
            cursor.execute("CREATE INDEX idx_token_fast ON symbols(token)")
            cursor.execute("CREATE INDEX idx_name_fast ON symbols(name)")
            
            conn.commit()
            conn.close()
            print("✅ SQLite database generated locally.")

            print("🚀 Uploading & Overwriting on Supabase Storage...")
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            
            with open(db_path, "rb") as f:
                file_bytes = f.read()
                
            supabase.storage.from_(BUCKET_NAME).upload(
                path="angel_master.db", 
                file=file_bytes,
                file_options={"x-upsert": "true", "content-type": "application/x-sqlite3"}
            )
            
            print("==================================================")
            print("🎉 SUCCESS: File Uploaded to Supabase Successfully!")
            print("==================================================")
            
            del records
            gc.collect()
            return True
            
    except Exception as e:
        print(f"❌ [Master] Process Failed: {e}")
        return False
    return False

if __name__ == "__main__":
    # 1. Start Web Server in Background Thread (Required for Render Web Service)
    web_thread = threading.Thread(target=start_health_server, daemon=True)
    web_thread.start()

    # 2. Execute Download and Upload on Deployment
    generate_and_upload_db()

    # 3. Keep main thread alive so Render Service stays Green/Live
    web_thread.join()
