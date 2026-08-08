import sqlite3
import tempfile
import requests
import os
import gc
import csv
import sys
import traceback
from http.server import HTTPServer, BaseHTTPRequestHandler

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================
SUPABASE_URL = "https://fnfynhgkdevxytxtfzrk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZuZnluaGdrZGV2eHl0eHRmenJrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc0OTAwMjgsImV4cCI6MjA5MzA2NjAyOH0.Tgr8kB6KGeAsAbXzH8a2wlLStqMFS3fnFPcowbL4Di8"
BUCKET_NAME = "myt"

# Correct Official FYERS Data URLs
FYERS_NSE_CM_URL = "https://public.fyers.in/sym_details/NSE_CM.csv"
FYERS_NSE_FO_URL = "https://public.fyers.in/sym_details/NSE_FO.csv"
FYERS_MCX_URL = "https://public.fyers.in/sym_details/MCX_FO.csv"

def fetch_pure_fyers_records():
    records = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    }
    
    urls = [
        (FYERS_NSE_CM_URL, "NSE"),
        (FYERS_NSE_FO_URL, "NFO"),
        (FYERS_MCX_URL, "MCX")
    ]

    for url, exch in urls:
        try:
            print(f"📥 Downloading Pure FYERS Master CSV: {exch}...")
            sys.stdout.flush()
            
            resp = requests.get(url, headers=headers, timeout=60)
            if resp.status_code == 200 and len(resp.text) > 100:
                lines = resp.text.splitlines()
                reader = csv.reader(lines)
                count = 0
                for row in reader:
                    # Fyers CSV Column Structure:
                    # row[0]: FyToken (e.g. 10100000003456)
                    # row[1]: Description
                    # row[2]: Lot Size
                    # row[3]: Tick Size
                    # row[8]: Expiry Unix Timestamp (if available)
                    # row[9]: Trading Symbol (e.g. NSE:TATAMOTORS-EQ)
                    # row[15]: Strike Price
                    # row[16]: Option Type
                    if len(row) >= 10:
                        fy_token = row[0].strip()
                        symbol = row[9].strip() if len(row) > 9 else row[1].strip()
                        name = row[1].strip()
                        lotsize = row[2].strip() if len(row) > 2 else "1"
                        tick_size = row[3].strip() if len(row) > 3 else "0.05"
                        
                        expiry = row[8].strip() if len(row) > 8 else ""
                        strike = row[15].strip() if len(row) > 15 else "0"
                        instrumenttype = row[16].strip() if len(row) > 16 else ""

                        records.append((
                            str(fy_token), str(symbol), str(name), str(expiry),
                            str(strike), str(lotsize), str(instrumenttype),
                            str(exch), str(tick_size)
                        ))
                        count += 1
                print(f"✅ Successfully Fetched {count} Pure FYERS Tokens for {exch}")
                sys.stdout.flush()
            else:
                print(f"❌ FYERS Download Failed for {exch} (Status: {resp.status_code})")
                sys.stdout.flush()
        except Exception as e:
            print(f"❌ Exception downloading FYERS {exch}: {e}")
            sys.stdout.flush()
            
    return records

def generate_and_upload_db():
    print("\n🔄 ==========================================")
    print("🔄 STARTING PURE FYERS SYNC & SUPABASE UPLOAD")
    print("🔄 ==========================================\n")
    sys.stdout.flush()

    tmp_dir = tempfile.gettempdir()
    db_path = os.path.join(tmp_dir, "angel_master.db")

    try:
        # Strictly fetch ONLY Fyers records
        records = fetch_pure_fyers_records()

        print(f"📊 Total Pure FYERS Records Collected: {len(records)}")
        sys.stdout.flush()

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

            print(f"⚙️ Inserting {len(records)} Pure FYERS records into SQLite DB...")
            sys.stdout.flush()
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", records)
            cursor.execute("CREATE INDEX idx_token_fast ON symbols(token)")
            cursor.execute("CREATE INDEX idx_name_fast ON symbols(name)")

            conn.commit()
            conn.close()
            print("✅ Pure FYERS SQLite Database Created Successfully.")
            sys.stdout.flush()

            print("🚀 Uploading to Supabase via Direct REST API...")
            sys.stdout.flush()

            upload_url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_NAME}/angel_master.db"
            headers = {
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "apiKey": SUPABASE_KEY,
                "x-upsert": "true",
                "Content-Type": "application/x-sqlite3"
            }

            with open(db_path, "rb") as f:
                file_bytes = f.read()

            upload_resp = requests.post(upload_url, headers=headers, data=file_bytes, timeout=300)

            if upload_resp.status_code in [200, 201]:
                print("\n==================================================")
                print("🎉 SUCCESS: Pure FYERS DB Uploaded & Overwritten on Supabase!")
                print("==================================================\n")
            else:
                print(f"❌ Supabase Upload Failed Response ({upload_resp.status_code}): {upload_resp.text}")

            sys.stdout.flush()
            del records, file_bytes
            gc.collect()
        else:
            print("❌ ABORTED: No FYERS tokens fetched. Supabase DB was NOT overwritten.")
            sys.stdout.flush()

    except Exception as e:
        print(f"❌ Sync Error: {e}")
        traceback.print_exc()
        sys.stdout.flush()

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b"Pure FYERS Master DB Sync Active.")

    def log_message(self, format, *args):
        return

if __name__ == "__main__":
    generate_and_upload_db()

    port = int(os.environ.get("PORT", 10000))
    print(f"🌐 Server initialized on port {port}. Keeping process active...")
    sys.stdout.flush()

    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    server.serve_forever()
