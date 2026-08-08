import sqlite3
import tempfile
import requests
import os
import gc
import csv
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================
SUPABASE_URL = "https://fnfynhgkdevxytxtfzrk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZuZnluaGdrZGV2eHl0eHRmenJrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc0OTAwMjgsImV4cCI6MjA5MzA2NjAyOH0.Tgr8kB6KGeAsAbXzH8a2wlLStqMFS3fnFPcowbL4Di8"
BUCKET_NAME = "myt"

FYERS_NSE_CM_URL = "https://public.fyers.in/sym_mapping/nse_cm.csv"
FYERS_NSE_FO_URL = "https://public.fyers.in/sym_mapping/nse_fo.csv"
FYERS_MCX_URL = "https://public.fyers.in/sym_mapping/mcx_fo.csv"

SYNC_STATUS = "NOT_STARTED"
IS_PROCESSING = True

def parse_fyers_csv(url, default_exch):
    records = []
    try:
        print(f"📥 Downloading Fyers CSV: {default_exch}...")
        sys.stdout.flush()
        resp = requests.get(url, timeout=120)
        if resp.status_code == 200:
            lines = resp.text.splitlines()
            reader = csv.reader(lines)
            for row in reader:
                if len(row) >= 9:
                    fy_token = row[0].strip() if len(row) > 0 else ""
                    symbol = row[1].strip() if len(row) > 1 else ""
                    name = symbol.split("-")[0] if "-" in symbol else symbol
                    lotsize = row[2].strip() if len(row) > 2 else "1"
                    tick_size = row[3].strip() if len(row) > 3 else "0.05"
                    instrumenttype = row[7].strip() if len(row) > 7 else ""
                    strike = row[8].strip() if len(row) > 8 else "0"
                    expiry = row[9].strip() if len(row) > 9 else ""
                    exch_seg = default_exch

                    records.append((
                        str(fy_token), str(symbol), str(name), str(expiry),
                        str(strike), str(lotsize), str(instrumenttype),
                        str(exch_seg), str(tick_size)
                    ))
            print(f"✅ Download Complete: {len(records)} records for {default_exch}")
            sys.stdout.flush()
    except Exception as e:
        print(f"⚠️ Error downloading {default_exch}: {e}")
        sys.stdout.flush()
    return records

def generate_and_upload_db():
    global SYNC_STATUS, IS_PROCESSING
    SYNC_STATUS = "RUNNING"
    print("\n🔄 ==========================================")
    print("🔄 STARTING FYERS MASTER SYNC & SUPABASE UPLOAD")
    print("🔄 ==========================================\n")
    sys.stdout.flush()

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

            print(f"⚙️ Inserting {len(records)} records into SQLite DB...")
            sys.stdout.flush()
            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", records)
            cursor.execute("CREATE INDEX idx_token_fast ON symbols(token)")
            cursor.execute("CREATE INDEX idx_name_fast ON symbols(name)")

            conn.commit()
            conn.close()
            print("✅ Local SQLite file generated successfully.")
            sys.stdout.flush()

            print("🚀 Uploading & Overwriting on Supabase...")
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
                print("🎉 SUCCESS: Master DB Uploaded & Overwritten on Supabase!")
                print("==================================================\n")
                SYNC_STATUS = "COMPLETED_SUCCESS"
            else:
                print(f"❌ Supabase Upload Failed: {upload_resp.status_code} - {upload_resp.text}")
                SYNC_STATUS = f"FAILED_{upload_resp.status_code}"

            sys.stdout.flush()
            del records, file_bytes
            gc.collect()

    except Exception as e:
        print(f"❌ Sync Error: {e}")
        sys.stdout.flush()
        SYNC_STATUS = f"ERROR_{str(e)}"
    finally:
        IS_PROCESSING = False

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        response_text = f"Status: {SYNC_STATUS}"
        self.wfile.write(response_text.encode('utf-8'))

    def log_message(self, format, *args):
        return

def self_ping_worker(port):
    """
    जब तक प्रोसेसिंग चालू रहेगी, यह हर 3 सेकंड में सर्वर को ऑटो-हिट करेगा 
    ताकि Render टाइमआउट न करे और थ्रेड Kill न हो।
    """
    time.sleep(2)
    while IS_PROCESSING:
        try:
            requests.get(f"http://127.0.0.1:{port}/", timeout=2)
        except Exception:
            pass
        time.sleep(3)

def run_sync_and_ping(port):
    # Auto Ping धागा (Throttling Timeout Preventer)
    threading.Thread(target=self_ping_worker, args=(port,), daemon=True).start()
    
    # Download & Upload Execute
    generate_and_upload_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🌐 Server initialized on port {port}...")
    sys.stdout.flush()

    # बैकग्राउंड थ्रेड में डाउनलोड, ऑटो-पिंग और अपलोड चालू करें
    threading.Thread(target=run_sync_and_ping, args=(port,), daemon=True).start()

    # मुख्य थ्रेड पर पोर्ट लाइव रखें
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    server.serve_forever()
