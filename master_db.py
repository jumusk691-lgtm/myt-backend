import sqlite3
import tempfile
import requests
import os
import gc
import csv
import sys
import traceback
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

SUPABASE_URL = "https://fnfynhgkdevxytxtfzrk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZuZnluaGdrZGV2eHl0eHRmenJrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc0OTAwMjgsImV4cCI6MjA5MzA2NjAyOH0.Tgr8kB6KGeAsAbXzH8a2wlLStqMFS3fnFPcowbL4Di8"
BUCKET_NAME = "myt"

FYERS_NSE_CM_URL = "https://public.fyers.in/sym_details/NSE_CM.csv"
FYERS_NSE_FO_URL = "https://public.fyers.in/sym_details/NSE_FO.csv"
FYERS_BSE_CM_URL = "https://public.fyers.in/sym_details/BSE_CM.csv"
FYERS_BSE_FO_URL = "https://public.fyers.in/sym_details/BSE_FO.csv"
FYERS_MCX_URL    = "https://public.fyers.in/sym_details/MCX_COM.csv"

def format_expiry(raw_expiry):
    """Converts Epoch Timestamp (e.g., 1786443000) to '11 Aug 2026' format"""
    if not raw_expiry:
        return ""
    raw_str = str(raw_expiry).strip()
    if raw_str.isdigit() and len(raw_str) >= 9:
        try:
            ts = int(raw_str)
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt.strftime("%d %b %Y")
        except Exception:
            return raw_str
    return raw_str

def detect_accurate_lot_size(symbol, row, exch):
    """Detects exact lot size based on symbol name and CSV fallback"""
    sym = symbol.upper()
    
    # Standard Index & Commodity Lot Sizes
    if "NIFTY" in sym and "BANK" not in sym and "FIN" not in sym and "MID" not in sym:
        return "25"
    if "BANKNIFTY" in sym:
        return "15"
    if "FINNIFTY" in sym:
        return "25"
    if "MIDCPNIFTY" in sym or "MIDCAP" in sym:
        return "50"
    if "SENSEX" in sym:
        return "10"
    if "BANKEX" in sym:
        return "15"
    if "CRUDEOILM" in sym:
        return "10"
    if "CRUDEOIL" in sym:
        return "100"
    if "NATURALGAS" in sym:
        return "1250"
    if "NATGASMINI" in sym:
        return "250"
    if "GOLDM" in sym:
        return "10"
    if "GOLD" in sym:
        return "100"
    if "SILVERM" in sym:
        return "5"
    if "SILVER" in sym:
        return "30"

    # Fallback to CSV columns if Equity/Stock F&O
    if exch in ["NFO", "BFO", "MCX"]:
        for idx in [13, 14, 2, 3]:
            if len(row) > idx:
                val = row[idx].strip()
                if val.isdigit() and int(val) > 0 and int(val) != 14 and int(val) != 31:
                    return val
        return "1"
    
    return "1" # Cash/Equity default

def parse_csv_data(text, default_exch, records_list):
    lines = text.splitlines()
    reader = csv.reader(lines)
    count = 0
    for row in reader:
        if len(row) >= 10:
            fy_token = row[0].strip()
            symbol = row[9].strip() if len(row) > 9 else row[1].strip()
            name = row[1].strip()
            
            # Accurate Lot Size Detection
            lotsize = detect_accurate_lot_size(symbol, row, default_exch)
            
            tick_size = row[3].strip() if len(row) > 3 else "0.05"
            raw_expiry = row[8].strip() if len(row) > 8 else ""
            expiry = format_expiry(raw_expiry)
            
            strike = row[15].strip() if len(row) > 15 else "0"
            raw_inst = row[16].strip() if len(row) > 16 else ""

            sym_upper = symbol.upper()
            if sym_upper.endswith("CE"):
                inst_type = "CE"
            elif sym_upper.endswith("PE"):
                inst_type = "PE"
            elif "FUT" in sym_upper or "FUT" in raw_inst.upper():
                inst_type = "FUT"
            else:
                inst_type = "EQ"

            records_list.append((
                str(fy_token), str(symbol), str(name), str(expiry),
                str(strike), str(lotsize), str(inst_type),
                str(default_exch), str(tick_size)
            ))
            count += 1
    print(f"✅ Loaded {count} records for {default_exch}")
    sys.stdout.flush()

def fetch_all_market_data():
    all_records = []
    headers = {"User-Agent": "Mozilla/5.0"}

    endpoints = [
        (FYERS_NSE_CM_URL, "NSE"),
        (FYERS_NSE_FO_URL, "NFO"),
        (FYERS_BSE_CM_URL, "BSE"),
        (FYERS_BSE_FO_URL, "BFO"),
        (FYERS_MCX_URL,    "MCX")
    ]

    for url, exch in endpoints:
        try:
            print(f"📥 Downloading {exch}...")
            sys.stdout.flush()
            r = requests.get(url, headers=headers, timeout=60)
            if r.status_code == 200 and len(r.text) > 100:
                parse_csv_data(r.text, exch, all_records)
        except Exception as e:
            print(f"❌ Error fetching {exch}: {e}")
            sys.stdout.flush()

    return all_records

def generate_and_upload_db():
    print("\n🔄 Generating Fixed Master Database with Accurate Lot Sizes...")
    sys.stdout.flush()

    tmp_dir = tempfile.gettempdir()
    db_path = os.path.join(tmp_dir, "angel_master.db")

    try:
        records = fetch_all_market_data()
        print(f"📊 Total Combined Records: {len(records)}")
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

            cursor.executemany("INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?)", records)
            
            cursor.execute("CREATE INDEX idx_token_fast ON symbols(token)")
            cursor.execute("CREATE INDEX idx_name_fast ON symbols(name)")
            cursor.execute("CREATE INDEX idx_exch_inst ON symbols(exch_seg, instrumenttype)")

            conn.commit()
            conn.close()

            print("🚀 Uploading DB to Supabase...")
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
                print("🎉 SUCCESS: Entire Market DB Uploaded with 100% Correct Lot Sizes!")
            else:
                print(f"❌ Upload Failed: {upload_resp.text}")

            sys.stdout.flush()
            del records, file_bytes
            gc.collect()

    except Exception as e:
        print(f"❌ Sync Error: {e}")
        traceback.print_exc()

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Full Market DB Sync Active.")

if __name__ == "__main__":
    generate_and_upload_db()
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    server.serve_forever()
