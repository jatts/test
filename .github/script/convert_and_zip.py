import pandas as pd
import sqlite3
import zipfile
import os
from datetime import datetime

# Define file paths
scan_path = 'conversation/xlsx/scanning.xlsx'
price_path = 'conversation/xlsx/prices.xlsx'
version_path = 'conversation/xlsx/version.txt'
csv_path = 'conversation/Merging/merging_temp.csv'
db_path = ''
zip_path = ''
log_path = 'conversation/Logs/workflow_log.txt'

def log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_path, 'a') as f:
        f.write(f"{timestamp} - {message}\n")
    print(f"{timestamp} - {message}")

try:
    log("✅ Starting process...")

    # Check files
    if not all(os.path.exists(p) for p in [scan_path, price_path, version_path]):
        raise FileNotFoundError("Missing one or more input files.")

    log("📂 All input files found.")

    # Load files
    scanning = pd.read_excel(scan_path)
    prices = pd.read_excel(price_path)
    with open(version_path, 'r') as f:
        version = f.read().strip()

    log(f"📄 Files read successfully. Version: {version}")

    # Merge data
    merged = pd.merge(scanning, prices[['Barcode', 'OriginalPrice']], on='Barcode', how='left')
    merged['OriginalPrice'] = merged['OriginalPrice'].fillna('').apply(lambda x: str(int(x)) if isinstance(x, float) else str(x))

    log("🔁 Merged scanning and prices into one DataFrame.")

    # Save to CSV temp
    merged.to_csv(csv_path, index=False)
    log("💾 Saved merged CSV.")

    # Convert to SQLite
    db_path = f"conversation/Ready/{version}.db"
    conn = sqlite3.connect(db_path)
    merged.to_sql("products", conn, if_exists="replace", index=False, dtype={"OriginalPrice": "TEXT"})
    conn.close()
    log("🗃️ Converted to SQLite database.")

    # Zip DB
    zip_path = f"conversation/Ready/{version}.zip"
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(db_path, arcname=os.path.basename(db_path))
    log("📦 Zipped SQLite DB.")

    # Delete unwanted files
    for file in [scan_path, price_path, version_path, csv_path, db_path]:
        try:
            os.remove(file)
            log(f"🗑️ Deleted file: {file}")
        except Exception as e:
            log(f"⚠️ Error deleting {file}: {e}")

    log("✅ Process completed successfully.\n")

except Exception as e:
    log(f"❌ Error: {e}")
    raise