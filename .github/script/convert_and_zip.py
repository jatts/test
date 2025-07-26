import pandas as pd
import sqlite3
import os
import zipfile
from datetime import datetime

log_path = "conversation/Logs/workflow_log.txt"
xlsx_dir = "conversation/xlsx"
ready_dir = "conversation/Ready"

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a") as f:
        f.write(f"{timestamp} - {message}\n")
    print(f"{timestamp} - {message}")

try:
    # Ensure directories exist
    os.makedirs(ready_dir, exist_ok=True)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    log("Start: Checking required files")
    required_files = ["scanning.xlsx", "prices.xlsx", "version.txt"]
    for file in required_files:
        if not os.path.exists(os.path.join(xlsx_dir, file)):
            raise FileNotFoundError(f"{file} not found.")

    log("All required files found.")

    # Read version
    with open(os.path.join(xlsx_dir, "version.txt"), "r") as f:
        version = f.read().strip()

    log(f"Version extracted: {version}")

    # Load Excel files
    scan_df = pd.read_excel(os.path.join(xlsx_dir, "scanning.xlsx"))
    price_df = pd.read_excel(os.path.join(xlsx_dir, "prices.xlsx"))

    log("Excel files loaded successfully")

    # Merge prices into scanning using Barcode
    if 'Barcode' not in scan_df.columns or 'Barcode' not in price_df.columns:
        raise KeyError("Missing 'Barcode' column in one of the files.")

    price_df = price_df[['Barcode', 'OriginalPrice']]
    scan_df = pd.merge(scan_df, price_df, on='Barcode', how='left')

    # Format price column
    scan_df['OriginalPrice'] = scan_df['OriginalPrice'].apply(lambda x: '' if pd.isna(x) else int(x))

    log("Files merged and cleaned successfully")

    # Write to SQLite
    db_path = os.path.join(ready_dir, f"{version}.db")
    conn = sqlite3.connect(db_path)
    scan_df.to_sql("products", conn, index=False, if_exists='replace', dtype={"OriginalPrice": "TEXT"})
    conn.close()

    log(f"Database created: {db_path}")

    # Zip DB
    zip_path = os.path.join(ready_dir, f"{version}.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(db_path, arcname=f"{version}.db")

    log(f"Database zipped: {zip_path}")

    # Delete .db after zipping
    os.remove(db_path)
    log(f"Deleted raw DB file: {db_path}")

    # Cleanup uploaded files
    for file in required_files:
        os.remove(os.path.join(xlsx_dir, file))
        log(f"Deleted: {file}")

    log("Cleanup completed.")
    log("Workflow Finished Successfully ✅")

except Exception as e:
    log(f"❌ ERROR: {e}")
    raise