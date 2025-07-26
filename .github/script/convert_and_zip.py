import os
import pandas as pd
import sqlite3
import zipfile
from datetime import datetime

# Paths
csv_folder = 'conversation/csv'
log_path = 'conversation/Logs/workflow_activity.log'
ready_folder = 'conversation/Ready'

# Step 0: Fresh Log File
if os.path.exists(log_path):
    os.remove(log_path)

# Logging Function
def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, 'a') as f:
        f.write(f"{timestamp} - {msg}\n")
    print(f"{timestamp} - {msg}")

# Step 1: Check all required files
required_files = ['scanning.xlsx', 'prices.xlsx', 'version.txt']
for file in required_files:
    full_path = os.path.join(csv_folder, file)
    if not os.path.exists(full_path):
        log(f"Missing required file: {file}")
        exit(1)
log("All required files found.")

# Step 2: Read files
try:
    scan_df = pd.read_excel(os.path.join(csv_folder, 'scanning.xlsx'))
    price_df = pd.read_excel(os.path.join(csv_folder, 'prices.xlsx'))
    version = open(os.path.join(csv_folder, 'version.txt')).read().strip()
    log(f"Read all input files successfully. Version: {version}")
except Exception as e:
    log(f"Error reading input files: {e}")
    exit(1)

# Step 3: Merge OriginalPrice from prices.xlsx into scanning.xlsx
try:
    if 'OriginalPrice' in price_df.columns:
        merged_df = pd.merge(scan_df, price_df[['Barcode', 'OriginalPrice']], on='Barcode', how='left')
    else:
        raise Exception("OriginalPrice column missing in prices.xlsx")

    # Keep only desired columns
    merged_df = merged_df[['Barcode', 'Article', 'Percentage', 'OriginalPrice']]

    # Clean OriginalPrice values
    merged_df['OriginalPrice'] = merged_df['OriginalPrice'].apply(
        lambda x: str(int(x)) if pd.notnull(x) and isinstance(x, (int, float)) else ""
    )

    log("Merged OriginalPrice from prices.xlsx into scanning.xlsx successfully.")
except Exception as e:
    log(f"Error during merging or cleaning: {e}")
    exit(1)

# Step 4: Save to SQLite
db_name = f"{version}.db"
db_path = os.path.join(ready_folder, db_name)
try:
    os.makedirs(ready_folder, exist_ok=True)
    conn = sqlite3.connect(db_path)
    merged_df.to_sql('data', conn, index=False, if_exists='replace',
                     dtype={
                         'Barcode': 'TEXT',
                         'Article': 'TEXT',
                         'Percentage': 'TEXT',
                         'OriginalPrice': 'TEXT'
                     })
    conn.close()
    log(f"SQLite DB created at {db_path}")
except Exception as e:
    log(f"Error saving to SQLite: {e}")
    exit(1)

# Step 5: Zip the DB
zip_path = os.path.join(ready_folder, f"{version}.zip")
try:
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(db_path, arcname=os.path.basename(db_path))
    log(f"Zipped DB at {zip_path}")
except Exception as e:
    log(f"Error zipping DB: {e}")
    exit(1)

# Step 6: Cleanup unzipped DB
try:
    os.remove(db_path)
    log("Removed raw DB file after zipping.")
except Exception as e:
    log(f"Error during DB cleanup: {e}")
    exit(1)

# Step 7: Delete all files in CSV folder
try:
    for file in os.listdir(csv_folder):
        file_path = os.path.join(csv_folder, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
    log("All temp files deleted.")

except Exception as e:
    log(f"Error deleting files in CSV folder: {e}")
    exit(1)
log("Done")
except Exception as e:
    log(f"Not done complete, some thing error on end {e}")
    exit(1)