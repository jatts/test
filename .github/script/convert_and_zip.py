import os
import pandas as pd
import sqlite3
import zipfile
from datetime import datetime

# Paths
csv_folder = 'conversation/csv'
log_path = 'conversation/Logs/workflow_activity.log'
ready_folder = 'conversation/Ready'

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

# Step 2: Read input files
try:
    scan_df = pd.read_excel(os.path.join(csv_folder, 'scanning.xlsx'))
    price_df = pd.read_excel(os.path.join(csv_folder, 'prices.xlsx'))
    version = open(os.path.join(csv_folder, 'version.txt')).read().strip()
    log(f"Read all input files successfully. Version: {version}")
except Exception as e:
    log(f"Error reading input files: {e}")
    exit(1)

# Step 3: Merge DataFrames
try:
    merged_df = pd.merge(scan_df, price_df, on='Barcode', how='left')
    merged_df = merged_df[['Barcode', 'Article', 'Percentage', 'OriginalPrice']]
    merged_df['OriginalPrice'] = merged_df['OriginalPrice'].apply(
        lambda x: str(int(x)) if pd.notnull(x) and isinstance(x, (int, float)) else ""
    )
    log("DataFrames merged and cleaned.")
except Exception as e:
    log(f"Error merging data: {e}")
    exit(1)

# Step 4: Save to SQLite
db_name = f"{version}.db"
db_path = os.path.join(ready_folder, db_name)
try:
    os.makedirs(ready_folder, exist_ok=True)
    conn = sqlite3.connect(db_path)
    merged_df.to_sql('data', conn, index=False, if_exists='replace',
                     dtype={'Barcode': 'TEXT', 'Article': 'TEXT', 'Percentage': 'TEXT', 'OriginalPrice': 'TEXT'})
    conn.close()
    log("SQLite DB created.")
except Exception as e:
    log(f"Error saving to SQLite: {e}")
    exit(1)

# Step 5: Zip the DB
zip_path = os.path.join(ready_folder, f"{version}.zip")
try:
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        zipf.write(db_path, arcname=os.path.basename(db_path))
    log("DB zipped successfully.")
except Exception as e:
    log(f"Error zipping DB: {e}")
    exit(1)

# Step 6: Delete original DB
try:
    os.remove(db_path)
    log("Cleaned up raw DB file after zipping.")
except Exception as e:
    log(f"Error during DB cleanup: {e}")
    exit(1)

# Step 7: Delete original input files
deleted = []
for f in required_files:
    full = os.path.join(csv_folder, f)
    try:
        os.remove(full)
        deleted.append(f)
    except Exception as e:
        log(f"Error deleting file {f}: {e}")

if len(deleted) == len(required_files):
    log("All input files deleted successfully.")
else:
    log("Some input files were not deleted.")

# Step 8: Log Done
try:
    with open(os.path.join(csv_folder, 'log_done.txt'), 'w') as done_log:
        done_log.write('Conversion and cleanup completed.\n')
    log("log_done.txt created.")
except Exception as e:
    log(f"Error creating log_done.txt: {e}")