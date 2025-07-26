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
log("All required files uploaded.")  
  
# Step 2: Read files  
try:  
    scan_df = pd.read_excel(os.path.join(csv_folder, 'scanning.xlsx'))  
    price_df = pd.read_excel(os.path.join(csv_folder, 'prices.xlsx'))  
    version = open(os.path.join(csv_folder, 'version.txt')).read().strip()  
    log(f"Read all input files successfully. New Version: {version}")  
except Exception as e:  
    log(f"Error reading input files: {e}")  
    exit(1)  
  
# Step 3: Merge OriginalPrice from prices.xlsx into scanning.xlsx  
try:  
    if 'OriginalPrice' in price_df.columns:  
        merged_df = pd.merge(scan_df, price_df[['Barcode', 'OriginalPrice']], on='Barcode', how='left')  
    else:  
        raise Exception("OriginalPrice column missing in prices.xlsx")  
  
    merged_df = merged_df[['Barcode', 'Article', 'Percentage', 'OriginalPrice']]  
  
    merged_df['OriginalPrice'] = merged_df['OriginalPrice'].apply(  
        lambda x: str(int(x)) if pd.notnull(x) and isinstance(x, (int, float)) else ""  
    )  
  
    log("Merged and cleaned successfully.")  
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
    log(f"SQLite DB created: {db_path}")  
except Exception as e:  
    log(f"Error saving to SQLite: {e}")  
    exit(1)  
  
# Step 5: Zip the DB  
zip_path = os.path.join(ready_folder, f"{version}.zip")  
try:  
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:  
        zipf.write(db_path, arcname=os.path.basename(db_path))  
    log(f"Zipped DB created: {zip_path}")  
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
  
# Step 7: Delete input files after processing  
try:  
    input_files = ["scanning.xlsx", "prices.xlsx", "version.txt"]  
    for fname in input_files:  
        fpath = os.path.join(csv_folder, fname)  
        if os.path.exists(fpath):  
            os.remove(fpath)  
            log(f"Deleted: {fname}")  
        else:  
            log(f"File not found: {fname}")  
    log("All input files removed successfully.")
except Exception as e:  
    log(f"Error deleting input files: {e}")  
    exit(1)