import os
import pandas as pd
import sqlite3
import zipfile
from datetime import datetime

# Get the absolute path of the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Paths - now based on the script's absolute location
csv_folder = os.path.join(script_dir, 'conversation', 'csv')
log_path = os.path.join(script_dir, 'conversation', 'Logs', 'workflow_activity.log')
ready_folder = os.path.join(script_dir, 'conversation', 'Ready')

# Ensure base directories exist if they don't
os.makedirs(csv_folder, exist_ok=True)
os.makedirs(os.path.dirname(log_path), exist_ok=True)
os.makedirs(ready_folder, exist_ok=True)

# Logging Function
def log(msg):
    """
    Logs messages to a file and prints them to the console.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, 'a') as f:
        f.write(f"{timestamp} - {msg}\n")
    print(f"{timestamp} - {msg}")

log("--- Starting Workflow ---")

# Step 1: Check required files
required_files = ['scanning.xlsx', 'prices.xlsx', 'version.txt']
log(f"Checking for required files in: {csv_folder}")
for file in required_files:
    full_path = os.path.join(csv_folder, file)
    if not os.path.exists(full_path):
        log(f"ERROR: Missing required file: {file} at {full_path}. Exiting.")
        exit(1)
log("All required files found.")

# Step 2: Read input files
scan_df = None
price_df = None
version = None
try:
    log("Attempting to read input files...")
    scan_df = pd.read_excel(os.path.join(csv_folder, 'scanning.xlsx'))
    price_df = pd.read_excel(os.path.join(csv_folder, 'prices.xlsx'))
    with open(os.path.join(csv_folder, 'version.txt'), 'r') as f:
        version = f.read().strip()
    log(f"Successfully read all input files. Version: {version}")
except FileNotFoundError as e:
    log(f"ERROR: Input file not found: {e}. Ensure files are in {csv_folder}. Exiting.")
    exit(1)
except pd.errors.EmptyDataError as e:
    log(f"ERROR: One of the Excel files is empty or corrupted: {e}. Exiting.")
    exit(1)
except Exception as e:
    log(f"ERROR: An unexpected error occurred while reading input files: {e}. Exiting.")
    exit(1)

# Step 3: Merge DataFrames
merged_df = None
try:
    log("Attempting to merge DataFrames...")
    merged_df = pd.merge(scan_df, price_df, on='Barcode', how='left')
    
    # Ensure all required columns are present after merge
    expected_columns = ['Barcode', 'Article', 'Percentage', 'OriginalPrice']
    if not all(col in merged_df.columns for col in expected_columns):
        log(f"ERROR: Merged DataFrame is missing one or more expected columns. Expected: {expected_columns}, Found: {merged_df.columns.tolist()}. Exiting.")
        exit(1)

    merged_df = merged_df[expected_columns]
    
    # Apply transformation to OriginalPrice
    # Using .loc for safe assignment and handling potential non-numeric values
    merged_df['OriginalPrice'] = merged_df['OriginalPrice'].apply(
        lambda x: str(int(x)) if pd.notnull(x) and pd.api.types.is_numeric_dtype(type(x)) else ""
    )
    log("DataFrames merged and cleaned successfully.")
except KeyError as e:
    log(f"ERROR: Missing column during merge or selection: {e}. Check 'Barcode' column in both Excel files. Exiting.")
    exit(1)
except Exception as e:
    log(f"ERROR: An error occurred during data merging or cleaning: {e}. Exiting.")
    exit(1)

# Step 4: Save to SQLite
db_name = f"{version}.db"
db_path = os.path.join(ready_folder, db_name)
conn = None # Initialize conn to None for finally block
try:
    log(f"Attempting to save data to SQLite DB: {db_path}")
    conn = sqlite3.connect(db_path)
    merged_df.to_sql('data', conn, index=False, if_exists='replace',
                     dtype={'Barcode': 'TEXT', 'Article': 'TEXT', 'Percentage': 'TEXT', 'OriginalPrice': 'TEXT'})
    log("SQLite DB created successfully.")
except sqlite3.Error as e:
    log(f"ERROR: SQLite database error: {e}. Exiting.")
    exit(1)
except Exception as e:
    log(f"ERROR: An unexpected error occurred while saving to SQLite: {e}. Exiting.")
    exit(1)
finally:
    if conn:
        conn.close()
        log("SQLite connection closed.")

# Step 5: Zip the SQLite DB
zip_path = os.path.join(ready_folder, f"{version}.zip")
try:
    log(f"Attempting to zip the SQLite DB: {zip_path}")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf: # Added compression
        zipf.write(db_path, arcname=os.path.basename(db_path))
    log("DB zipped successfully.")
except FileNotFoundError as e:
    log(f"ERROR: DB file not found for zipping: {db_path}. {e}. Exiting.")
    exit(1)
except Exception as e:
    log(f"ERROR: An error occurred during DB zipping: {e}. Exiting.")
    exit(1)

# Step 6: Cleanup unzipped DB
try:
    log(f"Attempting to clean up raw DB file: {db_path}")
    if os.path.exists(db_path):
        os.remove(db_path)
        log("Cleaned up raw DB file after zipping.")
    else:
        log(f"Raw DB file not found, skipping cleanup: {db_path}")
except OSError as e: # Catch OSError for more specific file system errors
    log(f"ERROR: Could not delete raw DB file {db_path}: {e}. This might be due to permissions or the file being in use.")
except Exception as e:
    log(f"ERROR: An unexpected error occurred during cleanup of raw DB: {e}. Exiting.")

# Step 7: Cleanup input files
log("Attempting to clean up input files in 'csv' folder...")
for file in required_files:
    file_path_to_delete = os.path.join(csv_folder, file)
    try:
        if os.path.exists(file_path_to_delete):
            os.remove(file_path_to_delete)
            log(f"Successfully deleted: {file}")
        else:
            log(f"File not found, skipping deletion: {file_path_to_delete}")
    except OSError as e: # Catch OSError for more specific file system errors
        log(f"ERROR: Could not delete input file {file_path_to_delete}: {e}. This might be due to permissions or the file being in use.")
    except Exception as e:
        log(f"ERROR: An unexpected error occurred while deleting {file_path_to_delete}: {e}.")

log("--- Workflow Finished ---")
