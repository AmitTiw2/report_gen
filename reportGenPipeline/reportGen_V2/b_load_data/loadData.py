import psycopg2
from psycopg2 import sql
import os
import pandas as pd
import datetime
from datetime import datetime as dt, timezone
import traceback
from dotenv import load_dotenv
import glob
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Now use absolute import
from reportGen_V2.validations.M1_Validation.m1_Tech_and_data_validation import validate_m1_csv
# Load environment variables
candidate_env_paths = [
    os.path.join(os.path.dirname(__file__), '..', '.env'),
    os.path.join(os.path.dirname(__file__), '..', '..', '.env'),
]

env_path = None
for path in candidate_env_paths:
    if os.path.exists(path):
        env_path = path
        break

if env_path:
    load_dotenv(env_path)
    print(f"Loaded environment variables from: {env_path}")
else:
    print("Warning: .env file not found. DB_PASSWORD must be set as environment variable.")

def get_connection():
    """Establish a connection to PostgreSQL database"""
    try:
        db_password = os.getenv("DB_PASSWORD")
        if not db_password:
            print("Error: DB_PASSWORD environment variable not found!")
            return None
        
        # conn = psycopg2.connect(
        #     host="qv-repgendev-psql1.postgres.database.azure.com",
        #     port="5432",
        #     database="test_reportgen",
        #     user="sondbadmin",
        #     password=db_password
        # )
        conn = psycopg2.connect(
        host="127.0.0.1",
        port="5432",
        database="test_reportgen",
        user="postgres",
        password="titandevil@12",
        connect_timeout=10
    )
        print("Connected to PostgreSQL successfully!")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        return None

def get_logs_directory():
    """Locate or create the logs directory"""
    candidate_dirs = [
        os.path.join(os.path.dirname(__file__), 'logs'),
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs')),
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs')),
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'logs')),
    ]
    
    logs_dir = None
    for d in candidate_dirs:
        if os.path.isdir(d):
            logs_dir = d
            break
    
    if not logs_dir:
        logs_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(logs_dir, exist_ok=True)
    
    return logs_dir

def get_operator_logs_directory(logs_dir, operator_name):
    """Get or create operator-specific logs directory"""
    operator_logs_dir = os.path.join(logs_dir, operator_name)
    os.makedirs(operator_logs_dir, exist_ok=True)
    return operator_logs_dir

def log_message(log_file, message):
    """Write a message to the log file with timestamp"""
    timestamp = dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S')
    with open(log_file, 'a') as lf:
        lf.write(f"{timestamp} - {message}\n")

def upload_csv_to_db(csv_file_path, logs_dir):
    """Upload a single CSV file to the database with logging - optimized for large files"""
    
    # Generate log file name based on CSV file name
    csv_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    
    # Get operator name from CSV file name (convert to lowercase, replace spaces/hyphens with underscores)
    operator_name = csv_name.lower().replace(' ', '_').replace('-', '_')
    operator_logs_dir = get_operator_logs_directory(logs_dir, operator_name)
    
    log_file = os.path.join(operator_logs_dir, f"{csv_name}.log")
    
    # Initialize log file
    start_ts = dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S')
    log_message(log_file, f"--- START UPLOAD: {csv_name} ---")
    log_message(log_file, f"CSV File: {csv_file_path}")
    
    try:
        # Check if CSV file exists
        if not os.path.exists(csv_file_path):
            error_msg = f"Error: CSV file '{csv_file_path}' not found!"
            print(error_msg)
            log_message(log_file, error_msg)
            return False
        
        # Get file size
        file_size = os.path.getsize(csv_file_path)
        log_message(log_file, f"File Size: {file_size} bytes ({file_size / (1024**3):.2f} GB)")
        
        # For large files (> 100MB), use chunked reading to get metadata
        is_large_file = file_size > 100 * 1024 * 1024  # 100MB threshold
        
        log_message(log_file, "Reading CSV metadata...")
        try:
            # Read only the first chunk to get column names and estimate row count
            if is_large_file:
                log_message(log_file, "Large file detected - using chunked processing")
                log_message(log_file, f"Processing large file: {csv_name} ({file_size / (1024**3):.2f} GB)")
                
                # Read first chunk to get columns
                chunk_iterator = pd.read_csv(csv_file_path, chunksize=10000)
                first_chunk = next(chunk_iterator)
                columns = first_chunk.columns.tolist()
                col_count = len(columns)
                
                # Count total rows efficiently using chunked reading
                row_count = len(first_chunk)
                chunk_num = 1
                for chunk in chunk_iterator:
                    row_count += len(chunk)
                    chunk_num += 1
                    if chunk_num % 100 == 0:
                        print(f"  Counting rows... processed {row_count:,} rows")
                
                log_message(log_file, f"CSV metadata loaaded - Rows: {row_count}, Columns: {col_count}")
                print(f"File contains {row_count:,} rows, {col_count} columns")
            else:
                # For smaller files, read normally
                df = pd.read_csv(csv_file_path)
                row_count = len(df)
                columns = df.columns.tolist()
                col_count = len(columns)
                log_message(log_file, f"CSV loaded successfully - Rows: {row_count}, Columns: {col_count}")
                print(f"Loaded {csv_name}: {row_count} rows, {col_count} columns")
                
        except Exception as e:
            error_msg = f"Error reading CSV file: {str(e)}"
            print(error_msg)
            log_message(log_file, error_msg)
            log_message(log_file, traceback.format_exc())
            return False
        
        # Get database connection
        conn = get_connection()
        if not conn:
            error_msg = "ERROR: Unable to get DB connection"
            print(error_msg)
            log_message(log_file, error_msg)
            return False
        
        # Create table name from CSV file name (sanitized) with new_car_wash_ prefix
        table_name = f"new_car_wash_{csv_name.lower().replace(' ', '_').replace('-', '_')}"
        
        log_message(log_file, f"Target Table: {table_name}")
        log_message(log_file, f"Inserting {row_count} rows...")
        
        cur = conn.cursor()
        
        try:
            log_message(log_file, f"Columns: {', '.join(columns)}")
            
            # Create table if it doesn't exist (auto-create with all columns as TEXT)
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            create_table_query += ", ".join([f"{col.lower().replace(' ', '_').replace('-', '_')} TEXT" for col in columns])
            create_table_query += ");"
            
            cur.execute(create_table_query)
            conn.commit()
            log_message(log_file, f"Table '{table_name}' ensured/created successfully")
            
            # Insert data into table using COPY for bulk loading (fastest method)
            column_names = [col.lower().replace(' ', '_').replace('-', '_') for col in columns]
            
            # Use COPY FROM with CSV format for bulk insert
            try:
                import sys
                import time
                
                log_message(log_file, "Starting bulk upload using COPY command...")
                print(f"Uploading data to database...", flush=True)
                
                upload_start = time.time()
                
                # For large files, stream directly from file instead of loading into memory
                if is_large_file:
                    log_message(log_file, f"Using streaming COPY for large file ({file_size / (1024**3):.2f} GB)")
                    print(f"  Streaming {row_count:,} rows to PostgreSQL...", flush=True)
                    
                    # Open file and skip header
                    with open(csv_file_path, 'r', encoding='utf-8') as f:
                        # Skip the header line
                        next(f)
                        
                        # Use COPY command with file object for streaming
                        copy_sql = f"COPY {table_name} ({', '.join(column_names)}) FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '', QUOTE '\"')"
                        
                        try:
                            cur.copy_expert(copy_sql, f)
                            conn.commit()
                        except Exception as copy_error:
                            error_msg = f"COPY streaming failed: {str(copy_error)}"
                            log_message(log_file, error_msg)
                            log_message(log_file, traceback.format_exc())
                            raise  # Re-raise to trigger fallback
                else:
                    # For smaller files, use the in-memory approach
                    from io import StringIO
                    
                    log_message(log_file, "Using in-memory COPY for smaller file")
                    
                    # Convert DataFrame to CSV string buffer
                    csv_buffer = StringIO()
                    df.to_csv(csv_buffer, index=False, header=False, quoting=1)  # quoting=1 is QUOTE_ALL
                    csv_buffer.seek(0)
                    
                    # Use COPY command for fast bulk insert
                    copy_sql = f"COPY {table_name} ({', '.join(column_names)}) FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL 'None')"
                    cur.copy_expert(copy_sql, csv_buffer)
                    conn.commit()
                
                upload_duration = time.time() - upload_start
                rows_per_sec = row_count / upload_duration if upload_duration > 0 else 0
                
                log_message(log_file, f"Successfully inserted all {row_count} rows using COPY command in {upload_duration:.1f} seconds ({rows_per_sec:,.0f} rows/sec)")
                print(f"✓ Successfully uploaded {row_count:,} rows to '{table_name}' in {upload_duration:.1f}s ({rows_per_sec:,.0f} rows/sec)", flush=True)
                
            except Exception as e:
                # Fallback to chunked batch insert if COPY fails
                error_msg = f"COPY command failed, falling back to chunked batch insert: {str(e)}"
                print(error_msg)
                log_message(log_file, error_msg)
                log_message(log_file, traceback.format_exc())
                
                # Rollback any partial transaction
                conn.rollback()
                
                insert_query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(['%s'] * len(columns))})"
                
                # Use chunked reading for large files
                batch_size = 1000
                total_inserted = 0
                
                if is_large_file:
                    log_message(log_file, "Using chunked batch insert for large file...")
                    chunk_iterator = pd.read_csv(csv_file_path, chunksize=batch_size)
                    
                    for chunk_num, chunk in enumerate(chunk_iterator, 1):
                        rows_to_insert = []
                        
                        for _, row in chunk.iterrows():
                            # Convert row to tuple, handling NaN values
                            row_tuple = tuple(None if pd.isna(val) else str(val) for val in row)
                            rows_to_insert.append(row_tuple)
                        
                        try:
                            cur.executemany(insert_query, rows_to_insert)
                            conn.commit()
                            total_inserted += len(rows_to_insert)
                            
                            if chunk_num % 100 == 0:
                                log_message(log_file, f"Inserted {total_inserted:,} rows...")
                                print(f"  Progress: {total_inserted:,} / {row_count:,} rows ({100*total_inserted/row_count:.1f}%)")
                                
                        except Exception as e:
                            error_msg = f"Error inserting chunk {chunk_num}: {str(e)}"
                            print(error_msg)
                            log_message(log_file, error_msg)
                            log_message(log_file, traceback.format_exc())
                            conn.rollback()
                            cur.close()
                            conn.close()
                            return False
                else:
                    # For smaller files, use the original batch approach
                    for i in range(0, len(df), batch_size):
                        batch = df.iloc[i:i+batch_size]
                        rows_to_insert = []
                        
                        for _, row in batch.iterrows():
                            # Convert row to tuple, handling NaN values
                            row_tuple = tuple(None if pd.isna(val) else str(val) for val in row)
                            rows_to_insert.append(row_tuple)
                        
                        try:
                            cur.executemany(insert_query, rows_to_insert)
                            conn.commit()
                            log_message(log_file, f"Inserted batch {i//batch_size + 1}: rows {i+1} to {min(i+batch_size, len(df))}")
                        except Exception as e:
                            error_msg = f"Error inserting batch {i//batch_size + 1}: {str(e)}"
                            print(error_msg)
                            log_message(log_file, error_msg)
                            log_message(log_file, traceback.format_exc())
                            conn.rollback()
                            cur.close()
                            conn.close()
                            return False
                
                log_message(log_file, f"Successfully inserted all rows using batch insert")
                print(f"✓ Successfully uploaded {row_count:,} rows using batch insert")
            
            # Success
            success_msg = f"SUCCESS: Uploaded {row_count} rows to table '{table_name}'"
            print(success_msg)
            log_message(log_file, success_msg)
            
            cur.close()
            conn.close()
            
            finish_ts = dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S')
            log_message(log_file, f"--- END UPLOAD: {csv_name} at {finish_ts} UTC ---\n")
            return True
            
        except Exception as e:
            error_msg = f"Error executing insert operation: {str(e)}"
            print(error_msg)
            log_message(log_file, error_msg)
            log_message(log_file, traceback.format_exc())
            conn.rollback()
            cur.close()
            conn.close()
            return False
    
    except Exception as e:
        error_msg = f"Unexpected error during CSV upload: {str(e)}"
        print(error_msg)
        log_message(log_file, error_msg)
        log_message(log_file, traceback.format_exc())
        return False

def upload_all_csvs():
    """Upload all CSV files from the data folder"""
    
    # Get the data folder path (navigate up from current script location)
    script_dir = os.path.dirname(__file__)
    data_folder = os.path.abspath(os.path.join(script_dir, '..', '..', '..', 'data'))
    
    # Get logs directory
    logs_dir = get_logs_directory()
    
    print(f"\n{'='*60}")
    print(f"CSV Upload Process Started")
    print(f"{'='*60}")
    print(f"Data folder: {data_folder}")
    print(f"Logs folder: {logs_dir}")
    print(f"{'='*60}\n")
    
    # Find all CSV files in data folder
    csv_files = glob.glob(os.path.join(data_folder, '*.csv'))
    csv_list = [Path(f) for f in csv_files]
    validate_m1_csv(csv_list, project_root / "logs" / "m1")
    return
    if not csv_files:
        print(f"No CSV files found in {data_folder}")
        return False
    
    print(f"Found {len(csv_files)} CSV file(s):\n")
    for csv_file in csv_files:
        print(f"  - {os.path.basename(csv_file)}")
    print()
    
    # Create summary log file in root logs folder
    timestamp = dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')
    summary_log = os.path.join(logs_dir, f"upload_summary_{timestamp}.log")
    log_message(summary_log, "CSV Upload Summary")
    log_message(summary_log, f"Started at: {dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')}")
    log_message(summary_log, f"Total CSV files to upload: {len(csv_files)}")
    log_message(summary_log, "")
    
    # Upload each CSV
    successful_uploads = 0
    failed_uploads = 0
    
    for csv_file in sorted(csv_files):
        csv_name = os.path.basename(csv_file)
        print(f"\nUploading: {csv_name}...")
        
        if upload_csv_to_db(csv_file, logs_dir):
            successful_uploads += 1
            log_message(summary_log, f"✓ SUCCESS: {csv_name}")
        else:
            failed_uploads += 1
            log_message(summary_log, f"✗ FAILED: {csv_name}")
    
    # Write summary
    print(f"\n{'='*60}")
    print(f"Upload Process Completed")
    print(f"{'='*60}")
    print(f"Total files: {len(csv_files)}")
    print(f"Successful: {successful_uploads}")
    print(f"Failed: {failed_uploads}")
    print(f"{'='*60}\n")
    
    log_message(summary_log, "")
    log_message(summary_log, f"Upload Summary:")
    log_message(summary_log, f"  Total files: {len(csv_files)}")
    log_message(summary_log, f"  Successful: {successful_uploads}")
    log_message(summary_log, f"  Failed: {failed_uploads}")
    log_message(summary_log, f"Completed at: {dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')}")
    
    print(f"Summary log: {summary_log}")
    
    return failed_uploads == 0

# Main execution
if __name__ == "__main__":
    success = upload_all_csvs()
    exit(0 if success else 1)
