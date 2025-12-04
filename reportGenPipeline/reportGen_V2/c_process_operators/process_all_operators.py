import psycopg2
from psycopg2 import sql
import os
import datetime
from datetime import datetime as dt, timezone
import traceback
from dotenv import load_dotenv
import glob
import subprocess
import time

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
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
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

def log_message(log_file, message):
    """Write a message to the log file with timestamp"""
    timestamp = dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S')
    print(message)
    with open(log_file, 'a') as lf:
        lf.write(f"{timestamp} - {message}\n")

def get_sql_file_path(relative_path):
    """Get absolute path to SQL files in ReportGen folder"""
    script_dir = os.path.dirname(__file__)
    # Navigate to ReportGen folder
    report_gen_dir = os.path.abspath(os.path.join(script_dir, '..', '..', 'ReportGen'))
    sql_file_path = os.path.join(report_gen_dir, relative_path)
    return sql_file_path

def execute_sql_file(conn, sql_file_path, table_name, log_file):
    """Execute a SQL file with proper error handling"""
    try:
        if not os.path.exists(sql_file_path):
            error_msg = f"ERROR: SQL file not found: {sql_file_path}"
            log_message(log_file, error_msg)
            return False
        
        with open(sql_file_path, 'r') as f:
            sql_content = f.read()
        
        # Replace table name references in SQL if needed
        # Most SQL files should reference 'car_wash_count' table
        sql_content = sql_content.replace('car_wash_count', table_name)
        
        cur = conn.cursor()
        cur.execute(sql_content)
        conn.commit()
        cur.close()
        
        return True
    except Exception as e:
        error_msg = f"ERROR executing SQL file {sql_file_path}: {str(e)}"
        log_message(log_file, error_msg)
        log_message(log_file, traceback.format_exc())
        conn.rollback()
        return False

def rename_table(conn, old_name, new_name, log_file):
    """Rename a table in the database"""
    try:
        cur = conn.cursor()
        
        # Drop the target table if it already exists
        try:
            drop_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(
                sql.Identifier(new_name)
            )
            cur.execute(drop_query)
            conn.commit()
        except:
            pass
        
        # Rename the table
        rename_query = sql.SQL("ALTER TABLE {} RENAME TO {};").format(
            sql.Identifier(old_name),
            sql.Identifier(new_name)
        )
        cur.execute(rename_query)
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        error_msg = f"ERROR renaming table {old_name} to {new_name}: {str(e)}"
        log_message(log_file, error_msg)
        conn.rollback()
        return False

def get_newly_created_tables(conn, log_file):
    """Get all tables that start with 'new_car_wash_' prefix"""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'new_car_wash_%'
            ORDER BY table_name
        """)
        tables = cur.fetchall()
        cur.close()
        
        table_names = [table[0] for table in tables]
        log_message(log_file, f"Found {len(table_names)} newly created tables: {', '.join(table_names)}")
        return table_names
    except Exception as e:
        error_msg = f"ERROR retrieving table list: {str(e)}"
        log_message(log_file, error_msg)
        return []

def process_single_operator(conn, table_name, logs_dir):
    """Process a single operator table through all transformation steps"""
    
    # Create log file for this operator
    operator_name = table_name.replace('new_car_wash_', '')
    operator_logs_dir = os.path.join(logs_dir, operator_name)
    os.makedirs(operator_logs_dir, exist_ok=True)
    
    log_file = os.path.join(operator_logs_dir, f"process_{operator_name}_{dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')}.log")
    
    log_message(log_file, f"\n{'='*70}")
    log_message(log_file, f"=== Starting processing for operator: {operator_name} ===")
    log_message(log_file, f"Table: {table_name}")
    log_message(log_file, f"Started at: {dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')}")
    log_message(log_file, f"{'='*70}")
    
    try:
        # Use standard working table name (same as bash script)
        working_table_name = "car_wash_count"
        
        # Define the transformation steps with their SQL files
        transformation_steps = [
            {
                "step": 1,
                "description": "Rename to active name (new_car_wash_* → car_wash_count)",
                "type": "rename",
                "old_name": table_name,
                "new_name": working_table_name
            },
            {
                "step": 2,
                "description": "Convert data types - Create timestamp column",
                "type": "sql",
                "sql_file": "b_convert_datatypes/create_ts.sql"
            },
            {
                "step": 3,
                "description": "Convert data types - Convert sales to integer",
                "type": "sql",
                "sql_file": "b_convert_datatypes/sales_int.sql"
            },
            {
                "step": 4,
                "description": "Add new column - Retail or Member classification",
                "type": "sql",
                "sql_file": "c_new_columns/retail_or_mem.sql"
            },
            {
                "step": 5,
                "description": "Add new column - Gross revenue calculation",
                "type": "sql",
                "sql_file": "c_new_columns/gross_revenue.sql"
            },
            {
                "step": 6,
                "description": "Create indexes",
                "type": "sql",
                "sql_file": "d_indexes/check_index.sql"
            },
            {
                "step": 7,
                "description": "Populate star schema - Site/Client dimension",
                "type": "sql",
                "sql_file": "f_populate_star/a_site_client.sql"
            },
            {
                "step": 8,
                "description": "Populate star schema - Quarter/Year dimension",
                "type": "sql",
                "sql_file": "f_populate_star/b_quarter_year.sql"
            },
            {
                "step": 9,
                "description": "Populate star schema - Count measures",
                "type": "sql",
                "sql_file": "f_populate_star/c_count.sql"
            },
            {
                "step": 10,
                "description": "Populate star schema - Revenue measures",
                "type": "sql",
                "sql_file": "f_populate_star/d_revenue.sql"
            },
            {
                "step": 11,
                "description": "Rename back to original name (car_wash_count → new_car_wash_*)",
                "type": "rename",
                "old_name": working_table_name,
                "new_name": table_name
            }
        ]
        
        # Execute each transformation step
        for step_info in transformation_steps:
            step_num = step_info["step"]
            description = step_info["description"]
            
            log_message(log_file, f"\n[STEP {step_num}] {description}")
            start_time = time.time()
            
            try:
                if step_info["type"] == "rename":
                    success = rename_table(conn, step_info["old_name"], step_info["new_name"], log_file)
                elif step_info["type"] == "sql":
                    sql_file_path = get_sql_file_path(step_info["sql_file"])
                    success = execute_sql_file(conn, sql_file_path, "car_wash_count", log_file)
                else:
                    success = False
                
                elapsed_time = time.time() - start_time
                
                if success:
                    log_message(log_file, f"✓ COMPLETED: {description} (took {elapsed_time:.2f}s)")
                else:
                    log_message(log_file, f"✗ FAILED: {description}")
                    log_message(log_file, f"\n{'='*70}")
                    log_message(log_file, f"=== PROCESSING FAILED for {operator_name} ===")
                    log_message(log_file, f"Failed at step {step_num}: {description}")
                    log_message(log_file, f"{'='*70}\n")
                    return False
                    
            except Exception as e:
                elapsed_time = time.time() - start_time
                error_msg = f"✗ ERROR in step {step_num}: {str(e)}"
                log_message(log_file, error_msg)
                log_message(log_file, traceback.format_exc())
                log_message(log_file, f"\n{'='*70}")
                log_message(log_file, f"=== PROCESSING FAILED for {operator_name} ===")
                log_message(log_file, f"Exception at step {step_num}: {description}")
                log_message(log_file, f"{'='*70}\n")
                return False
        
        # All steps completed successfully
        log_message(log_file, f"\n{'='*70}")
        log_message(log_file, f"✓ SUCCESS: All processing steps completed for {operator_name}")
        log_message(log_file, f"Completed at: {dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')}")
        log_message(log_file, f"{'='*70}\n")
        return True
        
    except Exception as e:
        error_msg = f"Unexpected error during processing: {str(e)}"
        log_message(log_file, error_msg)
        log_message(log_file, traceback.format_exc())
        log_message(log_file, f"\n{'='*70}")
        log_message(log_file, f"=== PROCESSING FAILED for {operator_name} ===")
        log_message(log_file, f"{'='*70}\n")
        return False

def process_all_operators():
    """Process all newly created tables through transformation pipeline"""
    
    # Get logs directory
    logs_dir = get_logs_directory()
    
    # Create summary log file
    summary_log = os.path.join(logs_dir, f"process_all_operators_summary_{dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')}.log")
    
    log_message(summary_log, f"\n{'='*70}")
    log_message(summary_log, f"=== PROCESS ALL OPERATORS STARTED ===")
    log_message(summary_log, f"Started at: {dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')}")
    log_message(summary_log, f"Logs directory: {logs_dir}")
    log_message(summary_log, f"{'='*70}\n")
    
    # Get database connection
    conn = get_connection()
    if not conn:
        error_msg = "ERROR: Unable to establish database connection"
        log_message(summary_log, error_msg)
        return False
    
    log_message(summary_log, "Connected to PostgreSQL successfully!")
    
    # Get list of newly created tables
    tables_to_process = get_newly_created_tables(conn, summary_log)
    
    if not tables_to_process:
        log_message(summary_log, "No tables to process. Exiting.")
        log_message(summary_log, f"\n{'='*70}\n")
        conn.close()
        return True
    
    log_message(summary_log, f"\nProcessing {len(tables_to_process)} table(s)...\n")
    
    # Process each table
    successful_count = 0
    failed_count = 0
    failed_tables = []
    
    for table_name in tables_to_process:
        log_message(summary_log, f"\n>>> Processing table: {table_name}")
        
        if process_single_operator(conn, table_name, logs_dir):
            successful_count += 1
            log_message(summary_log, f"✓ SUCCESS: {table_name}")
        else:
            failed_count += 1
            failed_tables.append(table_name)
            log_message(summary_log, f"✗ FAILED: {table_name}")
    
    # Close database connection
    conn.close()
    
    # Write final summary
    log_message(summary_log, f"\n{'='*70}")
    log_message(summary_log, f"=== PROCESS ALL OPERATORS COMPLETED ===")
    log_message(summary_log, f"Completed at: {dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')}")
    log_message(summary_log, f"{'='*70}")
    log_message(summary_log, f"\nSummary:")
    log_message(summary_log, f"  Total tables: {len(tables_to_process)}")
    log_message(summary_log, f"  Successful: {successful_count}")
    log_message(summary_log, f"  Failed: {failed_count}")
    
    if failed_tables:
        log_message(summary_log, f"\nFailed tables:")
        for table in failed_tables:
            log_message(summary_log, f"  - {table}")
    
    log_message(summary_log, f"\nSummary log: {summary_log}")
    log_message(summary_log, f"{'='*70}\n")
    
    return failed_count == 0

# Main execution
if __name__ == "__main__":
    success = process_all_operators()
    exit(0 if success else 1)
