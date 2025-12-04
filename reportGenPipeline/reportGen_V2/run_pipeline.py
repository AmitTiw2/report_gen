#!/usr/bin/env python3
"""
Master orchestration script for the ReportGen pipeline:
1. Load CSV data into database (loadData.py)
2. Process all operators through transformation pipeline (process_all_operators.py)
"""

import os
import sys
import subprocess
import datetime
from datetime import datetime as dt, timezone

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

def run_stage(stage_name, script_path, stage_num, master_log):
    """Run a single stage of the pipeline"""
    
    log_message(master_log, f"\n{'='*70}")
    log_message(master_log, f"STAGE {stage_num}: {stage_name}")
    log_message(master_log, f"{'='*70}")
    log_message(master_log, f"Started at: {dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')}")
    
    try:
        if not os.path.exists(script_path):
            error_msg = f"ERROR: Script not found: {script_path}"
            log_message(master_log, error_msg)
            return False
        
        log_message(master_log, f"Executing: {script_path}")
        
        # Run the script using the same Python interpreter as the current script
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=os.path.dirname(script_path),
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        # Log output
        if result.stdout:
            log_message(master_log, "--- STDOUT ---")
            for line in result.stdout.split('\n'):
                if line.strip():
                    log_message(master_log, line)
        
        if result.stderr:
            log_message(master_log, "--- STDERR ---")
            for line in result.stderr.split('\n'):
                if line.strip():
                    log_message(master_log, line)
        
        success = result.returncode == 0
        
        if success:
            log_message(master_log, f"✓ STAGE {stage_num} COMPLETED SUCCESSFULLY")
        else:
            log_message(master_log, f"✗ STAGE {stage_num} FAILED (exit code: {result.returncode})")
        
        log_message(master_log, f"Completed at: {dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')}")
        
        return success
        
    except subprocess.TimeoutExpired:
        error_msg = f"ERROR: STAGE {stage_num} timed out after 1 hour"
        log_message(master_log, error_msg)
        return False
    except Exception as e:
        error_msg = f"ERROR: Exception in STAGE {stage_num}: {str(e)}"
        log_message(master_log, error_msg)
        import traceback
        log_message(master_log, traceback.format_exc())
        return False

def main():
    """Main orchestration function"""
    
    # Get logs directory
    logs_dir = get_logs_directory()
    
    # Create master log file
    master_log = os.path.join(logs_dir, f"pipeline_master_{dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')}.log")
    
    print(f"\n{'='*70}")
    print(f"REPORTGEN PIPELINE ORCHESTRATOR")
    print(f"{'='*70}")
    print(f"Master log: {master_log}\n")
    
    log_message(master_log, f"\n{'='*70}")
    log_message(master_log, f"REPORTGEN PIPELINE ORCHESTRATOR STARTED")
    log_message(master_log, f"{'='*70}")
    log_message(master_log, f"Started at: {dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')}")
    log_message(master_log, f"Logs directory: {logs_dir}")
    log_message(master_log, f"{'='*70}\n")
    
    # Get the base directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define pipeline stages
    stages = [
        {
            "name": "Load CSV Data",
            "script": os.path.join(script_dir, "b_load_data", "loadData.py")
        },
        {
            "name": "Process All Operators",
            "script": os.path.join(script_dir, "c_process_operators", "process_all_operators.py")
        }
    ]
    
    # Execute each stage
    results = {}
    stage_num = 1
    
    for stage in stages:
        success = run_stage(stage["name"], stage["script"], stage_num, master_log)
        results[stage["name"]] = success
        stage_num += 1
        
        if not success:
            log_message(master_log, f"\nPipeline FAILED at stage: {stage['name']}")
            log_message(master_log, "Aborting remaining stages.")
            break
    
    # Write final summary
    log_message(master_log, f"\n{'='*70}")
    log_message(master_log, f"PIPELINE SUMMARY")
    log_message(master_log, f"{'='*70}")
    
    all_success = True
    for stage_name, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        log_message(master_log, f"{status}: {stage_name}")
        if not success:
            all_success = False
    
    log_message(master_log, f"\n{'='*70}")
    if all_success:
        log_message(master_log, f"✓ PIPELINE COMPLETED SUCCESSFULLY")
    else:
        log_message(master_log, f"✗ PIPELINE FAILED")
    
    log_message(master_log, f"Completed at: {dt.now(timezone.utc).strftime('%b-%d-%y %H:%M:%S UTC')}")
    log_message(master_log, f"Master log: {master_log}")
    log_message(master_log, f"{'='*70}\n")
    
    return 0 if all_success else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
