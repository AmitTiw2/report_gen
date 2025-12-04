# ReportGen Pipeline - Complete Processing Guide

## Overview

The ReportGen pipeline consists of three main stages:

1. **Stage 1: Load CSV Data** (`b_load_data/loadData.py`)

   - Reads CSV files from the `data/` folder
   - Creates tables in PostgreSQL database
   - Tables are named: `car_wash_name` (from CSV filename)
   - Each table gets renamed to `new_car_wash_{operator_name}` pattern

2. **Stage 2: Process All Operators** (`c_process_operators/process_all_operators.py`)

   - Automatically discovers all `new_car_wash_*` tables
   - Applies data transformation pipeline to each table
   - Executes 11-step transformation process per operator
   - Includes comprehensive logging

3. **Master Orchestrator** (`run_pipeline.py`)
   - Coordinates both stages
   - Provides unified logging and monitoring
   - Can be run standalone to execute complete pipeline

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Master Orchestrator                        │
│                   (run_pipeline.py)                           │
└────────┬────────────────────────────────────┬────────────────┘
         │                                    │
         ▼                                    ▼
   ┌──────────────┐              ┌──────────────────────────┐
   │ Load CSV     │              │ Process All Operators     │
   │ (Stage 1)    │─────────────▶│ (Stage 2)                 │
   └──────────────┘              │                          │
         │                       │ For each table:          │
         │                       │ ├─ Rename table          │
         │                       │ ├─ Convert data types    │
         │                       │ ├─ Add new columns       │
         │                       │ ├─ Create indexes        │
         │                       │ ├─ Populate star schema  │
         │                       │ └─ Rename back           │
         │                       └──────────────────────────┘
         ▼                                    ▼
    ┌──────────────────────────────────────────────────┐
    │         PostgreSQL Database                      │
    │  - raw data tables (car_wash_*)                 │
    │  - processed tables (new_car_wash_*)            │
    │  - dimension tables (site, time, etc)           │
    │  - fact tables (counts, revenue)                │
    └──────────────────────────────────────────────────┘
```

## Transformation Pipeline (11 Steps per Operator)

### Step 1: Rename to Active Name

```
new_car_wash_{operator_name} → car_wash_count
```

- Temporary rename for processing

### Steps 2-3: Convert Data Types

- Create timestamp column from date/time fields
- Convert sales amounts to integer type

### Steps 4-5: Add New Columns

- Create retail_or_member classification
- Calculate gross_revenue

### Step 6: Create Indexes

- Add database indexes for query performance
- Validate index creation

### Steps 7-10: Populate Star Schema

- **Step 7**: Populate site_client dimension
- **Step 8**: Populate quarter_year dimension
- **Step 9**: Populate count measures (fact table)
- **Step 10**: Populate revenue measures (fact table)

### Step 11: Rename Back

```
car_wash_count → new_car_wash_{operator_name}
```

- Restore original table name

## File Structure

```
reportGen_V2/
├── b_load_data/
│   └── loadData.py                    # CSV loading script
├── c_process_operators/
│   └── process_all_operators.py       # Operator processing script
├── run_pipeline.py                    # Master orchestrator
├── .env                               # Database credentials
└── logs/                              # Log files (auto-created)
    ├── pipeline_master_*.log          # Master orchestrator logs
    ├── upload_summary_*.log           # CSV upload summary
    ├── {csv_name}.log                 # Individual CSV logs
    └── process_{operator}_*.log       # Operator processing logs
```

## Usage

### Option 1: Run Complete Pipeline (Recommended)

```bash
cd /Users/ankushkapoor/Desktop/Rocket\ Frog/ReportGen/reportGenPipeline/reportGen_V2
python3 run_pipeline.py
```

This will:

1. Load all CSV files from `data/` folder
2. Automatically process all newly created tables
3. Generate unified master log

### Option 2: Run Individual Stages

#### Load CSV Data Only

```bash
cd reportGen_V2/b_load_data/
python3 loadData.py
```

#### Process All Operators Only

```bash
cd reportGen_V2/c_process_operators/
python3 process_all_operators.py
```

## Logging

All logs are saved to the `logs/` directory with timestamps:

### Log Files Generated

1. **Master Log**: `pipeline_master_YYYYMMDD_HHMMSS.log`

   - Complete pipeline execution flow
   - Stage-by-stage results
   - Final summary

2. **Upload Logs**:

   - `upload_summary_*.log` - CSV upload summary
   - `{csv_name}.log` - Individual CSV processing logs

3. **Process Logs**: `process_{operator_name}_YYYYMMDD_HHMMSS.log`
   - Individual operator transformation logs
   - 11-step process details
   - Timing for each step
   - Error messages with stack traces

### Log Format

Each log entry includes:

```
[Timestamp] - [Message]
```

Example:

```
Nov-24-25 14:30:45 - STAGE 1: Load CSV Data
Nov-24-25 14:30:45 - Connected to PostgreSQL successfully!
Nov-24-25 14:30:45 - Found 2 CSV file(s):
Nov-24-25 14:30:46 -   - American Car Wash.csv
Nov-24-25 14:30:46 -   - Evergreen Car Wash.csv
```

## Expected Database Tables

After successful pipeline execution:

### Raw Data Tables (After Stage 1)

- `american_car_wash` (from "American Car Wash.csv")
- `evergreen_car_wash` (from "Evergreen Car Wash.csv")
- etc.

### Processed Tables (After Stage 2)

- `new_car_wash_american_car_wash`
- `new_car_wash_evergreen_car_wash`
- etc.

### Dimension Tables (Star Schema)

- `site_client` - Dimension table for sites/clients
- `quarter_year` - Dimension table for time periods

### Fact Tables (Star Schema)

- `count_fact` - Transaction counts by dimensions
- `revenue_fact` - Revenue measures by dimensions

## Environment Setup

Required `.env` file (in workspace root or reportGen_V2 directory):

```
DB_PASSWORD=your_database_password
```

The script will search for `.env` in these locations (in order):

1. `reportGen_V2/.env`
2. `reportGenPipeline/.env`
3. `.env` (workspace root)

## Error Handling

The pipeline includes comprehensive error handling:

1. **Connection Errors**: If database connection fails, pipeline aborts with clear error message
2. **File Errors**: If SQL files are not found, error is logged and step is skipped
3. **SQL Errors**: If SQL execution fails, transaction is rolled back and logged
4. **File Not Found**: If CSV or required files missing, detailed error is logged
5. **Timeout Protection**: 1-hour timeout for long-running stages

## Monitoring

To monitor the pipeline in real-time:

```bash
# Watch master log
tail -f logs/pipeline_master_*.log

# Watch individual operator logs
tail -f logs/process_*.log

# Check upload logs
tail -f logs/upload_summary_*.log
```

## Troubleshooting

### Pipeline Aborts at Stage 1 (Load CSV)

- Check `.env` file exists and has correct DB_PASSWORD
- Verify CSV files exist in `data/` folder
- Check database connection parameters in `loadData.py`
- Review logs in `logs/upload_summary_*.log`

### Pipeline Aborts at Stage 2 (Process Operators)

- Check SQL files exist in `ReportGen/` folder
- Verify table names match expected `new_car_wash_*` pattern
- Check database has sufficient disk space
- Review logs in `logs/process_*.log` for SQL errors

### Individual Table Processing Failed

- Check the specific operator's log: `logs/process_{operator_name}_*.log`
- Review which step failed
- Verify the SQL file exists and is syntactically correct
- Check table exists in database

## Performance Considerations

- CSV loading: ~1-2 minutes per 100K rows
- Processing per operator: ~2-5 minutes (includes indexes, star schema population)
- Total pipeline time for 2 operators: ~10-15 minutes

## Output Summary

After successful execution, check:

1. **Master Log**: `logs/pipeline_master_*.log` for overall status
2. **Upload Summary**: `logs/upload_summary_*.log` for CSV loading results
3. **Individual Logs**: `logs/process_{operator}_*.log` for each operator's details
4. **Database**: Query `information_schema.tables` to verify table creation

Example query to verify:

```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public'
AND (table_name LIKE 'car_wash_%' OR table_name LIKE 'new_car_wash_%')
ORDER BY table_name;
```
