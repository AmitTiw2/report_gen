#!/usr/bin/env bash
# Quick Start Guide for ReportGen Pipeline

echo "=================================="
echo "ReportGen Pipeline - Quick Start"
echo "=================================="
echo

# Check if we're in the right directory
if [ ! -f "run_pipeline.py" ]; then
    echo "ERROR: Please run this script from the reportGen_V2 directory"
    echo "Example: cd reportGenPipeline/reportGen_V2 && ./quick_start.sh"
    exit 1
fi

echo "Checking environment..."
echo

# Check if .env file exists
if [ ! -f ".env" ] && [ ! -f "../.env" ] && [ ! -f "../../.env" ]; then
    echo "⚠️  WARNING: .env file not found"
    echo "   You need to set DB_PASSWORD environment variable or create .env file"
    echo "   .env should contain: DB_PASSWORD=your_password"
    echo
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check if Python3 is installed
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python3 is not installed"
    exit 1
fi

echo "✓ Environment check passed"
echo

# Display pipeline information
echo "Pipeline Configuration:"
echo "  - CSV Data Location: ../../data/"
echo "  - ReportGen Scripts: ../../ReportGen/"
echo "  - Logs Location: ./logs/"
echo "  - Database: test_reportgen (via .env)"
echo

# Create logs directory if it doesn't exist
mkdir -p logs
echo "✓ Logs directory ready"
echo

# Ask user to confirm
echo "=================================="
echo "Ready to start pipeline execution"
echo "=================================="
echo "This will:"
echo "1. Load all CSV files from the data folder"
echo "2. Process all newly created tables through transformation pipeline"
echo "3. Generate comprehensive logs in logs/ folder"
echo

read -p "Start pipeline? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Pipeline cancelled."
    exit 0
fi

echo
echo "Starting pipeline..."
echo "You can monitor logs with: tail -f logs/pipeline_master_*.log"
echo

# Run the pipeline
python3 run_pipeline.py

# Check result
if [ $? -eq 0 ]; then
    echo
    echo "✓ Pipeline completed successfully!"
    echo
    echo "Check logs for details:"
    ls -ltr logs/pipeline_master_*.log | tail -1 | awk '{print "  Master: logs/" $NF}'
    echo
    echo "View master log:"
    echo "  cat $(ls -t logs/pipeline_master_*.log | head -1)"
else
    echo
    echo "✗ Pipeline failed"
    echo
    echo "Check logs for errors:"
    echo "  cat $(ls -t logs/pipeline_master_*.log | head -1)"
    exit 1
fi
