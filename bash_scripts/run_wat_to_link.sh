#!/bin/bash

# Fail on first error
set -e

# Check if CRAWL argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <CRAWL-ID>"
    echo "Example: $0 CC-MAIN-2017-13"
    exit 1
fi

CRAWL="$1"

# Get the root of the project (one level above this script's directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Use SCRATCH if defined, else fallback to project-local data dir
# For cluster usage
if [ -z "$SCRATCH" ]; then
    echo "[WARN] SCRATCH not set, using local data directory."
    DATA_DIR="$PROJECT_ROOT/data"
else
    DATA_DIR="$SCRATCH"
    echo "Using SCRATCH directory: $DATA_DIR"
fi
INPUT_DIR="$DATA_DIR/crawl-data/$CRAWL/input"

VENV_PATH="$PROJECT_ROOT/venv"
SPARK_HOME="$HOME/spark"

# Activate the virtual environment
source "$VENV_PATH/bin/activate"

# Set PySpark to use the virtualenv's Python
export PYSPARK_PYTHON="$VENV_PATH/bin/python"
export PYSPARK_DRIVER_PYTHON="$VENV_PATH/bin/python"


# Run the Spark job
# Local testing: use "$INPUT_DIR/test_wat.txt"
# Cluster / full usage: ""$INPUT_DIR/all_wat_$CRAWL.txt"
"$SPARK_HOME/bin/spark-submit" \
  --py-files "$PROJECT_ROOT/tgrag/cc-scripts/sparkcc.py" \
  "$PROJECT_ROOT/tgrag/cc-scripts/wat_extract_links.py" \
  "$INPUT_DIR/all_wat_$CRAWL.txt" \
  "wat_output_table" \
  --input_base_url https://data.commoncrawl.org/
