#!/bin/bash

# Fail on first error
set -e

# Get the root of the project (one level above this script's directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_ROOT/data"
CRAWL=CC-MAIN-2017-13
INPUT_DIR="$DATA_DIR/crawl-data/$CRAWL/input"

OUTPUT_GRAPH="$DATA_DIR/output_graph"
mkdir -p "$OUTPUT_GRAPH"
VENV_PATH="$PROJECT_ROOT/.venv"

# Activate the virtual environment
source "$VENV_PATH/bin/activate"

# Set PySpark to use the virtualenv's Python
export PYSPARK_PYTHON="$VENV_PATH/bin/python"
export PYSPARK_DRIVER_PYTHON="$VENV_PATH/bin/python"


# Run the Spark job
/opt/spark/bin/spark-submit \
  --py-files "$PROJECT_ROOT/scripts/cc-scripts/sparkcc.py" \
  "$PROJECT_ROOT/scripts/cc-scripts/wat_extract_links.py" \
  "$INPUT_DIR/test_wat.txt" \
  "$DATA_DIR/wat_output_table" \
  --input_base_url https://data.commoncrawl.org/
