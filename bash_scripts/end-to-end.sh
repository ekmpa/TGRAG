#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <CRAWL_ID> (e.g. CC-MAIN-2017-13)"
    exit 1
fi

CRAWL="$1"

# Get the root of the project (one level above this script's directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INPUT_DIR="$DATA_DIR/crawl-data/$CRAWL/input"

# Use SCRATCH if defined, else fallback to project-local data dir
# For cluster usage
if [ -z "$SCRATCH" ]; then
    echo "[WARN] SCRATCH not set, using local data directory."
    DATA_DIR="$PROJECT_ROOT/data"
    rm -rf "spark-warehouse" # Remove re-created directories before running
else
    DATA_DIR="$SCRATCH"
    rm -rf "$SCRATCH/spark-warehouse" # Remove re-created directories before running
    echo "Using SCRATCH directory: $DATA_DIR"
fi

# Remove re-created directories before running
# rm -rf "spark-warehouse"

./get_data.sh "$CRAWL"
./run_wat_to_link.sh "$CRAWL"
./run_link_to_graph.sh "$CRAWL"
