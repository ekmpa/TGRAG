#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <CRAWL_ID> (e.g. CC-MAIN-2017-13)"
    exit 1
fi

CRAWL="$1"

# Get the root of the project (one level above this script's directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_ROOT/data"
INPUT_DIR="$DATA_DIR/crawl-data/$CRAWL/input"

# Remove re-created directories before running
rm -rf "$PROJECT_ROOT/spark-warehouse/*"

./get_data.sh "$CRAWL"
./run_wat_to_link.sh "$CRAWL"
./run_link_to_graph.sh "$CRAWL"

echo "Preprocessing graphs..."
