#!/bin/bash
set -e

# Check if crawl list file is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <crawl-list.txt>"
    exit 1
fi

CRAWL_LIST_FILE="$1"

if [ ! -f "$CRAWL_LIST_FILE" ]; then
    echo "File not found: $CRAWL_LIST_FILE"
    exit 1
fi

# Get the root of the project
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"


while read -r CRAWL || [[ -n "$CRAWL" ]]; do
    # Skip empty lines or comments
    [[ -z "$CRAWL" || "$CRAWL" =~ ^# ]] && continue

    echo "Processing $CRAWL..."
    echo "Removing previous $CRAWL spark-warehouse"
    rm -rf "$PROJECT_ROOT/bash_scripts/spark-warehouse"
    ./get_data.sh "$CRAWL"
    echo "Data Downloaded for $CRAWL."

    ./run_wat_to_link.sh "$CRAWL"
    echo "wat_output_table constructed for $CRAWL."

    ./run_link_to_graph.sh "$CRAWL"
    echo "Compressed graphs constructed for $CRAWL."

    echo "--------------------------------------"
done < "$CRAWL_LIST_FILE"
