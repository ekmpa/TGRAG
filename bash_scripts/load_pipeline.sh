#!/bin/bash

# Usage: ./download_slice.sh CC-MAIN-YYYY-NN [NUM_FILES]

# For cluster usage
#module load python/3.8
#source .venv/bin/activate
#uv sync

export PATH="/opt/homebrew/bin:$PATH"
set -euo pipefail

# 0: Clean up spark-warehouse (for multiple runs)
echo ">>> Cleaning spark-warehouse..."
rm -rf spark-warehouse/* || true

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <CC-MAIN-YYYY-NN> [NUM_FILES]"
  exit 1
fi

SLICE="$1"  # e.g., CC-MAIN-2015-35
NUM_FILES="${2:-25}"  # default to 25

# For local usage :
SLICE_DIR="wat_files/${SLICE}"
# for clusters:
# SLICE_DIR="/network/scratch/k/kondrupe/wat_files/${SLICE}"
export CURRENT_SLICE=$(echo "${SLICE}" | tr '-' '_')

echo ">>> Fetching WAT paths for slice $SLICE"

# Download wat.paths.gz
if curl -O "https://data.commoncrawl.org/crawl-data/${SLICE}/wat.paths.gz"; then
  echo "Downloaded wat.paths.gz successfully."
else
  echo "Failed to download wat.paths.gz."
  exit 1
fi

# Uncompress
if gunzip -f wat.paths.gz; then
  echo "Unzipped wat.paths.gz."
else
  echo "Failed to unzip wat.paths.gz."
  exit 1
fi

# Count total available WAT files
TOTAL_FILES=$(wc -l < wat.paths | xargs)
echo ">>> Requesting $NUM_FILES files out of $TOTAL_FILES WAT files available in slice $SLICE."

mkdir -p "$SLICE_DIR"

# Detect existing WAT files for this slice
existing_files=()
for f in "$SLICE_DIR"/*.warc.wat.gz; do
  if [[ -f "$f" ]]; then
    existing_files+=("$f")
  fi
done

# Prepare selected_paths.txt
echo ">>> Preparing list of WAT paths..."
if [ ${#existing_files[@]} -ge "$NUM_FILES" ]; then
  echo "Found ${#existing_files[@]} existing WAT files, using them."
  printf "%s\n" "${existing_files[@]}" | head -n "$NUM_FILES" > selected_paths.txt
else
  echo "Not enough existing WAT files (${#existing_files[@]}), selecting new ones."
  shuf wat.paths | head -n "$NUM_FILES" > selected_paths.txt || true
fi

# Download WAT files
echo ">>> Downloading WAT files..."
i=0
while read -r path; do
  filename=$(basename "$path")
  target_path="${SLICE_DIR}/${filename}"
  if [ -f "$target_path" ]; then
    echo "[$i] Skipping $filename (already exists)."
  else
    echo "[$i] Downloading https://data.commoncrawl.org/${path}..."
    if curl -o "$target_path" "https://data.commoncrawl.org/${path}"; then
      echo "[$i] Success."
    else
      echo "[$i] Failed to download: ${path}"
    fi
  fi
  i=$((i + 1))
done < selected_paths.txt

# Create input_paths.txt
echo ">>> Creating input_paths.txt..."
> input_paths.txt
for file in "$SLICE_DIR"/*.warc.wat.gz; do
  abs_path=$(realpath "$file")
  echo "file://$abs_path" >> input_paths.txt
done

echo ">>> Done! Using $NUM_FILES out of $TOTAL_FILES available files."

export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)

# Define graph file locations
VERTICES_FILE="external/cc-webgraph/${CURRENT_SLICE}/vertices.txt.gz"
EDGES_FILE="external/cc-webgraph/${CURRENT_SLICE}/edges.txt.gz"

# Remove any old graph files
if [[ -f "$VERTICES_FILE" || -f "$EDGES_FILE" ]]; then
  echo ">>> Removing existing graph files."
  rm -f "$VERTICES_FILE" "$EDGES_FILE"
fi

echo ">>> Running external scripts."
python3 external_run/run_external_scripts.py

# Show final graph stats
if [[ -f "$VERTICES_FILE" && -f "$EDGES_FILE" ]]; then
  echo ">>> Final graph statistics:"
  num_nodes=$(gunzip -c "$VERTICES_FILE" | wc -l)
  num_edges=$(gunzip -c "$EDGES_FILE" | wc -l)
  echo "Graph statistics for slice $SLICE:"
  echo "  Nodes: $num_nodes"
  echo "  Edges: $num_edges"
else
  echo ">>> Graph files not found, cannot compute final statistics."
fi

echo ">>> Running label matching."
$PYSPARK_PYTHON utils/load_labels.py
