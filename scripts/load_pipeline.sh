#!/bin/bash

# Usage: ./download_slice.sh CC-MAIN-YYYY-NN [NUM_FILES]

export PATH="/opt/homebrew/bin:$PATH"
set -euo pipefail

# 0: Clean up spark-warehouse (for multiple runs)
echo ">>> Cleaning spark-warehouse..."
rm -rf spark-warehouse/* || true

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <CC-MAIN-YYYY-NN> [NUM_FILES]"
  exit 1
fi

SLICE="$1"  # Time slice, e.g., CC-MAIN-2014-15
NUM_FILES="${2:-25}"  # Optional second argument; default to 25 if not set

echo ">>> Fetching WAT paths for slice $SLICE"

# Download the wat.paths.gz for the given slice
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

# Export CURRENT_SLICE env variable
export CURRENT_SLICE=$(echo "${SLICE}" | tr '-' '_')

# Count total available WAT files
TOTAL_FILES=$(wc -l < wat.paths | xargs)
echo ">>> Requesting $NUM_FILES files out of $TOTAL_FILES WAT files available in slice $SLICE."

mkdir -p wat_files

# Detect existing WAT files
existing_files=()
for f in wat_files/*.warc.wat.gz; do
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
  /opt/homebrew/bin/gshuf wat.paths | head -n "$NUM_FILES" > selected_paths.txt || true
fi

# Download WAT files (only if not already present)
echo ">>> Downloading WAT files..."
i=0
while read -r path; do
  # If 'path' is a local file (existing) -> skip download
  if [[ "$path" == wat_files/* ]]; then
    echo "[$i] Skipping download, already have $path"
  else
    filename=$(basename "$path")
    target_path="wat_files/$filename"
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
  fi
  i=$((i + 1))
done < selected_paths.txt

# create input_paths.txt (only from current wat_files/)
echo ">>> Creating input_paths.txt..."
> input_paths.txt  # empty the file first
for file in wat_files/*.warc.wat.gz; do
  abs_path=$(realpath "$file")
  echo "file://$abs_path" >> input_paths.txt
done

echo ">>> Done! Using $NUM_FILES out of $TOTAL_FILES available files. Ready to run Spark scripts."