#!/bin/bash

# Usage: ./download_slice.sh CC-MAIN-YYYY-NN

export PATH="/opt/homebrew/bin:$PATH"
set -euo pipefail

# 0: Clean up spark-warehouse (for multiple runs)
echo ">>> Cleaning spark-warehouse..."
rm -rf spark-warehouse/*

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <CC-MAIN-YYYY-NN>"
  exit 1
fi

SLICE="$1" # Time slice, e.g., CC-MAIN-2014-15

echo ">>> Fetching WAT paths for slice $SLICE"

# 1: Download the wat.paths.gz for the given slice
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

# 2: Export CURRENT_SLICE env variable
export CURRENT_SLICE=$(echo "${SLICE}" | tr '-' '_')
echo "CURRENT_SLICE set to: $CURRENT_SLICE"

# 3: Randomly select 25 WAT paths
echo ">>> Selecting 25 random WAT paths..."
/opt/homebrew/bin/gshuf wat.paths | head -n 25 > selected_paths.txt || true
echo "Selected 25 random WAT paths."

# 4: Create wat_files directory and clear old WAT files
mkdir -p wat_files
echo ">>> Cleaning old WAT files in wat_files/..."
rm -f wat_files/*.warc.wat.gz || true

# 5: Download WAT files (only if not already present)
echo ">>> Downloading WAT files..."
i=0
while read -r path; do
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
  i=$((i + 1))
done < selected_paths.txt

# 6: Create input_paths.txt (only from current wat_files/ )
echo ">>> Creating input_paths.txt..."
> input_paths.txt  # empty the file first
for file in wat_files/*.warc.wat.gz; do
  abs_path=$(realpath "$file")
  echo "file://$abs_path" >> input_paths.txt
done

echo ">>> Done! Ready to run Spark scripts."
