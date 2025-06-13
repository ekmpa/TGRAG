#!/bin/bash

# Usage: ./merge_pipeline.sh CC-MAIN-YYYY-NN [CC-MAIN-YYYY-NN ...]

python3 utils/temporal_merge.py "$@"