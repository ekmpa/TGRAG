#!/bin/bash
set -e

# Create and prepare venv
uv venv virt_env
uv sync
# Confirm Python path
PYTHON_BIN="./virt_env/bin/python"
echo "Using: $PYTHON_BIN"

# Test
$PYTHON_BIN -c "import idna; print(idna.__version__)"
