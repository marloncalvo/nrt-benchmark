#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

# Create venv if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
  echo "Virtual environment created at $VENV_DIR"
fi

# Install dependencies
"$VENV_DIR/bin/pip" install -r "$SCRIPT_DIR/requirements.txt"

# Run the Python initialization script
"$VENV_DIR/bin/python" "$SCRIPT_DIR/initialize.py"
