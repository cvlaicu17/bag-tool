#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if pip install -e "$SCRIPT_DIR" 2>/dev/null; then
    echo "Installed bag-tool."
else
    echo "Retrying with --break-system-packages (Ubuntu externally-managed env)..."
    pip install -e "$SCRIPT_DIR" --break-system-packages
    echo "Installed bag-tool."
fi
