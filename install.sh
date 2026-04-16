#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Ensure pip is new enough for PEP 660 editable installs with pyproject.toml
pip install --user --upgrade pip setuptools wheel 2>/dev/null || true

pip install --user -e "$SCRIPT_DIR"
echo "Installed bag-tool."
