#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Ensure pip is new enough for PEP 660 editable installs with pyproject.toml
pip install --user --upgrade pip setuptools wheel 2>/dev/null || true

pip install --user -e "$SCRIPT_DIR"
echo "Installed bag-tool."

# mcap CLI (foxglove/mcap): `bag-tool index` shells out to it (sort / merge / recover / info).
# Pinned release into ~/bin/mcap, which `bag-tool index` searches after $MCAP_BIN and PATH.
# Override the version with MCAP_CLI_VERSION; set MCAP_CLI_SKIP=1 to skip.
MCAP_CLI_VERSION="${MCAP_CLI_VERSION:-v0.4.0}"
MCAP_DEST="$HOME/bin/mcap"
if [[ "${MCAP_CLI_SKIP:-0}" == 1 ]]; then
  echo "mcap CLI: skipped (MCAP_CLI_SKIP=1)."
elif [[ -x "$MCAP_DEST" ]] && "$MCAP_DEST" --version 2>/dev/null | grep -q "^mcap ${MCAP_CLI_VERSION#v} "; then
  echo "mcap CLI: $("$MCAP_DEST" --version) already in $MCAP_DEST."
else
  case "$(uname -s)" in
    Linux)  os=linux ;;
    Darwin) os=macos ;;
    *) echo "mcap CLI: unsupported OS $(uname -s); install it by hand." >&2; exit 0 ;;
  esac
  case "$(uname -m)" in
    x86_64|amd64)  arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    armv7l|arm)    arch=arm ;;
    *) echo "mcap CLI: unsupported arch $(uname -m); install it by hand." >&2; exit 0 ;;
  esac
  url="https://github.com/foxglove/mcap/releases/download/releases/mcap-cli/${MCAP_CLI_VERSION}/mcap-${os}-${arch}"
  mkdir -p "$(dirname "$MCAP_DEST")"
  tmp="$(mktemp)"
  if curl -fsSL -o "$tmp" "$url"; then
    chmod +x "$tmp" && mv "$tmp" "$MCAP_DEST"
    echo "mcap CLI: installed $("$MCAP_DEST" --version) -> $MCAP_DEST"
  else
    rm -f "$tmp"
    echo "mcap CLI: download failed ($url); bag-tool index needs it." >&2
  fi
fi
