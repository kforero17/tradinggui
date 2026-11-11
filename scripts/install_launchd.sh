#!/usr/bin/env bash
# Install (or with --uninstall, remove) the TradingGui weekly and daily LaunchAgents.
# Usage: scripts/install_launchd.sh [--uninstall]
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AGENTS_DIR="$HOME/Library/LaunchAgents"
DOMAIN="gui/$(id -u)"
JOBS=(weekly daily)

uninstall=false
case "${1:-}" in
    "") ;;
    --uninstall) uninstall=true ;;
    *) echo "Usage: $0 [--uninstall]" >&2; exit 64 ;;
esac

if $uninstall; then
    for job in "${JOBS[@]}"; do
        label="com.tradinggui.$job"
        launchctl bootout "$DOMAIN/$label" 2>/dev/null || true
        rm -f "$AGENTS_DIR/$label.plist"
        echo "Removed $label"
    done
    exit 0
fi

if [[ ! -x "$REPO/.venv/bin/python" ]]; then
    echo "error: $REPO/.venv/bin/python not found; create the venv first" >&2
    exit 1
fi
if [[ "$REPO" == *"|"* ]]; then
    echo "error: the repo path must not contain '|'" >&2
    exit 1
fi

mkdir -p "$AGENTS_DIR" "$REPO/logs"
for job in "${JOBS[@]}"; do
    label="com.tradinggui.$job"
    template="$REPO/deploy/launchd/$label.plist"
    target="$AGENTS_DIR/$label.plist"
    sed "s|__REPO__|$REPO|g" "$template" > "$target"
    plutil -lint "$target" >/dev/null
    launchctl bootout "$DOMAIN/$label" 2>/dev/null || true
    launchctl bootstrap "$DOMAIN" "$target"
    echo "Installed $label -> $target"
done

cat <<HINTS

Check the agents:
  launchctl print $DOMAIN/com.tradinggui.weekly | grep -E 'state|last exit|runs'
  launchctl print $DOMAIN/com.tradinggui.daily | grep -E 'state|last exit|runs'
Run one now:
  launchctl kickstart -k $DOMAIN/com.tradinggui.daily
Logs: $REPO/logs/{weekly,daily}.{out,err}.log and $REPO/logs/trading.log
Remove: $0 --uninstall
HINTS
