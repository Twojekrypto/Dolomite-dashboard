#!/bin/bash
# Double-click this file to launch the local Dolomite dashboard
# and open the Borrow → Risk Simulator page in your browser.

cd "$(dirname "$0")" || exit 1

PORT=8899
URL="http://localhost:${PORT}/borrow/"

echo ""
echo "  Starting Dolomite dashboard…"

# Free the port: stop whatever is already holding it (a stale server, etc.)
PIDS="$(lsof -ti tcp:${PORT} 2>/dev/null)"
if [ -n "$PIDS" ]; then
  echo "  Port ${PORT} was busy — clearing it…"
  kill $PIDS 2>/dev/null
  sleep 1
  PIDS="$(lsof -ti tcp:${PORT} 2>/dev/null)"
  [ -n "$PIDS" ] && kill -9 $PIDS 2>/dev/null
  sleep 1
fi

echo "  Opening: $URL"
echo "  (Keep this window open. Close it or press Ctrl+C to stop.)"
echo ""

# Open the browser once the server is actually responding (waits up to ~10s).
(
  for i in $(seq 1 20); do
    if curl -s -o /dev/null "http://localhost:${PORT}/"; then
      open "$URL"; break
    fi
    sleep 0.5
  done
) &

# Start the server (foreground; keeps this window alive).
node server.js
