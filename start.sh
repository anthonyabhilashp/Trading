#!/bin/bash
cd "$(dirname "$0")"

source venv/bin/activate
pip install -e . --quiet

# Kill anything on port 5001
fuser -k 5001/tcp 2>/dev/null

sleep 1

nohup kite-server --host 127.0.0.1 --port 5001 > server.log 2>&1 &
echo "Server started (PID: $!) — logs: tail -f server.log"
