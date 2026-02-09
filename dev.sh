#!/bin/bash
# Run Dagster dev server
#
# Usage:
#   ./dev.sh              # Run on default port 3000
#   ./dev.sh 3005         # Run on custom port

PORT="${1:-3000}"

echo "Starting Dagster dev server on port $PORT..."
echo "UI will be available at: http://127.0.0.1:$PORT"
echo ""

dagster dev -f dagster_elt_project/definitions.py -p "$PORT"
