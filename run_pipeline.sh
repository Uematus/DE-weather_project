#!/bin/bash
# Wrapper around `bruin run /app` for cron.
#
# Before using this script on your own host, edit the two paths below
# (COMPOSE_DIR, LOG_DIR) to match where you cloned the repo.
#
# Example cron entry (runs daily at 08:00 — matches pipeline.yml schedule):
#   0 8 * * * /home/USER/run_pipeline.sh >> /home/USER/logs/pipeline.log 2>&1

set -e

# --- edit these two paths for your host ---
COMPOSE_DIR="/home/$(whoami)/zoomcamp/project"
LOG_DIR="$COMPOSE_DIR/logs"
# ------------------------------------------

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

mkdir -p "$LOG_DIR"

echo "[$TIMESTAMP] Starting pipeline run..."
cd "$COMPOSE_DIR"

docker exec de_bruin bruin run /app

echo "[$TIMESTAMP] Pipeline finished successfully."
