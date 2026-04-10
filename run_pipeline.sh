#!/bin/bash
# path: ~/run_pipeline.sh
# Add to cron: 0 6 * * * /home/USER/run_pipeline.sh >> /home/USER/logs/pipeline.log 2>&1

set -e

COMPOSE_DIR="/home/$(whoami)/zoomcamp/project"
LOG_DIR="/home/$(whoami)/zoomcamp/project/logs"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

mkdir -p "$LOG_DIR"

echo "[$TIMESTAMP] Starting pipeline run..."
cd "$COMPOSE_DIR"

docker exec de_bruin bruin run /app

echo "[$TIMESTAMP] Pipeline finished successfully."
