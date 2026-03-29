#!/bin/bash
# Расположение: ~/run_pipeline.sh
# Добавить в cron: 0 6 * * * /home/USER/run_pipeline.sh >> /home/USER/logs/pipeline.log 2>&1

set -e

COMPOSE_DIR="/home/$(whoami)/weather-pipeline"
LOG_DIR="/home/$(whoami)/logs"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

mkdir -p "$LOG_DIR"

echo "[$TIMESTAMP] Starting pipeline run..."
cd "$COMPOSE_DIR"

docker compose run --rm bruin bruin run /app

echo "[$TIMESTAMP] Pipeline finished successfully."
