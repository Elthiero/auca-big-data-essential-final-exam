#!/bin/bash
# scripts/clean_checkpoints.sh
docker compose down
echo "Cleaning local Spark streaming checkpoints..."
docker run --rm -v $(pwd):/app alpine rm -rf /app/checkpoints/
echo "Done! Run 'docker compose up' to start fresh."