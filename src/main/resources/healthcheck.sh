#!/bin/bash
set -e

# Use configured port or default to 50051
PORT=${DAS_SERVER_PORT:-50051}

# Check if the process is listening on the configured port
if nc -z localhost $PORT; then
  echo "Health check passed: service listening on port $PORT"
  exit 0
else
  echo "Health check failed: service not listening on port $PORT"
  exit 1
fi
