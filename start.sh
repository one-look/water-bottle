#!/bin/sh
# Railway deployment script

# Ensure PORT is treated as a number and has a fallback
R_PORT=${PORT:-8080}

# Set CONFIG environment variable to point to config.yaml in /app directory
export CONFIG=${CONFIG:-/app/config.yaml}

echo "Starting server on port: $R_PORT"
echo "Using config file: $CONFIG"
echo "Working directory: $(pwd)"
echo "Files in directory:"
ls -la

# Use exec to make uvicorn main process (better for Docker)

exec uvicorn api.application:app --host 0.0.0.0 --port $R_PORT --workers 4