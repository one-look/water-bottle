#!/bin/sh
# Railway deployment script
# Handles PORT environment variable properly

# Use Railway's PORT or fallback to 8080
PORT=${PORT:-8080}

echo "Starting server on port: $PORT"

# Start uvicorn with the correct port
exec uvicorn api.application:app --host 0.0.0.0 --port "$PORT"
