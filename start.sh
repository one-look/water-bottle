#!/bin/sh
# Railway deployment script

# Ensure PORT is treated as a number and has a fallback
R_PORT=${PORT:-8080}

echo "Starting server on port: $R_PORT"

# Use exec to make uvicorn the main process (better for Docker)
# Remove the quotes from $R_PORT to ensure it is passed as a literal number
exec uvicorn api.application:app --host 0.0.0.0 --port $R_PORT