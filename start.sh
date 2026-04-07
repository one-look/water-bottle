# Railway deployment script

# Ensure PORT is treated as a number and has a fallback
R_PORT=${PORT:-8080}    #: - means fallback operator

# Set CONFIG environment variable to point to config.yaml in /app directory
export CONFIG=${CONFIG:-/app/config.yaml}   # export = make variable available to all processes

echo "Starting server on port: $R_PORT"
echo "Using config file: $CONFIG"
echo "Working directory: $(pwd)"
echo "Files in directory:"
ls -la      # l = long format; a = all files

# Use exec to make uvicorn main process (better for Docker)

exec uvicorn api.application:app --host 0.0.0.0 --port $R_PORT --workers 4  # exec = replace current process with new command