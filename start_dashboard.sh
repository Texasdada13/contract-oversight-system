#!/bin/bash
# Contract Oversight System - Startup Script
# Starts both dashboards:
#   - Main Dashboard (port 5002)
#   - Surtax Oversight Dashboard (port 5847)

echo "========================================"
echo "Contract Oversight System"
echo "========================================"
echo "Starting both dashboards..."
echo ""

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Configuration
MAIN_PORT=5002
SURTAX_PORT=5847

# Check for virtual environment
VENV_PATH="$SCRIPT_DIR/venv"
if [ -d "$VENV_PATH" ]; then
    echo "Activating virtual environment..."
    source "$VENV_PATH/bin/activate"
else
    echo "No virtual environment found, using system Python"
fi

# Set PYTHONPATH
export PYTHONPATH="$SCRIPT_DIR"

echo ""
echo "========================================"
echo "Starting Both Dashboards..."
echo "========================================"
echo ""
echo "1. Main Contract Dashboard: http://localhost:$MAIN_PORT"
echo "2. Surtax Oversight Dashboard: http://localhost:$SURTAX_PORT"
echo ""
echo "Press Ctrl+C to stop the servers"
echo ""

# Start Surtax Dashboard in background
echo "Starting Surtax Oversight Dashboard (port $SURTAX_PORT)..."
cd "$SCRIPT_DIR/surtax_app"
python -c "from app import app; app.run(debug=False, host='127.0.0.1', port=$SURTAX_PORT)" &
SURTAX_PID=$!

# Wait a moment then open browsers
sleep 3
if command -v xdg-open &> /dev/null; then
    xdg-open "http://localhost:$MAIN_PORT" &
    sleep 1
    xdg-open "http://localhost:$SURTAX_PORT" &
elif command -v open &> /dev/null; then
    open "http://localhost:$MAIN_PORT" &
    sleep 1
    open "http://localhost:$SURTAX_PORT" &
fi

# Start Main Dashboard in foreground
echo "Starting Main Contract Dashboard (port $MAIN_PORT)..."
cd "$SCRIPT_DIR/web"
python -c "from app import app; app.run(debug=True, host='127.0.0.1', port=$MAIN_PORT)"

# Cleanup background process when main app exits
kill $SURTAX_PID 2>/dev/null
