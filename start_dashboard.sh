#!/bin/bash
# Contract Oversight System - Startup Script

echo "================================"
echo "Contract Oversight System"
echo "================================"
echo ""

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Check for virtual environment
VENV_PATH="$ROOT_DIR/venv"
if [ -d "$VENV_PATH" ]; then
    echo "Activating virtual environment..."
    source "$VENV_PATH/bin/activate"
else
    echo "No virtual environment found, using system Python"
fi

# Set PYTHONPATH
export PYTHONPATH="$SCRIPT_DIR"

# Change to web directory
cd "$SCRIPT_DIR/web"

echo ""
echo "Starting Contract Oversight Dashboard..."
echo "Dashboard will be available at: http://localhost:5002"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Run the Flask app
python app.py
