#!/bin/bash
# Script to run the MCP Inspector with the simplified MCP server

# First, ensure ports 3000 and 5173 are available
echo "Checking for processes using MCP Inspector ports (3000 and 5173)..."

# Check port 3000
PORT_3000_PID=$(lsof -ti:3000)
if [ -n "$PORT_3000_PID" ]; then
    echo "Found process using port 3000: PID $PORT_3000_PID"
    echo "Killing process..."
    kill -9 $PORT_3000_PID
    echo "Process killed."
else
    echo "No process found using port 3000."
fi

# Check port 5173
PORT_5173_PID=$(lsof -ti:5173)
if [ -n "$PORT_5173_PID" ]; then
    echo "Found process using port 5173: PID $PORT_5173_PID"
    echo "Killing process..."
    kill -9 $PORT_5173_PID
    echo "Process killed."
else
    echo "No process found using port 5173."
fi

# Check port 8001 (MCP server port)
PORT_8001_PID=$(lsof -ti:8001)
if [ -n "$PORT_8001_PID" ]; then
    echo "Found process using port 8001: PID $PORT_8001_PID"
    echo "Killing process..."
    kill -9 $PORT_8001_PID
    echo "Process killed."
else
    echo "No process found using port 8001."
fi

# Set environment variable to use python instead of uv
export FASTMCP_PYTHON_COMMAND=python

# Run the inspector with our mcp_inspector.py file
echo "Starting MCP Inspector with mcp_inspector.py..."
cd "$(dirname "$0")"
source venv/bin/activate
fastmcp dev mcp_inspector.py
