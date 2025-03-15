#!/bin/bash
# Script to launch the MCP Inspector to connect to the running MCP server

# Ensure the MCP Inspector ports are available
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

# Launch the MCP Inspector
echo "Launching MCP Inspector..."
cd "$(dirname "$0")"
npx @modelcontextprotocol/inspector http://localhost:8001
