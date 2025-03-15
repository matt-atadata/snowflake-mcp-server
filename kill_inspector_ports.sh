#!/bin/bash
# Script to find and kill processes using the MCP Inspector ports

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

echo "Done. You can now try running the MCP Inspector again."
