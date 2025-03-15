#!/bin/bash
# Script to run the Snowflake MCP Inspector with the Snowflake MCP Server

# Check for processes using MCP Inspector ports (3000 and 5173)
echo "Checking for processes using MCP Inspector ports (3000 and 5173)..."
PORT_3000_PID=$(lsof -ti:3000)
PORT_5173_PID=$(lsof -ti:5173)
PORT_8001_PID=$(lsof -ti:8001)

# Kill processes if they exist
if [ ! -z "$PORT_3000_PID" ]; then
    echo "Killing process using port 3000..."
    kill -9 $PORT_3000_PID
else
    echo "No process found using port 3000."
fi

if [ ! -z "$PORT_5173_PID" ]; then
    echo "Killing process using port 5173..."
    kill -9 $PORT_5173_PID
else
    echo "No process found using port 5173."
fi

if [ ! -z "$PORT_8001_PID" ]; then
    echo "Killing process using port 8001..."
    kill -9 $PORT_8001_PID
else
    echo "No process found using port 8001."
fi

# Start MCP Inspector with our Snowflake MCP Server
echo "Starting MCP Inspector with snowflake_mcp_server.py..."
echo "Starting MCP inspector..."

# Set the Python command for FastMCP to avoid uv errors
export FASTMCP_PYTHON_COMMAND=python

# Run the MCP Inspector with our Snowflake MCP Server
fastmcp dev snowflake_mcp_server.py

# Note: The script will continue running until you press Ctrl+C to stop it
