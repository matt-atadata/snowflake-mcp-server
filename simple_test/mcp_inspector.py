#!/usr/bin/env python3
"""
Entry point for the Simple MCP Server Inspector

This script exposes the Simple MCP server for use with the FastMCP Inspector.

Usage:
    1. Run with environment variable to avoid uv errors:
       FASTMCP_PYTHON_COMMAND=python fastmcp dev mcp_inspector.py
       
    2. Or use the convenience script:
       ./run_inspector.sh
"""

# Import necessary modules
import os
import logging
from fastmcp import FastMCP

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("mcp_inspector")

# Set PORT environment variable for the MCP server
os.environ["PORT"] = "8001"


# Create a simple MCP server
server = FastMCP("Simple MCP Server", allow_write=True)

# Define a simple echo tool
@server.tool("echo")
def echo(message: str) -> str:
    """Echo back the message sent to the server"""
    return f"Server received: {message}"

# Define a server info tool
@server.tool("server_info")
def server_info() -> dict:
    """Get information about the server"""
    return {
        "server_name": "Simple MCP Server",
        "port": 8001,
        "allow_write": True
    }

# If you run this file directly, print helpful information
if __name__ == "__main__":
    print("This file is intended to be used with the FastMCP CLI:")
    print("  FASTMCP_PYTHON_COMMAND=python fastmcp dev mcp_inspector.py")
    print("\nOr use the convenience script:")
    print("  ./run_inspector.sh")
