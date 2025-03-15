#!/usr/bin/env python3
"""
Simple Model Context Protocol (MCP) Server

A minimal MCP server using the FastMCP library.
"""
import os
import sys
import logging
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("mcp_server")

# Parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Simple MCP Server")
    parser.add_argument("--port", type=int, default=8001, help="Port to run the MCP server on (default: 8001)")
    parser.add_argument("--allow-write", action="store_true", help="Allow write operations")
    return parser.parse_args()

# Parse arguments early
args = parse_args()

# Set PORT environment variable BEFORE importing FastMCP
os.environ["PORT"] = str(args.port)

# Import FastMCP after setting environment variables
from fastmcp import FastMCP  # noqa: E402

def main():
    # Create a simple MCP server
    server = FastMCP("Simple MCP Server", allow_write=args.allow_write)
    
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
            "port": args.port,
            "allow_write": args.allow_write
        }
    
    # Run the server
    try:
        logger.info(f"Running MCP server on port {args.port}")
        logger.info("Use 'mcp dev test.py' to connect with the MCP Inspector")
        server.run()
    except Exception as e:
        logger.error(f"Error running server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()