#!/bin/bash
# Script to run the Snowflake MCP Server with the MCP Inspector

# Set colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Create logs directory if it doesn't exist
mkdir -p logs

# Kill any existing processes
echo -e "${YELLOW}Cleaning up existing processes...${NC}"

# Kill any existing MCP Inspector processes
if pgrep -f "mcp-inspector" > /dev/null; then
  echo -e "${YELLOW}Killing existing MCP Inspector processes...${NC}"
  pkill -f "mcp-inspector"
  sleep 1
fi

# Kill any existing Node.js processes related to our server
if pgrep -f "node.*index.js" > /dev/null; then
  echo -e "${YELLOW}Killing existing Node.js server processes...${NC}"
  pkill -f "node.*index.js"
  sleep 1
fi

# Check if port 5173 is in use
echo -e "${YELLOW}Checking if port 5173 is in use...${NC}"
if lsof -i :5173 > /dev/null; then
  echo -e "${RED}Port 5173 is already in use. Attempting to free it...${NC}"
  lsof -i :5173 -t | xargs kill -9 2>/dev/null || true
  sleep 1
  
  # Check again to make sure the port is free
  if lsof -i :5173 > /dev/null; then
    echo -e "${RED}Failed to free port 5173. Please close the application using this port and try again.${NC}"
    exit 1
  fi
fi

# Also check if port 3000 is in use (used by the MCP server)
echo -e "${YELLOW}Checking if port 3000 is in use...${NC}"
if lsof -i :3000 > /dev/null; then
  echo -e "${RED}Port 3000 is already in use. Attempting to free it...${NC}"
  lsof -i :3000 -t | xargs kill -9 2>/dev/null || true
  sleep 1
  
  # Check again to make sure the port is free
  if lsof -i :3000 > /dev/null; then
    echo -e "${RED}Failed to free port 3000. Please close the application using this port and try again.${NC}"
    exit 1
  fi
fi

echo -e "${GREEN}Ports 5173 and 3000 are free. Starting MCP Inspector...${NC}"

# Clear all previous log files
echo -e "${YELLOW}Clearing previous log files...${NC}"
rm -f logs/inspector-*.log
rm -f logs/server-*.log

# Create empty log files with proper permissions
touch logs/inspector-console.log
touch logs/inspector-debug.log
touch logs/inspector-filtered.log
touch logs/inspector-stderr.log
touch logs/server-stdio.log

# Make sure the log files are writable
chmod 666 logs/inspector-*.log logs/server-*.log

# Create a debug flag file to enable more verbose logging
echo "true" > logs/debug-enabled

# Create a debug script to help with troubleshooting
cat > debug-inspector.js << 'EOF'
#!/usr/bin/env node

/**
 * Debug script to help troubleshoot MCP Inspector issues
 * This script logs information about the MCP SDK and resource providers
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Create logs directory if it doesn't exist
const logsDir = path.join(__dirname, 'logs');
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}

// Log file path
const logFile = path.join(logsDir, 'debug-inspector.log');

// Helper function to log messages
function log(message) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}\n`;
  
  // Log to console and file
  console.log(logMessage);
  fs.appendFileSync(logFile, logMessage);
}

// Clear previous log file
fs.writeFileSync(logFile, '');

log('Starting MCP Inspector debug script');

// Check SDK version
try {
  const sdkPackageJsonPath = path.join(__dirname, 'node_modules', '@modelcontextprotocol', 'sdk', 'package.json');
  if (fs.existsSync(sdkPackageJsonPath)) {
    const sdkPackageJson = JSON.parse(fs.readFileSync(sdkPackageJsonPath, 'utf8'));
    log(`MCP SDK version: ${sdkPackageJson.version}`);
    
    // Log SDK structure
    const sdkDir = path.join(__dirname, 'node_modules', '@modelcontextprotocol', 'sdk');
    const sdkFiles = fs.readdirSync(sdkDir);
    log(`SDK directory contents: ${JSON.stringify(sdkFiles)}`);
    
    // Check for dist directory
    const distDir = path.join(sdkDir, 'dist');
    if (fs.existsSync(distDir)) {
      const distFiles = fs.readdirSync(distDir);
      log(`SDK dist directory contents: ${JSON.stringify(distFiles)}`);
      
      // Check for ESM directory
      const esmDir = path.join(distDir, 'esm');
      if (fs.existsSync(esmDir)) {
        const esmFiles = fs.readdirSync(esmDir);
        log(`SDK ESM directory contents: ${JSON.stringify(esmFiles)}`);
        
        // Check for server directory
        const serverDir = path.join(esmDir, 'server');
        if (fs.existsSync(serverDir)) {
          const serverFiles = fs.readdirSync(serverDir);
          log(`SDK server directory contents: ${JSON.stringify(serverFiles)}`);
        } else {
          log('SDK server directory not found in ESM');
        }
      } else {
        log('SDK ESM directory not found');
      }
    } else {
      log('SDK dist directory not found');
    }
  } else {
    log('MCP SDK package.json not found');
  }
} catch (error) {
  log(`Error checking SDK version: ${error.message}`);
}

// Check inspector version
try {
  const inspectorPackageJsonPath = path.join(__dirname, 'node_modules', '@modelcontextprotocol', 'inspector', 'package.json');
  if (fs.existsSync(inspectorPackageJsonPath)) {
    const inspectorPackageJson = JSON.parse(fs.readFileSync(inspectorPackageJsonPath, 'utf8'));
    log(`MCP Inspector version: ${inspectorPackageJson.version}`);
  } else {
    log('MCP Inspector package.json not found');
  }
} catch (error) {
  log(`Error checking Inspector version: ${error.message}`);
}

log('MCP Inspector debug script completed');
EOF

# Make the debug script executable
chmod +x debug-inspector.js

# Run the debug script to gather information
echo -e "${YELLOW}Running debug script to gather information...${NC}"
node debug-inspector.js

# Run the MCP Inspector with the correct command format
echo -e "${YELLOW}Starting MCP Inspector with Snowflake MCP Server...${NC}"

# The MCP Inspector should start the server, not the other way around
# The correct format is: npx @modelcontextprotocol/inspector <server-path> --stdio
echo -e "${YELLOW}Running: NODE_ENV=production npx @modelcontextprotocol/inspector ./src/index.js --stdio${NC}"

# Run with debug enabled to see more information
# We'll use a custom environment variable to control debug output
# Add NODE_OPTIONS to enable more verbose debugging
NODE_OPTIONS="--trace-warnings" DEBUG_MCP=true NODE_ENV=production npx @modelcontextprotocol/inspector ./src/index.js --stdio 2> logs/inspector-stderr.log

# Note: This script will keep running until you close the inspector
echo -e "${GREEN}MCP Inspector has been started.${NC}"
echo -e "${GREEN}Check logs/inspector-debug.log for details.${NC}"
echo -e "${GREEN}To view logs in real-time, run: tail -f logs/inspector-debug.log${NC}"
echo -e "${GREEN}To view filtered JSON-RPC messages, run: tail -f logs/inspector-filtered.log${NC}"

# If we get here, the inspector has been closed
echo -e "${YELLOW}MCP Inspector has been closed.${NC}"
