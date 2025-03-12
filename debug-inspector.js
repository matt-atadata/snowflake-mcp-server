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
