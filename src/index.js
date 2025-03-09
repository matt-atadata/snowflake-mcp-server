#!/usr/bin/env node

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { registerTools } from './tools.js';
import { registerResources } from './resources.js';
import { initializeSnowflakeConnection } from './snowflake.js';
import logger from './logger.js';

// Get the directory name of the current module
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load environment variables from .env file
try {
  const envPath = path.resolve(__dirname, '../.env');
  if (fs.existsSync(envPath)) {
    dotenv.config({ path: envPath });
    logger.info('Environment variables loaded from .env file');
  } else {
    logger.warn('No .env file found, using environment variables');
    dotenv.config();
  }
} catch (error) {
  logger.error('Error loading environment variables:', { error });
  process.exit(1);
}

// Create logs directory if it doesn't exist (for production environments)
if (process.env.NODE_ENV === 'production') {
  const logsDir = path.resolve(__dirname, '../logs');
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
    logger.info('Created logs directory');
  }
}

// Handle uncaught exceptions and unhandled rejections
process.on('uncaughtException', (error) => {
  logger.error('Uncaught exception:', { error });
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled rejection:', { reason, promise });
});

// Setup graceful shutdown
function handleShutdown() {
  logger.info('Shutting down Snowflake MCP Server...');
  // Close any open connections or resources here
  process.exit(0);
}

process.on('SIGINT', handleShutdown);
process.on('SIGTERM', handleShutdown);

async function main() {
  try {
    logger.info('Starting Snowflake MCP Server...');
    
    // Create MCP server with stdio transport (required for Windsurf)
    const server = new McpServer({
      name: process.env.SERVER_NAME || 'snowflake-mcp-server',
      version: process.env.SERVER_VERSION || '1.0.0'
    });

    // Initialize Snowflake connection
    logger.info('Initializing Snowflake connection...');
    const snowflakeConnection = await initializeSnowflakeConnection();
    logger.info('Snowflake connection established');
    
    // Register tools and resources
    logger.info('Registering tools and resources...');
    registerTools(server, snowflakeConnection);
    registerResources(server, snowflakeConnection);

    // Connect server to stdio transport
    logger.info('Connecting to stdio transport...');
    const transport = new StdioServerTransport();
    await server.connect(transport);
    
    // Log server start (SDK handles notifications differently)
    logger.info('Snowflake MCP Server started successfully');
    
    logger.info('Snowflake MCP Server started successfully');
  } catch (error) {
    logger.error('Failed to start Snowflake MCP Server:', { error });
    process.exit(1);
  }
}

main();
