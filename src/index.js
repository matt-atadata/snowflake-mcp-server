#!/usr/bin/env node

import { Server, StdioServerTransport } from '@modelcontextprotocol/server';
import dotenv from 'dotenv';
import { registerTools } from './tools.js';
import { registerResources } from './resources.js';
import { initializeSnowflakeConnection } from './snowflake.js';

// Load environment variables
dotenv.config();

async function main() {
  try {
    console.error('Starting Snowflake MCP Server...');
    
    // Create MCP server with stdio transport (required for Windsurf)
    const server = new Server({
      name: process.env.SERVER_NAME || 'snowflake-mcp-server',
      version: process.env.SERVER_VERSION || '1.0.0'
    }, {
      capabilities: {
        tools: {},
        resources: {}
      }
    });

    // Initialize Snowflake connection
    const snowflakeConnection = await initializeSnowflakeConnection();
    
    // Register tools and resources
    registerTools(server, snowflakeConnection);
    registerResources(server, snowflakeConnection);

    // Connect server to stdio transport
    const transport = new StdioServerTransport();
    await server.connect(transport);
    
    console.error('Snowflake MCP Server started successfully');
  } catch (error) {
    console.error('Failed to start Snowflake MCP Server:', error);
    process.exit(1);
  }
}

main();
