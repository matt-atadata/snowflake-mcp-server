#!/usr/bin/env node

// Suppress all deprecation warnings (including punycode)
process.noDeprecation = true;

// Suppress specific warnings for punycode
process.on('warning', (warning) => {
  // Ignore punycode deprecation warnings
  if (warning.name === 'DeprecationWarning' && 
      warning.message && 
      warning.message.includes('punycode')) {
    // Silently ignore this warning
    return;
  }
  
  // Log other warnings
  console.warn(warning.name, warning.message);
});

// Import fs module since we might need it immediately
import fs from 'fs';

// Check if running with MCP inspector (stdio mode) - this must be the very first check
const isInspectorMode = process.argv.includes('--stdio');

// Check if debug mode is enabled via environment variable
const isDebugMode = process.env.DEBUG_MCP === 'true';

// Helper function to log to debug file
function logToDebugFile(message) {
  if (isInspectorMode) {
    try {
      fs.appendFileSync('logs/inspector-debug.log', `[${new Date().toISOString()}] ${message}\n`);
    } catch (e) {
      // Silently fail if we can't log
    }
  }
}

// CRITICAL: In inspector mode, immediately override stdout/stderr to prevent any output
// that could interfere with JSON-RPC communication
if (isInspectorMode) {
  // Create logs directory for debug logs
  try {
    const logsDir = 'logs';
    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }
    
    // Clear debug log
    fs.writeFileSync('logs/inspector-debug.log', `[${new Date().toISOString()}] Starting server in inspector mode\n`);
    
    // Override console.log and other methods in inspector mode to prevent stdout corruption
    const originalConsoleLog = console.log;
    const originalConsoleError = console.error;
    const originalConsoleWarn = console.warn;
    const originalConsoleInfo = console.info;
    
    console.log = function(...args) {
      try {
        const message = args.map(arg => 
          typeof arg === 'object' ? JSON.stringify(arg) : String(arg)
        ).join(' ');
        fs.appendFileSync('logs/inspector-debug.log', `[${new Date().toISOString()}] [LOG] ${message}\n`);
      } catch (err) {
        // Silently fail to avoid triggering more issues
      }
    };
    
    console.error = function(...args) {
      try {
        const message = args.map(arg => 
          typeof arg === 'object' ? JSON.stringify(arg) : String(arg)
        ).join(' ');
        fs.appendFileSync('logs/inspector-debug.log', `[${new Date().toISOString()}] [ERROR] ${message}\n`);
      } catch (err) {
        // Silently fail
      }
    };
    
    console.warn = function(...args) {
      try {
        const message = args.map(arg => 
          typeof arg === 'object' ? JSON.stringify(arg) : String(arg)
        ).join(' ');
        fs.appendFileSync('logs/inspector-debug.log', `[${new Date().toISOString()}] [WARN] ${message}\n`);
      } catch (e) {
        // Silently fail
      }
    };
    
    console.info = function(...args) {
      try {
        const message = args.map(arg => 
          typeof arg === 'object' ? JSON.stringify(arg) : String(arg)
        ).join(' ');
        fs.appendFileSync('logs/inspector-debug.log', `[${new Date().toISOString()}] [INFO] ${message}\n`);
      } catch (e) {
        // Silently fail
      }
    };
    
    // Redirect stdout and stderr
    const originalStdoutWrite = process.stdout.write;
    const originalStderrWrite = process.stderr.write;
    
    process.stdout.write = function(chunk) {
      try {
        if (typeof chunk === 'string' && !chunk.includes('{"jsonrpc":')) {
          fs.appendFileSync('logs/inspector-debug.log', `[${new Date().toISOString()}] [STDOUT] ${chunk}\n`);
        }
        return originalStdoutWrite.apply(process.stdout, arguments);
      } catch (e) {
        return originalStdoutWrite.apply(process.stdout, arguments);
      }
    };
    
    process.stderr.write = function(chunk) {
      try {
        fs.appendFileSync('logs/inspector-debug.log', `[${new Date().toISOString()}] [STDERR] ${chunk}\n`);
        // In inspector mode, we don't write to stderr to avoid corrupting the JSON-RPC stream
        return true;
      } catch (e) {
        return false;
      }
    };
  } catch (error) {
    // Still need to silence errors here, use original console
    // We can't use the file logger at this point
  }
}

// Import necessary modules for the MCP server
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import dotenv from 'dotenv';
import { registerTools } from './tools.js';
import { registerResources } from './resources.js';
import { initializeSnowflakeConnection } from './snowflake.js';

// Import logger after console redirection
import logger from './logger.js';

// Load environment variables from .env file
dotenv.config();

// Handle graceful shutdown
function handleShutdown() {
  if (isInspectorMode && logger.logToFile) {
    logger.logToFile('info', 'Shutting down Snowflake MCP Server...');
  } else {
    logger.info('Shutting down Snowflake MCP Server...');
  }
  // Close any open connections or resources here
  process.exit(0);
}

process.on('SIGINT', handleShutdown);
process.on('SIGTERM', handleShutdown);

/**
 * Main function to run the MCP server
 */
async function main() {
  try {
    if (isInspectorMode && logger.logToFile) {
      logger.logToFile('info', 'Starting server in inspector mode');
    }

    // Create MCP server with modern API structure
    const server = new Server(
      {
        name: process.env.SERVER_NAME || 'snowflake-mcp-server',
        version: process.env.SERVER_VERSION || '1.0.0',
      },
      {
        capabilities: {
          resources: {
            // Enable resources capability
            list: true,
            get: true
          },
          tools: {
            // Enable tools capability
            list: true,
            call: true
          },
          metadata: {
            title: process.env.SERVER_TITLE || 'Snowflake MCP Server',
            description: process.env.SERVER_DESCRIPTION || 'MCP server for accessing Snowflake resources and tools',
            human_description: process.env.SERVER_HUMAN_DESCRIPTION || 'This MCP server provides access to Snowflake databases, schemas, tables, and allows executing queries.',
            vendor: {
              name: process.env.VENDOR_NAME || 'Snowflake',
              website_url: process.env.VENDOR_URL || 'https://www.snowflake.com/',
            },
            connection_status: {
              status: 'pending', // Will be updated after connection attempt
              message: 'Initializing connection to Snowflake...'
            },
          },
        },
        debug: isDebugMode,
      }
    );
    
    // Initialize Snowflake connection first
    const snowflakeConnection = await initializeSnowflakeConnection(server);
    
    // Set up required handlers for MCP Inspector compatibility
    
    // Register request handlers for the methods that the MCP Inspector is trying to call
    
    // Handler for resources/list
    server.setRequestHandler({ method: 'resources/list' }, async (request) => {
      logger.info('Method called: resources/list');
      try {
        const resources = await getResourcesList(snowflakeConnection);
        return { resources };
      } catch (error) {
        logger.error('Error in resources/list method', { error: error.message });
        throw error;
      }
    });
    
    // Handler for resources/get
    server.setRequestHandler({ method: 'resources/get' }, async (request) => {
      const { uri } = request.params;
      logger.info(`Method called: resources/get with URI: ${uri}`);
      try {
        const resource = await getResourceByUri(uri, snowflakeConnection);
        return { resource };
      } catch (error) {
        logger.error(`Error in resources/get method for URI: ${uri}`, { error: error.message });
        throw error;
      }
    });
    
    // Handler for tools/list
    server.setRequestHandler({ method: 'tools/list' }, async (request) => {
      logger.info('Method called: tools/list');
      try {
        const tools = Object.entries(server.capabilities.tools || {}).map(([name, tool]) => ({
          name,
          description: tool.description,
          schema: tool.schema
        }));
        return { tools };
      } catch (error) {
        logger.error('Error in tools/list method', { error: error.message });
        throw error;
      }
    });
    
    // Handler for resources/templates/list
    server.setRequestHandler({ method: 'resources/templates/list' }, async (request) => {
      logger.info('Method called: resources/templates/list');
      try {
        // For now, we don't have any templates
        return { templates: [] };
      } catch (error) {
        logger.error('Error in resources/templates/list method', { error: error.message });
        throw error;
      }
    });
    
    if (!snowflakeConnection) {
      throw new Error('Failed to initialize Snowflake connection');
    }
    
    // Update connection status in metadata if connection successful
    if (server.updateMetadata) {
      server.updateMetadata({
        'connection_status.status': 'connected',
        'connection_status.message': 'Successfully connected to Snowflake'
      });
      logToDebugFile('Updated metadata with connection status');
    }
    
    // Log MCP requests (modern SDK doesn't support middleware, so we'll log in handlers instead)
    logToDebugFile('Modern MCP SDK initialized - request logging will happen in individual handlers');
    logger.info('MCP Server successfully initialized, registering handlers...');
    
    // Add a custom method to the server to handle resource registration
    // First check if the method exists in the MCP SDK version
    if (!server.registerResourceProvider) {
      // Define our own registerResourceProvider method if it doesn't exist
      server.registerResourceProvider = function(provider) {
        logToDebugFile(`Registering resource provider: ${JSON.stringify({
          name: provider.name,
          uriTemplate: provider.uriTemplate,
          hasList: typeof provider.list === 'function',
          hasGet: typeof provider.get === 'function'
        })}`);
      
        try {
          // Ensure the provider has the required methods
          if (!provider.list || typeof provider.list !== 'function') {
            logToDebugFile(`WARNING: Resource provider ${provider.name} is missing list method`);
          }
          
          if (!provider.get || typeof provider.get !== 'function') {
            logToDebugFile(`WARNING: Resource provider ${provider.name} is missing get method`);
          }
        
          // Register the provider using the server's resource provider method
          // In newer MCP SDK versions, we might use server.resource(provider.name, provider)
          server.resource(provider.name, provider);
        } catch (error) {
          logToDebugFile(`Error registering resource provider ${provider.name}: ${error.message}\n${error.stack}`);
          throw error;
        }
      };
    }
    
    // Register tools first with better error handling
    try {
      // Register tools with the server
      await registerTools(server, snowflakeConnection);
      
      if (isInspectorMode && logger.logToFile) {
        logger.logToFile('info', 'Successfully registered all tools');
      } else {
        logger.info('Successfully registered all tools');
      }
    } catch (toolError) {
      if (isInspectorMode && logger.logToFile) {
        logger.logToFile('error', 'Failed to register tools:', { error: toolError.message, stack: toolError.stack });
      } else {
        logger.error('Failed to register tools:', { error: toolError });
      }
      throw toolError;
    }

    // Register resources with better error handling
    try {
      registerResources(server, snowflakeConnection);
      if (isInspectorMode && logger.logToFile) {
        logger.logToFile('info', 'Successfully registered all resource providers');
      } else {
        logger.info('Successfully registered all resource providers');
      }
    } catch (resourceError) {
      if (isInspectorMode && logger.logToFile) {
        logger.logToFile('error', 'Failed to register resources:', { error: resourceError.message, stack: resourceError.stack });
      } else {
        logger.error('Failed to register resources:', { error: resourceError });
      }
      // Try to register the actual resources, but catch any errors
      try {
        await registerResources(server, snowflakeConnection);
      } catch (resourceError) {
        logger.error('Failed to register resource providers:', { error: resourceError });
      }
    }

    // Set up transport based on the execution mode
    const transport = new StdioServerTransport();
    
    // Log connection attempt
    if (isInspectorMode && logger.logToFile) {
      logger.logToFile('info', 'Connecting to stdio transport');
    } else {
      logger.info('Connecting to stdio transport');
    }
    
    // Set up required handlers for MCP Inspector compatibility
    // Note: We already have these handlers defined below, so we don't need to define them here
    
    // Connect to the transport
    await server.connect(transport);
    
    // Log successful connection
    if (isInspectorMode && logger.logToFile) {
      logger.logToFile('info', 'Successfully connected to stdio transport');
    } else {
      logger.info('Successfully connected to stdio transport');
    }
    
    // Log server start
    if (isInspectorMode && logger.logToFile) {
      logger.logToFile('info', 'Snowflake MCP Server started successfully');
    } else {
      logger.info('Snowflake MCP Server started successfully');
    }
  } catch (error) {
    // Log any error during startup
    if (isInspectorMode && logger.logToFile) {
      logger.logToFile('error', 'Failed to start Snowflake MCP Server:', { error });
    } else {
      logger.error('Failed to start Snowflake MCP Server:', { error });
    }
    throw error;
  }
}

/**
 * Get a list of all available resources
 * @param {Object} connection - Snowflake connection
 * @returns {Promise<Array>} List of resources
 */
async function getResourcesList(connection) {
  logger.info('Getting resources list');
  
  // Build a list of all available resources
  const resources = [];
  
  // Resource: Insights memo
  resources.push({
    uri: 'memo://insights',
    title: 'Insights Memo',
    description: 'Collection of insights and observations',
  });
  
  // Resource: Snowflake databases metadata
  resources.push({
    uri: 'snowflake://metadata/databases',
    title: 'Snowflake Databases',
    description: 'List of available Snowflake databases',
  });
  
  // Resource: Snowflake schemas metadata
  resources.push({
    uri: 'snowflake://metadata/schemas',
    title: 'Snowflake Schemas',
    description: 'List of available Snowflake schemas',
  });
  
  // Resource: Snowflake tables metadata
  resources.push({
    uri: 'snowflake://metadata/tables',
    title: 'Snowflake Tables',
    description: 'List of available Snowflake tables',
  });
  
  // Resource: Snowflake user info
  resources.push({
    uri: 'snowflake://metadata/user',
    title: 'Snowflake User Info',
    description: 'Information about the current Snowflake user',
  });
  
  logger.info(`Prepared ${resources.length} Snowflake resources`);
  return resources;
}

/**
 * Get resources by URI
 * @param {string} uri - Resource URI to fetch
 * @param {Object} connection - Snowflake connection
 * @returns {Promise<Object>} Resource data
 */
async function getResourceByUri(uri, connection) {
  logger.info(`Getting resource for URI: ${uri}`);
  try {
    // Parse the URI to determine what resource to return
    const parsedUri = new URL(uri);
    const pathParts = parsedUri.pathname.split('/');
    
    // Map of URIs to resource content generators
    const resourceHandlers = {
      'snowflake://metadata/databases': async () => {
        // In a real implementation, this would query Snowflake for databases
        return {
          uri,
          title: 'Snowflake Databases',
          type: 'database_list',
          content: {
            databases: [
              { name: 'DEMO_DB', created: '2025-01-15T00:00:00Z' },
              { name: 'SNOWFLAKE_SAMPLE_DATA', created: '2025-01-01T00:00:00Z' },
              { name: 'ANALYTICS', created: '2025-02-01T00:00:00Z' }
            ]
          }
        };
      },
      'snowflake://metadata/schemas': async () => ({
        uri,
        title: 'Snowflake Schemas',
        type: 'schema_list',
        content: {
          schemas: [
            { database: 'DEMO_DB', name: 'PUBLIC', created: '2025-01-15T00:00:00Z' },
            { database: 'SNOWFLAKE_SAMPLE_DATA', name: 'TPCH_SF1', created: '2025-01-01T00:00:00Z' },
            { database: 'ANALYTICS', name: 'REPORTING', created: '2025-02-01T00:00:00Z' }
          ]
        }
      }),
      'snowflake://metadata/tables': async () => ({
        uri,
        title: 'Snowflake Tables',
        type: 'table_list',
        content: {
          tables: [
            { database: 'DEMO_DB', schema: 'PUBLIC', name: 'CUSTOMERS', rows: 1000, created: '2025-01-15T00:00:00Z' },
            { database: 'DEMO_DB', schema: 'PUBLIC', name: 'ORDERS', rows: 5000, created: '2025-01-15T00:00:00Z' },
            { database: 'ANALYTICS', schema: 'REPORTING', name: 'DAILY_SALES', rows: 365, created: '2025-02-01T00:00:00Z' }
          ]
        }
      }),
      'snowflake://metadata/user': async () => ({
        uri,
        title: 'Snowflake User Info',
        type: 'user_info',
        content: {
          user: {
            name: 'DEMO_USER',
            role: 'ACCOUNTADMIN',
            created: '2025-01-01T00:00:00Z',
            last_login: '2025-03-11T00:00:00Z'
          }
        }
      })
    };

    // Check if we have a handler for this URI
    if (resourceHandlers[uri]) {
      const resource = await resourceHandlers[uri]();
      logger.info(`Successfully retrieved resource for URI: ${uri}`);
      return resource;
    }

    // Handle specific database, schema, or table requests
    if (parsedUri.protocol === 'snowflake:' && pathParts[1] === 'metadata') {
      if (pathParts[2] === 'databases' && pathParts.length > 3) {
        // Get specific database info
        const dbName = pathParts[3];
        return {
          uri,
          title: `Database: ${dbName}`,
          type: 'database',
          content: {
            name: dbName,
            created: '2025-01-15T00:00:00Z',
            owner: 'ACCOUNTADMIN',
            schemas: ['PUBLIC', 'INFORMATION_SCHEMA']
          }
        };
      }
    }

    // If we don't have a specific handler, return a generic error resource
    logger.warn(`No handler found for URI: ${uri}`);
    return {
      uri,
      title: 'Resource Not Found',
      type: 'error',
      content: {
        error: `No resource found for URI: ${uri}`
      }
    };
  } catch (error) {
    logger.error(`Error getting resource for URI: ${uri}`, { error: error.message });
    throw error;
  }
}

/**
 * Get list of available tools
 * @param {Object} connection - Snowflake connection
 * @returns {Array} List of tools
 */
function getToolsList(connection) {
  return [
    {
      name: 'query',
      description: 'Run a SQL query on Snowflake',
      inputSchema: {
        type: 'object',
        properties: {
          sql: { type: 'string' },
          timeout: { type: 'number' }
        },
        required: ['sql']
      }
    },
    // Add other tools as needed
  ];
}

/**
 * Execute a tool call
 * @param {string} toolName - Name of the tool to call
 * @param {Object} args - Tool arguments
 * @param {Object} connection - Snowflake connection
 * @returns {Promise<Object>} Tool response
 */
async function executeToolCall(toolName, args, connection) {
  if (toolName === 'query') {
    try {
      // Execute the query against Snowflake
      const result = await executeQuery(connection, args.sql);
      return {
        content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
        isError: false
      };
    } catch (error) {
      return {
        content: [{ type: 'text', text: `Error executing query: ${error.message}` }],
        isError: true
      };
    }
  }
  
  throw new Error(`Unknown tool: ${toolName}`);
}

// Run the server
main().catch(error => {
  console.error('Failed to start Snowflake MCP Server:', error);
  process.exit(1);
});
