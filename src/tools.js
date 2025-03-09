import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/server';
import { executeQuery, executeWriteQuery, executeDDLQuery } from './snowflake.js';
import logger from './logger.js';

/**
 * Register all Snowflake tools with the MCP server
 * @param {Server} server - MCP server instance
 * @param {Object} connection - Snowflake connection object
 */
export function registerTools(server, connection) {
  // Define available tools
  server.setRequestHandler(ListToolsRequestSchema, async () => {
    return {
      tools: [
        // Query tools
        {
          name: "read_query",
          description: "Execute SELECT queries to read data from Snowflake",
          inputSchema: {
            type: "object",
            properties: {
              query: { 
                type: "string",
                description: "The SELECT SQL query to execute" 
              }
            },
            required: ["query"]
          }
        },
        {
          name: "write_query",
          description: "Execute INSERT, UPDATE, or DELETE queries in Snowflake",
          inputSchema: {
            type: "object",
            properties: {
              query: { 
                type: "string",
                description: "The SQL modification query" 
              }
            },
            required: ["query"]
          }
        },
        {
          name: "create_table",
          description: "Create new tables in Snowflake",
          inputSchema: {
            type: "object",
            properties: {
              query: { 
                type: "string",
                description: "CREATE TABLE SQL statement" 
              }
            },
            required: ["query"]
          }
        },
        
        // Schema tools
        {
          name: "list_databases",
          description: "Get a list of all accessible databases in Snowflake",
          inputSchema: {
            type: "object",
            properties: {}
          }
        },
        {
          name: "list_schemas",
          description: "Get a list of all schemas in the current or specified database",
          inputSchema: {
            type: "object",
            properties: {
              database: { 
                type: "string",
                description: "Optional database name (uses current database if not specified)" 
              }
            }
          }
        },
        {
          name: "list_tables",
          description: "Get a list of all tables in the current schema or specified schema",
          inputSchema: {
            type: "object",
            properties: {
              database: { 
                type: "string",
                description: "Optional database name (uses current database if not specified)" 
              },
              schema: { 
                type: "string",
                description: "Optional schema name (uses current schema if not specified)" 
              }
            }
          }
        },
        {
          name: "describe_table",
          description: "View column information for a specific table",
          inputSchema: {
            type: "object",
            properties: {
              table_name: { 
                type: "string",
                description: "Name of table to describe (can be fully qualified)" 
              }
            },
            required: ["table_name"]
          }
        },
        {
          name: "get_query_history",
          description: "Retrieve recent query history for the current user",
          inputSchema: {
            type: "object",
            properties: {
              limit: { 
                type: "number",
                description: "Maximum number of queries to return (default: 10)" 
              }
            }
          }
        },
        {
          name: "get_user_roles",
          description: "Get all roles assigned to the current user",
          inputSchema: {
            type: "object",
            properties: {}
          }
        },
        {
          name: "get_table_sample",
          description: "Get a sample of data from a table",
          inputSchema: {
            type: "object",
            properties: {
              table_name: { 
                type: "string",
                description: "Name of table to sample (can be fully qualified)" 
              },
              limit: { 
                type: "number",
                description: "Maximum number of rows to return (default: 10)" 
              }
            },
            required: ["table_name"]
          }
        }
      ]
    };
  });

  // Handle tool execution
  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;
    let result;

    logger.info('Tool execution request received', { tool: name, args });

    try {
      switch (name) {
        // Query tools
        case "read_query":
          if (!args.query || typeof args.query !== 'string') {
            throw new Error("Invalid query: must be a non-empty string");
          }
          if (!args.query.trim().toUpperCase().startsWith("SELECT")) {
            throw new Error("Only SELECT queries are allowed with read_query");
          }
          logger.debug('Executing read query', { query: args.query.substring(0, 100) + (args.query.length > 100 ? '...' : '') });
          result = await executeQuery(connection, args.query);
          logger.info('Read query executed successfully', { rowCount: result.length });
          break;
          
        case "write_query":
          if (!args.query || typeof args.query !== 'string') {
            throw new Error("Invalid query: must be a non-empty string");
          }
          if (args.query.trim().toUpperCase().startsWith("SELECT")) {
            throw new Error("Use read_query for SELECT queries");
          }
          if (args.query.trim().toUpperCase().startsWith("CREATE") || 
              args.query.trim().toUpperCase().startsWith("ALTER") || 
              args.query.trim().toUpperCase().startsWith("DROP")) {
            throw new Error("Use create_table for DDL operations");
          }
          logger.debug('Executing write query', { query: args.query.substring(0, 100) + (args.query.length > 100 ? '...' : '') });
          result = await executeWriteQuery(connection, args.query);
          logger.info('Write query executed successfully', { affectedRows: result.affected_rows });
          break;
          
        case "create_table":
          if (!args.query || typeof args.query !== 'string') {
            throw new Error("Invalid query: must be a non-empty string");
          }
          logger.debug('Executing DDL query', { query: args.query.substring(0, 100) + (args.query.length > 100 ? '...' : '') });
          result = await executeDDLQuery(connection, args.query);
          logger.info('DDL query executed successfully');
          break;
          
        // Schema tools
        case "list_databases":
          result = await executeQuery(connection, "SHOW DATABASES");
          break;
          
        case "list_schemas":
          let listSchemasQuery = "SHOW SCHEMAS";
          if (args.database) {
            listSchemasQuery = `SHOW SCHEMAS IN DATABASE ${args.database}`;
          }
          result = await executeQuery(connection, listSchemasQuery);
          break;
          
        case "list_tables":
          let listTablesQuery = "SHOW TABLES";
          if (args.database && args.schema) {
            listTablesQuery = `SHOW TABLES IN ${args.database}.${args.schema}`;
          } else if (args.schema) {
            listTablesQuery = `SHOW TABLES IN SCHEMA ${args.schema}`;
          } else if (args.database) {
            listTablesQuery = `SHOW TABLES IN DATABASE ${args.database}`;
          }
          result = await executeQuery(connection, listTablesQuery);
          break;
          
        case "describe_table":
          result = await executeQuery(connection, `DESCRIBE TABLE ${args.table_name}`);
          break;
          
        case "get_query_history":
          const limit = args.limit || 10;
          result = await executeQuery(connection, 
            `SELECT QUERY_ID, QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, 
                    EXECUTION_STATUS, START_TIME, END_TIME, TOTAL_ELAPSED_TIME 
             FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER()) 
             ORDER BY START_TIME DESC 
             LIMIT ${limit}`);
          break;
          
        case "get_user_roles":
          result = await executeQuery(connection, "SHOW ROLES");
          break;
          
        case "get_table_sample":
          const sampleLimit = args.limit || 10;
          result = await executeQuery(connection, 
            `SELECT * FROM ${args.table_name} LIMIT ${sampleLimit}`);
          break;
          
        default:
          throw new Error(`Tool not found: ${name}`);
      }

      // Format the result for better readability
      const formattedResult = formatToolResult(result);
      
      logger.info(`Tool ${name} executed successfully`);
      return {
        content: [
          {
            type: "text",
            text: formattedResult
          }
        ]
      };
    } catch (error) {
      logger.error(`Error executing tool ${name}:`, { error, args });
      
      // Provide a more helpful error message to the client
      const errorMessage = {
        error: error.message,
        tool: name,
        suggestion: getSuggestionForError(name, error)
      };
      
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(errorMessage, null, 2)
          }
        ],
        error: error.message
      };
    }
  });
}

/**
 * Format tool results for better readability
 * @param {any} result - The result to format
 * @returns {string} Formatted result as a string
 */
function formatToolResult(result) {
  if (!result) {
    return 'No results returned';
  }
  
  // Handle array results (typical for query results)
  if (Array.isArray(result)) {
    if (result.length === 0) {
      return 'Query executed successfully. No rows returned.';
    }
    
    // For small result sets, return the full JSON
    if (result.length <= 20) {
      return JSON.stringify(result, null, 2);
    }
    
    // For larger result sets, summarize and return a sample
    return `Query returned ${result.length} rows. Here's a sample of the first 10:\n\n${JSON.stringify(result.slice(0, 10), null, 2)}\n\n...and ${result.length - 10} more rows.`;
  }
  
  // Handle objects (typical for write/DDL operations)
  return JSON.stringify(result, null, 2);
}

/**
 * Get a helpful suggestion based on the error
 * @param {string} toolName - The name of the tool that was called
 * @param {Error} error - The error that occurred
 * @returns {string} A suggestion to help resolve the error
 */
function getSuggestionForError(toolName, error) {
  const errorMsg = error.message.toLowerCase();
  
  // Authentication errors
  if (errorMsg.includes('authentication') || errorMsg.includes('login') || errorMsg.includes('password')) {
    return 'Check your Snowflake credentials in the .env file';
  }
  
  // Syntax errors
  if (errorMsg.includes('syntax') || errorMsg.includes('sql')) {
    return 'There appears to be a syntax error in your SQL query. Please check the query syntax.';
  }
  
  // Object not found errors
  if (errorMsg.includes('does not exist') || errorMsg.includes('not found')) {
    return 'The database object (table, view, etc.) referenced in your query may not exist or you may not have access to it.';
  }
  
  // Permission errors
  if (errorMsg.includes('permission') || errorMsg.includes('privilege') || errorMsg.includes('access')) {
    return 'You may not have the necessary permissions to perform this operation.';
  }
  
  // Tool-specific suggestions
  switch (toolName) {
    case 'read_query':
      return 'Make sure you are using a SELECT query with the read_query tool.';
    case 'write_query':
      return 'Make sure you are using an INSERT, UPDATE, or DELETE query with the write_query tool.';
    case 'create_table':
      return 'Make sure you are using a CREATE TABLE statement with the create_table tool.';
    default:
      return 'Try checking the tool documentation for correct usage.';
  }
}
