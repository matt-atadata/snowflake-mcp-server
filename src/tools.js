import { z } from 'zod';
import { executeQuery, executeWriteQuery, executeDDLQuery } from './snowflake.js';
import { zodToJsonSchema } from 'zod-to-json-schema';
import logger from './logger.js';

// Define content types according to MCP specification
const CONTENT_TYPE_TEXT = 'text/plain';
const CONTENT_TYPE_JSON = 'application/json';

/**
 * Configure Snowflake tools and prepare handlers
 * @param {Object} connection - Snowflake connection object
 * @returns {Object} Object containing tools and handleToolCall function
 */
async function setupSnowflakeTools(connection) {
  logger.info('Setting up Snowflake tools');

  // Validate connection
  if (!connection) {
    logger.error('Invalid Snowflake connection provided');
    throw new Error('Invalid Snowflake connection');
  }

  // Define tool schemas using zod
  const ReadQuerySchema = z.object({
    query: z.string().describe('The SELECT SQL query to execute'),
  });
  
  const WriteQuerySchema = z.object({
    query: z.string().describe('The SQL modification query'),
  });
  
  const CreateTableSchema = z.object({
    query: z.string().describe('The DDL statement to execute (CREATE, ALTER, DROP)'),
  });
  
  const GetUserRolesSchema = z.object({});
  
  const GetDatabasesSchema = z.object({});
  
  const GetSchemasSchema = z.object({
    database: z.string().describe('The database to list schemas from'),
  });
  
  const GetTablesSchema = z.object({
    database: z.string().describe('The database name'),
    schema: z.string().describe('The schema name'),
  });
  
  const GetTableInfoSchema = z.object({
    database: z.string().describe('The database name'),
    schema: z.string().describe('The schema name'),
    table: z.string().describe('The table name'),
  });
  
  // Define our tools in MCP format
  const tools = {
    read_query: {
      name: 'read_query',
      description: 'Execute a SELECT query against Snowflake',
      inputSchema: zodToJsonSchema(ReadQuerySchema),
    },
    
    write_query: {
      name: 'write_query',
      description: 'Execute a non-SELECT query against Snowflake',
      inputSchema: zodToJsonSchema(WriteQuerySchema),
    },
    
    create_table: {
      name: 'create_table',
      description: 'Execute a DDL statement (CREATE, ALTER, DROP) against Snowflake',
      inputSchema: zodToJsonSchema(CreateTableSchema),
    },
    
    get_user_roles: {
      name: 'get_user_roles',
      description: 'Get the roles available to the current Snowflake user',
      inputSchema: zodToJsonSchema(GetUserRolesSchema),
    },
    
    get_databases: {
      name: 'get_databases',
      description: 'List all databases accessible to the current user',
      inputSchema: zodToJsonSchema(GetDatabasesSchema),
    },
    
    get_schemas: {
      name: 'get_schemas',
      description: 'List all schemas in the specified database',
      inputSchema: zodToJsonSchema(GetSchemasSchema),
    },
    
    get_tables: {
      name: 'get_tables',
      description: 'List all tables in the specified database and schema',
      inputSchema: zodToJsonSchema(GetTablesSchema),
    },
    
    get_table_info: {
      name: 'get_table_info',
      description: 'Get information about the specified table',
      inputSchema: zodToJsonSchema(GetTableInfoSchema),
    },
  };
  
  // Simple in-memory cache for query results
  const queryCache = new Map();
  const CACHE_TTL = 5 * 60 * 1000; // 5 minutes in milliseconds
  
  /**
   * Get cached result if available and not expired
   * @param {string} cacheKey - The cache key
   * @returns {Object|null} - The cached result or null if not found/expired
   */
  const getCachedResult = (cacheKey) => {
    if (!queryCache.has(cacheKey)) return null;
    
    const { timestamp, result } = queryCache.get(cacheKey);
    const now = Date.now();
    
    // Check if cache entry has expired
    if (now - timestamp > CACHE_TTL) {
      queryCache.delete(cacheKey);
      return null;
    }
    
    return result;
  };
  
  /**
   * Cache a result with the current timestamp
   * @param {string} cacheKey - The cache key
   * @param {Object} result - The result to cache
   */
  const cacheResult = (cacheKey, result) => {
    queryCache.set(cacheKey, {
      timestamp: Date.now(),
      result
    });
  };
  
  // Handler function for tool calls
  const handleToolCall = async (toolName, args) => {
    const startTime = Date.now();
    logger.info('Tool execution request received', { tool: toolName, args });
    
    switch (toolName) {
      case 'read_query': {
        const { query } = args;
        try {
          if (!query.trim().toUpperCase().startsWith('SELECT')) {
            throw new Error('Only SELECT queries are allowed with read_query');
          }

          // Generate a cache key for this query
          const cacheKey = `read_query:${query}`;
          
          // Check cache first
          const cachedResult = getCachedResult(cacheKey);
          if (cachedResult) {
            logger.info('Returning cached read query result', { 
              query: query.substring(0, 100) + (query.length > 100 ? '...' : ''),
              duration: `${Date.now() - startTime}ms (cached)`
            });
            return cachedResult;
          }

          logger.debug('Executing read query', {
            query: query.substring(0, 100) + (query.length > 100 ? '...' : ''),
          });
          const result = await executeQuery(connection, query);
          logger.info('Read query executed successfully', { 
            rowCount: Array.isArray(result) ? result.length : 'non-array',
            duration: `${Date.now() - startTime}ms`
          });

          const response = {
            content: [{ 
              type: 'text', 
              text: formatToolResult(result),
            }],
          };
          
          // Cache the result for future use
          cacheResult(cacheKey, response);
          
          return response;
        } catch (error) {
          const duration = Date.now() - startTime;
          logger.error('Error executing read query', { 
            error: error.message, 
            duration: `${duration}ms`,
            query: query.substring(0, 100) + (query.length > 100 ? '...' : '')
          });
          const suggestion = getSuggestionForError('read_query', error);
          return {
            content: [{ 
              type: 'text', 
              text: `Error: ${error.message}\n\n${suggestion}`,
            }],
            isError: true,
            suggestion
          };
        }
      }
      
      case 'write_query': {
        const { query } = args;
        try {
          if (query.trim().toUpperCase().startsWith('SELECT')) {
            throw new Error('Use read_query for SELECT queries');
          }
          if (
            query.trim().toUpperCase().startsWith('CREATE') ||
            query.trim().toUpperCase().startsWith('ALTER') ||
            query.trim().toUpperCase().startsWith('DROP')
          ) {
            throw new Error('Use create_table for DDL operations');
          }

          logger.debug('Executing write query', {
            query: query.substring(0, 100) + (query.length > 100 ? '...' : ''),
          });
          const result = await executeWriteQuery(connection, query);
          logger.info('Write query executed successfully', { affectedRows: result.affected_rows });

          return {
            content: [
              {
                type: 'text',
                text: `Query executed successfully. Rows affected: ${result.affected_rows}`,
              },
            ],
          };
        } catch (error) {
          logger.error('Error executing write query', { error: error.message });
          const suggestion = getSuggestionForError('write_query', error);
          return {
            content: [{ 
              type: 'text', 
              text: `Error: ${error.message}\n\n${suggestion}`,
            }],
            isError: true
          };
        }
      }
      
      case 'create_table': {
        const { query } = args;
        try {
          logger.debug('Executing DDL query', {
            query: query.substring(0, 100) + (query.length > 100 ? '...' : ''),
          });
          const result = await executeDDLQuery(connection, query);
          logger.info('DDL query executed successfully');

          return {
            content: [
              {
                type: 'text',
                text: 'DDL statement executed successfully.',
              },
            ],
          };
        } catch (error) {
          logger.error('Error executing DDL query', { error: error.message });
          const suggestion = getSuggestionForError('create_table', error);
          return {
            content: [{ 
              type: 'text', 
              text: `Error: ${error.message}\n\n${suggestion}`,
            }],
            isError: true
          };
        }
      }
      
      case 'get_user_roles': {
        try {
          // Check cache first
          const cacheKey = 'get_user_roles';
          const cachedResult = getCachedResult(cacheKey);
          if (cachedResult) {
            logger.info('Returning cached user roles', { duration: `${Date.now() - startTime}ms (cached)` });
            return cachedResult;
          }
          
          const query = 'SHOW ROLES;';
          const result = await executeQuery(connection, query);
          logger.info('Successfully retrieved user roles', { 
            count: result.length,
            duration: `${Date.now() - startTime}ms`
          });

          const response = {
            content: [
              {
                type: 'text',
                text: formatToolResult(result),
              },
            ],
          };
          
          // Cache the result
          cacheResult(cacheKey, response);
          
          return response;
        } catch (error) {
          logger.error('Error retrieving user roles', { error: error.message });
          return {
            content: [{ 
              type: 'text', 
              text: `Error retrieving roles: ${error.message}`,
            }],
            isError: true
          };
        }
      }
      
      case 'get_databases': {
        try {
          // Check cache first
          const cacheKey = 'get_databases';
          const cachedResult = getCachedResult(cacheKey);
          if (cachedResult) {
            logger.info('Returning cached databases', { duration: `${Date.now() - startTime}ms (cached)` });
            return cachedResult;
          }
          
          const query = 'SHOW DATABASES;';
          const result = await executeQuery(connection, query);
          logger.info('Successfully retrieved databases', { 
            count: result.length,
            duration: `${Date.now() - startTime}ms`
          });

          const response = {
            content: [
              {
                type: 'text',
                text: formatToolResult(result),
              },
            ],
          };
          
          // Cache the result
          cacheResult(cacheKey, response);
          
          return response;
        } catch (error) {
          logger.error('Error retrieving databases', { error: error.message });
          return {
            content: [{ 
              type: 'text', 
              text: `Error retrieving databases: ${error.message}`,
            }],
            isError: true
          };
        }
      }
      
      case 'get_schemas': {
        const { database } = args;
        try {
          const query = `SHOW SCHEMAS IN DATABASE ${database};`;
          const result = await executeQuery(connection, query);
          logger.info('Successfully retrieved schemas', { database, count: result.length });

          return {
            content: [
              {
                type: 'text',
                text: formatToolResult(result),
              },
            ],
          };
        } catch (error) {
          logger.error('Error retrieving schemas', { database, error: error.message });
          return {
            content: [{ 
              type: 'text', 
              text: `Error retrieving schemas from database ${database}: ${error.message}`,
            }],
            isError: true
          };
        }
      }
      
      case 'get_tables': {
        const { database, schema } = args;
        try {
          const query = `SHOW TABLES IN ${database}.${schema};`;
          const result = await executeQuery(connection, query);
          logger.info('Successfully retrieved tables', { database, schema, count: result.length });

          return {
            content: [
              {
                type: 'text',
                text: formatToolResult(result),
              },
            ],
          };
        } catch (error) {
          logger.error('Error retrieving tables', { database, schema, error: error.message });
          return {
            content: [{ 
              type: 'text', 
              text: `Error retrieving tables from ${database}.${schema}: ${error.message}`,
            }],
            isError: true
          };
        }
      }
      
      case 'get_table_info': {
        const { database, schema, table } = args;
        try {
          const query = `DESCRIBE TABLE ${database}.${schema}.${table};`;
          const result = await executeQuery(connection, query);
          logger.info('Successfully retrieved table info', { database, schema, table, columns: result.length });

          return {
            content: [
              {
                type: 'text',
                text: formatToolResult(result),
              },
            ],
          };
        } catch (error) {
          logger.error('Error retrieving table info', { database, schema, table, error: error.message });
          return {
            content: [{ 
              type: 'text', 
              text: `Error describing table ${database}.${schema}.${table}: ${error.message}`,
            }],
            isError: true
          };
        }
      }
      
      default: {
        const errorMessage = `Unknown tool: ${toolName}`;
        logger.error(errorMessage);
        
        // Log performance metrics even for errors
        const duration = Date.now() - startTime;
        logger.info('Tool call performance', { tool: toolName, status: 'error', duration: `${duration}ms` });
        
        return {
          content: [{ type: 'text', text: errorMessage }],
          isError: true,
          suggestion: 'Please use one of the available Snowflake tools. You can see the list of available tools by checking the server capabilities.'
        };
      }
    }
  };
  
  // Return the tools and handler function
  return { tools, handleToolCall };
}

/**
 * Format tool results for better readability
 * @param {any} result - The result to format
 * @returns {string} Formatted result as a string
 */
function formatToolResult(result) {
  if (!result) {
    return 'No results';
  }

  if (typeof result === 'string') {
    return result;
  }

  if (Array.isArray(result)) {
    if (result.length === 0) {
      return 'No results found';
    }

    // Handle large datasets more efficiently
    const isLargeDataset = result.length > 1000;
    if (isLargeDataset) {
      logger.info(`Formatting large dataset with ${result.length} rows`);
    }

    if (typeof result[0] === 'object') {
      // Format tabular data with column headers and rows
      const columns = Object.keys(result[0]);
      const header = columns.join('\t');
      
      // For large datasets, limit the number of rows to prevent memory issues
      const rowsToFormat = isLargeDataset ? result.slice(0, 100) : result;
      const rows = rowsToFormat.map(row => 
        columns.map(col => {
          const value = row[col];
          return value === null || value === undefined ? '' : String(value);
        }).join('\t')
      );
      
      let output = [header, ...rows].join('\n');
      if (isLargeDataset) {
        output += `\n\n[Showing 100 of ${result.length} rows]`;
      }
      return output;
    }
    
    // For large arrays of primitives, also limit the output
    if (isLargeDataset) {
      return `${result.slice(0, 100).join('\n')}\n\n[Showing 100 of ${result.length} items]`;
    }
    
    return result.join('\n');
  }

  // For objects, try to format as JSON
  try {
    // For large objects, limit the depth
    if (result && typeof result === 'object' && Object.keys(result).length > 50) {
      logger.info('Formatting large object result');
      return JSON.stringify(result, null, 1); // Use smaller indentation
    }
    return JSON.stringify(result, null, 2);
  } catch (error) {
    logger.warn('Error stringifying result', { error: error.message });
    return String(result);
  }
}

/**
 * Get a helpful suggestion based on the error
 * @param {string} toolName - The name of the tool that was called
 * @param {Error} error - The error that occurred
 * @returns {string} A suggestion to help resolve the error
 */
function getSuggestionForError(toolName, error) {
  const errorMessage = error.message.toLowerCase();
  
  if (errorMessage.includes('access denied') || errorMessage.includes('permission')) {
    return 'It looks like you may not have the necessary permissions. Try using a different role or requesting access from your Snowflake administrator.';
  }
  
  if (errorMessage.includes('timeout')) {
    return 'The operation timed out. This could be due to a complex query or system load. Try simplifying your query or running it during off-peak hours.';
  }
  
  if (errorMessage.includes('not found') || errorMessage.includes('does not exist')) {
    if (toolName === 'read_query' || toolName === 'write_query') {
      return 'The referenced database, schema, or table may not exist. Double-check the object names in your query.';
    }
    return 'The requested object does not exist. Check the spelling and try again.';
  }
  
  if (errorMessage.includes('syntax error')) {
    return 'There appears to be a syntax error in your SQL. Check for typos, missing commas, or unbalanced parentheses.';
  }
  
  // Generic fallback suggestion
  return 'Try modifying your request to address the error.';
}

/**
 * Register Snowflake tools with the MCP server
 * @param {Object} server - MCP server instance
 * @param {Object} connection - Snowflake connection object
 */
export async function registerTools(server, connection) {
  logger.info('Registering Snowflake tools with MCP server');
  
  // Validate inputs
  if (!server) {
    const error = new Error('Invalid server instance provided');
    logger.error('Failed to register tools: missing server instance', { error: error.message });
    throw error;
  }
  
  try {
    const { tools, handleToolCall } = await setupSnowflakeTools(connection);
  
  // Set up the tool handler for the server with improved error handling
  server.onToolCall = async (toolName, args) => {
    const startTime = Date.now();
    logger.info(`Tool call received: ${toolName}`, { args });
    
    try {
      const result = await handleToolCall(toolName, args);
      const duration = Date.now() - startTime;
      logger.info(`Tool call completed: ${toolName}`, { duration: `${duration}ms` });
      return result;
    } catch (error) {
      const duration = Date.now() - startTime;
      logger.error(`Tool call failed: ${toolName}`, { duration: `${duration}ms`, error: error.message });
      throw error;
    }
  };
  
  // Add tools to the server's capabilities (use nullish coalescing for better readability)
  server.capabilities ??= {};
  server.capabilities.tools ??= {};
  
  // Register each tool with the server
  Object.entries(tools).forEach(([name, tool]) => {
    server.capabilities.tools[name] = {
      description: tool.description,
      schema: tool.schema
    };
    logger.info(`Registered tool: ${name}`);
  });
  
  logger.info('All Snowflake tools registered successfully');
  } catch (error) {
    logger.error('Error setting up Snowflake tools', { error: error.message, stack: error.stack });
    throw error;
  }
}

