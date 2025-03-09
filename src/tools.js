import { z } from 'zod';
import { executeQuery, executeWriteQuery, executeDDLQuery } from './snowflake.js';
import logger from './logger.js';

/**
 * Register all Snowflake tools with the MCP server
 * @param {McpServer} server - MCP server instance
 * @param {Object} connection - Snowflake connection object
 */
export function registerTools(server, connection) {
  logger.info('Registering Snowflake tools');

  // Register read_query tool
  server.tool(
    "read_query",
    {
      query: z.string().describe("The SELECT SQL query to execute")
    },
    async ({ query }) => {
      logger.info('Tool execution request received', { tool: "read_query", query });
      
      try {
        if (!query.trim().toUpperCase().startsWith("SELECT")) {
          throw new Error("Only SELECT queries are allowed with read_query");
        }
        
        logger.debug('Executing read query', { query: query.substring(0, 100) + (query.length > 100 ? '...' : '') });
        const result = await executeQuery(connection, query);
        logger.info('Read query executed successfully', { rowCount: result.length });
        
        return {
          content: [{ type: "text", text: formatToolResult(result) }]
        };
      } catch (error) {
        logger.error('Error executing read query', { error: error.message });
        const suggestion = getSuggestionForError("read_query", error);
        return {
          content: [{ type: "text", text: `Error: ${error.message}\n\n${suggestion}` }]
        };
      }
    }
  );
  
  // Register write_query tool
  server.tool(
    "write_query",
    {
      query: z.string().describe("The SQL modification query")
    },
    async ({ query }) => {
      logger.info('Tool execution request received', { tool: "write_query", query });
      
      try {
        if (query.trim().toUpperCase().startsWith("SELECT")) {
          throw new Error("Use read_query for SELECT queries");
        }
        if (query.trim().toUpperCase().startsWith("CREATE") || 
            query.trim().toUpperCase().startsWith("ALTER") || 
            query.trim().toUpperCase().startsWith("DROP")) {
          throw new Error("Use create_table for DDL operations");
        }
        
        logger.debug('Executing write query', { query: query.substring(0, 100) + (query.length > 100 ? '...' : '') });
        const result = await executeWriteQuery(connection, query);
        logger.info('Write query executed successfully', { affectedRows: result.affected_rows });
        
        return {
          content: [{ type: "text", text: `Query executed successfully. Rows affected: ${result.affected_rows}` }]
        };
      } catch (error) {
        logger.error('Error executing write query', { error: error.message });
        const suggestion = getSuggestionForError("write_query", error);
        return {
          content: [{ type: "text", text: `Error: ${error.message}\n\n${suggestion}` }]
        };
      }
    }
  );
  
  // Register create_table tool
  server.tool(
    "create_table",
    {
      query: z.string().describe("CREATE TABLE SQL statement")
    },
    async ({ query }) => {
      logger.info('Tool execution request received', { tool: "create_table", query });
      
      try {
        logger.debug('Executing DDL query', { query: query.substring(0, 100) + (query.length > 100 ? '...' : '') });
        const result = await executeDDLQuery(connection, query);
        logger.info('DDL query executed successfully');
        
        return {
          content: [{ type: "text", text: "Table operation completed successfully." }]
        };
      } catch (error) {
        logger.error('Error executing DDL query', { error: error.message });
        const suggestion = getSuggestionForError("create_table", error);
        return {
          content: [{ type: "text", text: `Error: ${error.message}\n\n${suggestion}` }]
        };
      }
    }
  );
  
  // Register list_databases tool
  server.tool(
    "list_databases",
    {},
    async () => {
      logger.info('Tool execution request received', { tool: "list_databases" });
      
      try {
        const result = await executeQuery(connection, "SHOW DATABASES");
        logger.info('List databases executed successfully');
        
        return {
          content: [{ type: "text", text: formatToolResult(result) }]
        };
      } catch (error) {
        logger.error('Error listing databases', { error: error.message });
        const suggestion = getSuggestionForError("list_databases", error);
        return {
          content: [{ type: "text", text: `Error: ${error.message}\n\n${suggestion}` }]
        };
      }
    }
  );
  
  // Register list_schemas tool
  server.tool(
    "list_schemas",
    {
      database: z.string().optional().describe("Optional database name (uses current database if not specified)")
    },
    async ({ database }) => {
      logger.info('Tool execution request received', { tool: "list_schemas", database });
      
      try {
        let listSchemasQuery = "SHOW SCHEMAS";
        if (database) {
          listSchemasQuery = `SHOW SCHEMAS IN DATABASE ${database}`;
        }
        
        const result = await executeQuery(connection, listSchemasQuery);
        logger.info('List schemas executed successfully');
        
        return {
          content: [{ type: "text", text: formatToolResult(result) }]
        };
      } catch (error) {
        logger.error('Error listing schemas', { error: error.message });
        const suggestion = getSuggestionForError("list_schemas", error);
        return {
          content: [{ type: "text", text: `Error: ${error.message}\n\n${suggestion}` }]
        };
      }
    }
  );
  
  // Register list_tables tool
  server.tool(
    "list_tables",
    {
      database: z.string().optional().describe("Optional database name (uses current database if not specified)"),
      schema: z.string().optional().describe("Optional schema name (uses current schema if not specified)")
    },
    async ({ database, schema }) => {
      logger.info('Tool execution request received', { tool: "list_tables", database, schema });
      
      try {
        let listTablesQuery = "SHOW TABLES";
        if (database && schema) {
          listTablesQuery = `SHOW TABLES IN ${database}.${schema}`;
        } else if (schema) {
          listTablesQuery = `SHOW TABLES IN SCHEMA ${schema}`;
        } else if (database) {
          listTablesQuery = `SHOW TABLES IN DATABASE ${database}`;
        }
        
        const result = await executeQuery(connection, listTablesQuery);
        logger.info('List tables executed successfully');
        
        return {
          content: [{ type: "text", text: formatToolResult(result) }]
        };
      } catch (error) {
        logger.error('Error listing tables', { error: error.message });
        const suggestion = getSuggestionForError("list_tables", error);
        return {
          content: [{ type: "text", text: `Error: ${error.message}\n\n${suggestion}` }]
        };
      }
    }
  );
  
  // Register describe_table tool
  server.tool(
    "describe_table",
    {
      table_name: z.string().describe("Name of table to describe (can be fully qualified)")
    },
    async ({ table_name }) => {
      logger.info('Tool execution request received', { tool: "describe_table", table_name });
      
      try {
        const result = await executeQuery(connection, `DESCRIBE TABLE ${table_name}`);
        logger.info('Describe table executed successfully');
        
        return {
          content: [{ type: "text", text: formatToolResult(result) }]
        };
      } catch (error) {
        logger.error('Error describing table', { error: error.message });
        const suggestion = getSuggestionForError("describe_table", error);
        return {
          content: [{ type: "text", text: `Error: ${error.message}\n\n${suggestion}` }]
        };
      }
    }
  );
  
  // Register get_query_history tool
  server.tool(
    "get_query_history",
    {
      limit: z.number().optional().describe("Maximum number of queries to return (default: 10)")
    },
    async ({ limit = 10 }) => {
      logger.info('Tool execution request received', { tool: "get_query_history", limit });
      
      try {
        const result = await executeQuery(connection, 
          `SELECT QUERY_ID, QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, 
                  EXECUTION_STATUS, START_TIME, END_TIME, TOTAL_ELAPSED_TIME 
           FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER()) 
           ORDER BY START_TIME DESC 
           LIMIT ${limit}`);
        logger.info('Get query history executed successfully');
        
        return {
          content: [{ type: "text", text: formatToolResult(result) }]
        };
      } catch (error) {
        logger.error('Error getting query history', { error: error.message });
        const suggestion = getSuggestionForError("get_query_history", error);
        return {
          content: [{ type: "text", text: `Error: ${error.message}\n\n${suggestion}` }]
        };
      }
    }
  );
  
  // Register get_user_roles tool
  server.tool(
    "get_user_roles",
    {},
    async () => {
      logger.info('Tool execution request received', { tool: "get_user_roles" });
      
      try {
        const result = await executeQuery(connection, "SHOW ROLES");
        logger.info('Get user roles executed successfully');
        
        return {
          content: [{ type: "text", text: formatToolResult(result) }]
        };
      } catch (error) {
        logger.error('Error getting user roles', { error: error.message });
        const suggestion = getSuggestionForError("get_user_roles", error);
        return {
          content: [{ type: "text", text: `Error: ${error.message}\n\n${suggestion}` }]
        };
      }
    }
  );
  
  // Register get_table_sample tool
  server.tool(
    "get_table_sample",
    {
      table_name: z.string().describe("Name of table to sample (can be fully qualified)"),
      limit: z.number().optional().describe("Maximum number of rows to return (default: 10)")
    },
    async ({ table_name, limit = 10 }) => {
      logger.info('Tool execution request received', { tool: "get_table_sample", table_name, limit });
      
      try {
        const result = await executeQuery(connection, `SELECT * FROM ${table_name} LIMIT ${limit}`);
        logger.info('Get table sample executed successfully');
        
        return {
          content: [{ type: "text", text: formatToolResult(result) }]
        };
      } catch (error) {
        logger.error('Error getting table sample', { error: error.message });
        const suggestion = getSuggestionForError("get_table_sample", error);
        return {
          content: [{ type: "text", text: `Error: ${error.message}\n\n${suggestion}` }]
        };
      }
    }
  );
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
