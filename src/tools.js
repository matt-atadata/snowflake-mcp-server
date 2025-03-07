import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/server';
import { executeQuery, executeWriteQuery, executeDDLQuery } from './snowflake.js';

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

    try {
      switch (name) {
        // Query tools
        case "read_query":
          if (!args.query.trim().toUpperCase().startsWith("SELECT")) {
            throw new Error("Only SELECT queries are allowed with read_query");
          }
          result = await executeQuery(connection, args.query);
          break;
          
        case "write_query":
          if (args.query.trim().toUpperCase().startsWith("SELECT")) {
            throw new Error("Use read_query for SELECT queries");
          }
          if (args.query.trim().toUpperCase().startsWith("CREATE") || 
              args.query.trim().toUpperCase().startsWith("ALTER") || 
              args.query.trim().toUpperCase().startsWith("DROP")) {
            throw new Error("Use create_table for DDL operations");
          }
          result = await executeWriteQuery(connection, args.query);
          break;
          
        case "create_table":
          result = await executeDDLQuery(connection, args.query);
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

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(result, null, 2)
          }
        ]
      };
    } catch (error) {
      console.error(`Error executing tool ${name}:`, error);
      throw error;
    }
  });
}
