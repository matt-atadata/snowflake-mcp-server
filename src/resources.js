import {
  ListResourcesRequestSchema,
  ReadResourceRequestSchema
} from '@modelcontextprotocol/server';
import { executeQuery } from './snowflake.js';

// Store for insights memo
let insightsMemo = [];

/**
 * Register all resources with the MCP server
 * @param {Server} server - MCP server instance
 * @param {Object} connection - Snowflake connection object
 */
export function registerResources(server, connection) {
  // Define available resources
  server.setRequestHandler(ListResourcesRequestSchema, async () => {
    return {
      resources: [
        {
          uri: "memo://insights",
          description: "A continuously updated data insights memo that aggregates discovered insights during analysis"
        },
        {
          uri: "snowflake://metadata/databases",
          description: "List of all accessible Snowflake databases"
        },
        {
          uri: "snowflake://metadata/schemas",
          description: "List of all schemas in the current database"
        },
        {
          uri: "snowflake://metadata/tables",
          description: "List of all tables in the current schema"
        },
        {
          uri: "snowflake://metadata/user_info",
          description: "Information about the current Snowflake user, including roles and privileges"
        }
      ]
    };
  });

  // Handle resource reading
  server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
    const { uri } = request.params;
    let content;

    try {
      if (uri === "memo://insights") {
        // Return the insights memo
        content = {
          type: "text",
          text: insightsMemo.length > 0 
            ? insightsMemo.join("\n\n")
            : "No insights have been recorded yet."
        };
      } else if (uri === "snowflake://metadata/databases") {
        // Get list of databases
        const databases = await executeQuery(connection, "SHOW DATABASES");
        content = {
          type: "text",
          text: JSON.stringify(databases, null, 2)
        };
      } else if (uri === "snowflake://metadata/schemas") {
        // Get list of schemas in current database
        const schemas = await executeQuery(connection, "SHOW SCHEMAS");
        content = {
          type: "text",
          text: JSON.stringify(schemas, null, 2)
        };
      } else if (uri === "snowflake://metadata/tables") {
        // Get list of tables in current schema
        const tables = await executeQuery(connection, "SHOW TABLES");
        content = {
          type: "text",
          text: JSON.stringify(tables, null, 2)
        };
      } else if (uri === "snowflake://metadata/user_info") {
        // Get current user information
        const currentUser = await executeQuery(connection, "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()");
        const userRoles = await executeQuery(connection, "SHOW ROLES");
        
        content = {
          type: "text",
          text: JSON.stringify({
            current_session: currentUser[0],
            available_roles: userRoles
          }, null, 2)
        };
      } else {
        throw new Error(`Resource not found: ${uri}`);
      }

      return { content };
    } catch (error) {
      console.error(`Error reading resource ${uri}:`, error);
      throw error;
    }
  });

  // Add a tool to append insights to the memo
  server.addTool({
    name: "append_insight",
    description: "Add new data insights to the memo resource",
    handler: async (args) => {
      const { insight } = args;
      
      if (!insight || typeof insight !== 'string') {
        throw new Error("Invalid insight: must provide a non-empty string");
      }
      
      // Add timestamp to insight
      const timestamp = new Date().toISOString();
      const formattedInsight = `[${timestamp}] ${insight}`;
      
      // Add to insights memo
      insightsMemo.push(formattedInsight);
      
      // Notify resource update
      server.notifyResourceUpdate({
        uri: "memo://insights",
        updateType: "content"
      });
      
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify({ 
              success: true, 
              message: "Insight added to memo",
              insight: formattedInsight
            })
          }
        ]
      };
    }
  });
}
