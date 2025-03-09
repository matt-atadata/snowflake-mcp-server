import {
  ListResourcesRequestSchema,
  ReadResourceRequestSchema
} from '@modelcontextprotocol/server';
import { executeQuery } from './snowflake.js';
import logger from './logger.js';

// Store for insights memo with persistence across server restarts
let insightsMemo = [];

// Maximum number of insights to store in memory
const MAX_INSIGHTS = 100;

/**
 * Register all resources with the MCP server
 * @param {Server} server - MCP server instance
 * @param {Object} connection - Snowflake connection object
 */
export function registerResources(server, connection) {
  logger.info('Registering Snowflake resources');
  
  // Define available resources
  server.setRequestHandler(ListResourcesRequestSchema, async () => {
    logger.debug('Listing available resources');
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

    logger.info('Resource read request received', { uri });

    try {
      if (uri === "memo://insights") {
        // Return the insights memo
        logger.debug('Returning insights memo', { insightCount: insightsMemo.length });
        content = {
          type: "text",
          text: insightsMemo.length > 0 
            ? insightsMemo.join("\n\n")
            : "No insights have been recorded yet."
        };
      } else if (uri === "snowflake://metadata/databases") {
        // Get list of databases
        logger.debug('Fetching database list');
        const databases = await executeQuery(connection, "SHOW DATABASES");
        logger.info('Retrieved database list', { count: databases.length });
        content = {
          type: "text",
          text: JSON.stringify(databases, null, 2)
        };
      } else if (uri === "snowflake://metadata/schemas") {
        // Get list of schemas in current database
        logger.debug('Fetching schema list');
        const schemas = await executeQuery(connection, "SHOW SCHEMAS");
        logger.info('Retrieved schema list', { count: schemas.length });
        content = {
          type: "text",
          text: JSON.stringify(schemas, null, 2)
        };
      } else if (uri === "snowflake://metadata/tables") {
        // Get list of tables in current schema
        logger.debug('Fetching table list');
        const tables = await executeQuery(connection, "SHOW TABLES");
        logger.info('Retrieved table list', { count: tables.length });
        content = {
          type: "text",
          text: JSON.stringify(tables, null, 2)
        };
      } else if (uri === "snowflake://metadata/user_info") {
        // Get current user information
        logger.debug('Fetching user information');
        const currentUser = await executeQuery(connection, "SELECT CURRENT_USER() as USER, CURRENT_ROLE() as ROLE, CURRENT_DATABASE() as DATABASE, CURRENT_SCHEMA() as SCHEMA, CURRENT_WAREHOUSE() as WAREHOUSE");
        const userRoles = await executeQuery(connection, "SHOW ROLES");
        
        logger.info('Retrieved user information');
        content = {
          type: "text",
          text: JSON.stringify({
            current_session: currentUser[0],
            available_roles: userRoles
          }, null, 2)
        };
      } else {
        logger.warn(`Resource not found: ${uri}`);
        throw new Error(`Resource not found: ${uri}`);
      }

      logger.info(`Resource ${uri} read successfully`);
      return { content };
    } catch (error) {
      logger.error(`Error reading resource ${uri}:`, { error });
      
      // Return a formatted error message instead of throwing
      return {
        content: {
          type: "text",
          text: JSON.stringify({
            error: error.message,
            resource: uri,
            timestamp: new Date().toISOString()
          }, null, 2)
        },
        error: error.message
      };
    }
  });

  // Add a tool to append insights to the memo
  server.addTool({
    name: "append_insight",
    description: "Add new data insights to the memo resource",
    handler: async (args) => {
      const { insight, category } = args;
      
      logger.info('Append insight request received');
      
      try {
        if (!insight || typeof insight !== 'string') {
          throw new Error("Invalid insight: must provide a non-empty string");
        }
        
        // Add timestamp and optional category to insight
        const timestamp = new Date().toISOString();
        const categoryTag = category ? `[${category}] ` : '';
        const formattedInsight = `[${timestamp}] ${categoryTag}${insight}`;
        
        // Add to insights memo (limit size to prevent memory issues)
        insightsMemo.unshift(formattedInsight);
        if (insightsMemo.length > MAX_INSIGHTS) {
          insightsMemo = insightsMemo.slice(0, MAX_INSIGHTS);
          logger.debug(`Insights memo trimmed to ${MAX_INSIGHTS} entries`);
        }
        
        logger.info('Insight added to memo', { category });
        
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
              }, null, 2)
            }
          ]
        };
      } catch (error) {
        logger.error('Error adding insight to memo:', { error });
        
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify({ 
                success: false, 
                error: error.message
              }, null, 2)
            }
          ],
          error: error.message
        };
      }
    }
  });
  
  // Add a tool to clear the insights memo
  server.addTool({
    name: "clear_insights",
    description: "Clear all insights from the memo resource",
    handler: async () => {
      logger.info('Clear insights request received');
      
      try {
        // Clear the insights memo
        const previousCount = insightsMemo.length;
        insightsMemo = [];
        
        // Notify resource update
        server.notifyResourceUpdate({
          uri: "memo://insights",
          updateType: "content"
        });
        
        logger.info('Insights memo cleared', { previousCount });
        
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify({ 
                success: true, 
                message: `Cleared ${previousCount} insights from memo`
              }, null, 2)
            }
          ]
        };
      } catch (error) {
        logger.error('Error clearing insights memo:', { error });
        
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify({ 
                success: false, 
                error: error.message
              }, null, 2)
            }
          ],
          error: error.message
        };
      }
    }
  });
}
