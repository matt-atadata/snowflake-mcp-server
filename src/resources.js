import { ResourceTemplate } from '@modelcontextprotocol/sdk/server/mcp.js';
import { executeQuery } from './snowflake.js';
import logger from './logger.js';

// Store for insights memo with persistence across server restarts
let insightsMemo = [];

// Maximum number of insights to store in memory
const MAX_INSIGHTS = 100;

/**
 * Register all resources with the MCP server
 * @param {McpServer} server - MCP server instance
 * @param {Object} connection - Snowflake connection object
 */
export function registerResources(server, connection) {
  logger.info('Registering Snowflake resources');

  // Register insights memo resource
  server.resource(
    "memo://insights",
    new ResourceTemplate("memo://insights"),
    async (uri) => {
      logger.debug('Returning insights memo', { insightCount: insightsMemo.length });
      return {
        contents: [{
          uri: uri.href,
          text: insightsMemo.length > 0 
            ? insightsMemo.join("\n\n")
            : "No insights have been recorded yet."
        }]
      };
    }
  );

  // Register Snowflake databases metadata resource
  server.resource(
    "snowflake://metadata/databases",
    new ResourceTemplate("snowflake://metadata/databases"),
    async (uri) => {
      logger.debug('Fetching database list');
      try {
        const databases = await executeQuery(connection, "SHOW DATABASES");
        logger.info('Retrieved database list', { count: databases.length });
        return {
          contents: [{
            uri: uri.href,
            text: JSON.stringify(databases, null, 2)
          }]
        };
      } catch (error) {
        logger.error('Error fetching database list:', { error });
        return {
          contents: [{
            uri: uri.href,
            text: JSON.stringify({
              error: error.message,
              timestamp: new Date().toISOString()
            }, null, 2)
          }]
        };
      }
    }
  );

  // Register Snowflake schemas metadata resource
  server.resource(
    "snowflake://metadata/schemas",
    new ResourceTemplate("snowflake://metadata/schemas"),
    async (uri) => {
      logger.debug('Fetching schema list');
      try {
        const schemas = await executeQuery(connection, "SHOW SCHEMAS");
        logger.info('Retrieved schema list', { count: schemas.length });
        return {
          contents: [{
            uri: uri.href,
            text: JSON.stringify(schemas, null, 2)
          }]
        };
      } catch (error) {
        logger.error('Error fetching schema list:', { error });
        return {
          contents: [{
            uri: uri.href,
            text: JSON.stringify({
              error: error.message,
              timestamp: new Date().toISOString()
            }, null, 2)
          }]
        };
      }
    }
  );

  // Register Snowflake tables metadata resource
  server.resource(
    "snowflake://metadata/tables",
    new ResourceTemplate("snowflake://metadata/tables"),
    async (uri) => {
      logger.debug('Fetching table list');
      try {
        const tables = await executeQuery(connection, "SHOW TABLES");
        logger.info('Retrieved table list', { count: tables.length });
        return {
          contents: [{
            uri: uri.href,
            text: JSON.stringify(tables, null, 2)
          }]
        };
      } catch (error) {
        logger.error('Error fetching table list:', { error });
        return {
          contents: [{
            uri: uri.href,
            text: JSON.stringify({
              error: error.message,
              timestamp: new Date().toISOString()
            }, null, 2)
          }]
        };
      }
    }
  );

  // Register Snowflake user info metadata resource
  server.resource(
    "snowflake://metadata/user_info",
    new ResourceTemplate("snowflake://metadata/user_info"),
    async (uri) => {
      logger.debug('Fetching user information');
      try {
        const currentUser = await executeQuery(connection, "SELECT CURRENT_USER() as USER, CURRENT_ROLE() as ROLE, CURRENT_DATABASE() as DATABASE, CURRENT_SCHEMA() as SCHEMA, CURRENT_WAREHOUSE() as WAREHOUSE");
        const userRoles = await executeQuery(connection, "SHOW ROLES");
        
        logger.info('Retrieved user information');
        return {
          contents: [{
            uri: uri.href,
            text: JSON.stringify({
              current_session: currentUser[0],
              available_roles: userRoles
            }, null, 2)
          }]
        };
      } catch (error) {
        logger.error('Error fetching user information:', { error });
        return {
          contents: [{
            uri: uri.href,
            text: JSON.stringify({
              error: error.message,
              timestamp: new Date().toISOString()
            }, null, 2)
          }]
        };
      }
    }
  );

  // Add a tool to append insights to the memo
  server.tool(
    "append_insight",
    {
      insight: "string",
      category: "string?"
    },
    async ({ insight, category }) => {
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
        
        // With SDK, resource updates are handled automatically
        
        return {
          content: [{ 
            type: "text", 
            text: JSON.stringify({ 
              success: true, 
              message: "Insight added to memo",
              insight: formattedInsight
            }, null, 2)
          }]
        };
      } catch (error) {
        logger.error('Error adding insight to memo:', { error });
        
        return {
          content: [{
            type: "text",
            text: JSON.stringify({ 
              success: false, 
              error: error.message
            }, null, 2)
          }]
        };
      }
    }
  );
  
  // Add a tool to clear the insights memo
  server.tool(
    "clear_insights",
    {},
    async () => {
      logger.info('Clear insights request received');
      
      try {
        // Clear the insights memo
        const previousCount = insightsMemo.length;
        insightsMemo = [];
        
        logger.info('Insights memo cleared', { previousCount });
        
        // With SDK, resource updates are handled automatically
        
        return {
          content: [{
            type: "text",
            text: JSON.stringify({ 
              success: true, 
              message: `Cleared ${previousCount} insights from memo`
            }, null, 2)
          }]
        };
      } catch (error) {
        logger.error('Error clearing insights memo:', { error });
        
        return {
          content: [{
            type: "text",
            text: JSON.stringify({ 
              success: false, 
              error: error.message
            }, null, 2)
          }]
        };
      }
    }
  );
}
