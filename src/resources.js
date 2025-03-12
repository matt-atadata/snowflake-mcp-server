import { executeQuery } from './snowflake.js';
import logger from './logger.js';

// Ensure proper content type format according to MCP specification
const CONTENT_TYPE_JSON = 'application/json';
const CONTENT_TYPE_TEXT = 'text/plain';

// Store for insights memo with persistence across server restarts
let insightsMemo = [];

// Maximum number of insights to store in memory
const MAX_INSIGHTS = 100;

/**
 * Get all resources available from Snowflake
 * @param {Object} connection - Snowflake connection object
 * @returns {Array} List of available resources
 */
export function registerResources(connection) {
  logger.info('Preparing Snowflake resources list');
  
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
  
  // Resource: Snowflake user info metadata
  resources.push({
    uri: 'snowflake://metadata/user_info',
    title: 'Snowflake User Information',
    description: 'Current user session and role information',
  });
  
  logger.info(`Prepared ${resources.length} Snowflake resources`);  
  return resources;
}

/**
 * Get resource data for a specific URI
 * @param {string} uri - The URI to fetch resource data for
 * @param {Object} connection - Snowflake connection object
 * @returns {Promise<Object>} The resource data
 */
export async function getResourceByUri(uri, connection) {
  logger.debug('Fetching resource data for URI', { uri });
  
  try {
    const parsedUri = new URL(uri);
    const protocol = parsedUri.protocol;
    const pathname = parsedUri.pathname;
    
    // Handle memo: protocol (insights memo)
    if (protocol === 'memo:') {
      if (pathname === '/insights') {
        return handleInsightsResource();
      }
    }
    
    // Handle snowflake: protocol
    if (protocol === 'snowflake:') {
      const pathParts = pathname.split('/');
      
      // Handle metadata resources
      if (pathParts[1] === 'metadata') {
        // Handle databases resource
        if (pathParts[2] === 'databases') {
          return await getDatabasesResource(connection);
        }
        
        // Handle schemas resource
        if (pathParts[2] === 'schemas') {
          return await getSchemasResource(connection);
        }
        
        // Handle tables resource
        if (pathParts[2] === 'tables') {
          return await getTablesResource(connection);
        }
        
        // Handle user info resource
        if (pathParts[2] === 'user_info') {
          return await getUserInfoResource(connection);
        }
      }
    }
    
    // Unknown URI
    throw new Error(`Unknown resource URI: ${uri}`);
  } catch (error) {
    logger.error('Error fetching resource data:', { error });
    throw error;
  }
}

/**
 * Handle insights resource
 * @returns {Object} Insights data
 */
function handleInsightsResource() {
  logger.debug('Returning insights memo', { insightCount: insightsMemo.length });
  return {
    memo: insightsMemo.length > 0 ? insightsMemo : [],
    count: insightsMemo.length,
    isEmpty: insightsMemo.length === 0
  };
}

/**
 * Get databases resource data
 * @param {Object} connection - Snowflake connection
 * @returns {Promise<Object>} Databases data
 */
async function getDatabasesResource(connection) {
  logger.debug('Fetching database list');
  const databases = await executeQuery(connection, 'SHOW DATABASES');
  logger.info('Retrieved database list', { count: databases.length });
  return databases;
}

/**
 * Get schemas resource data
 * @param {Object} connection - Snowflake connection
 * @returns {Promise<Object>} Schemas data
 */
async function getSchemasResource(connection) {
  logger.debug('Fetching schema list');
  const schemas = await executeQuery(connection, 'SHOW SCHEMAS');
  logger.info('Retrieved schema list', { count: schemas.length });
  return schemas;
}

/**
 * Get tables resource data
 * @param {Object} connection - Snowflake connection
 * @returns {Promise<Object>} Tables data
 */
async function getTablesResource(connection) {
  logger.debug('Fetching table list');
  const tables = await executeQuery(connection, 'SHOW TABLES');
  logger.info('Retrieved table list', { count: tables.length });
  return tables;
}

/**
 * Get user info resource data
 * @param {Object} connection - Snowflake connection
 * @returns {Promise<Object>} User info data
 */
async function getUserInfoResource(connection) {
  logger.debug('Fetching user information');
  const currentUser = await executeQuery(
    connection,
    'SELECT CURRENT_USER() as USER, CURRENT_ROLE() as ROLE, CURRENT_DATABASE() as DATABASE, CURRENT_SCHEMA() as SCHEMA, CURRENT_WAREHOUSE() as WAREHOUSE'
  );
  const userRoles = await executeQuery(connection, 'SHOW ROLES');
  
  logger.info('Retrieved user information');
  return {
    current_session: currentUser[0],
    available_roles: userRoles,
  };
}

/**
 * Append an insight to the memo
 * @param {string} insight - The insight text
 * @param {string} category - Optional category tag
 * @returns {Object} Result of the operation
 */
export function appendInsight(insight, category) {
  logger.info('Adding insight to memo');

  try {
    if (!insight || typeof insight !== 'string') {
      throw new Error('Invalid insight: must provide a non-empty string');
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

    return {
      success: true,
      message: 'Insight added to memo',
      insight: formattedInsight,
    };
  } catch (error) {
    logger.error('Error adding insight to memo:', { error });
    return {
      success: false,
      error: error.message,
    };
  }
}

/**
 * Clear all insights from the memo
 * @returns {Object} Result of the operation
 */
export function clearInsights() {
  logger.info('Clearing insights memo');

  try {
    // Clear the insights memo
    const previousCount = insightsMemo.length;
    insightsMemo = [];

    logger.info('Insights memo cleared', { previousCount });

    return {
      success: true,
      message: `Cleared ${previousCount} insights from memo`,
    };
  } catch (error) {
    logger.error('Error clearing insights memo:', { error });
    return {
      success: false,
      error: error.message,
    };
  }
}
