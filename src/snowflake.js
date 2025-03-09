import snowflake from 'snowflake-sdk';
import fs from 'fs';
import { promisify } from 'util';
import logger from './logger.js';

// Convert callback-based Snowflake functions to Promise-based
const readFile = promisify(fs.readFile);

// Configure Snowflake driver logging
snowflake.configure({ logLevel: process.env.SNOWFLAKE_LOG_LEVEL || 'error' });

/**
 * Initialize Snowflake connection using credentials from environment variables
 * Supports both password-based and private key authentication
 * @returns {Promise<Object>} Snowflake connection object with helper methods
 */
export async function initializeSnowflakeConnection() {
  try {
    // Validate required environment variables
    const requiredVars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USERNAME'];
    const missingVars = requiredVars.filter(varName => !process.env[varName]);
    
    if (missingVars.length > 0) {
      throw new Error(`Missing required environment variables: ${missingVars.join(', ')}`);
    }
    
    // Check for authentication method
    if (!process.env.SNOWFLAKE_PASSWORD && !process.env.SNOWFLAKE_PRIVATE_KEY_PATH) {
      throw new Error('No authentication method provided. Set either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH');
    }

    // Create Snowflake connection configuration
    const connectionConfig = {
      account: process.env.SNOWFLAKE_ACCOUNT,
      username: process.env.SNOWFLAKE_USERNAME,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE,
      database: process.env.SNOWFLAKE_DATABASE,
      schema: process.env.SNOWFLAKE_SCHEMA,
      role: process.env.SNOWFLAKE_ROLE
    };

    // Use private key authentication if configured, otherwise use password
    if (process.env.SNOWFLAKE_PRIVATE_KEY_PATH) {
      logger.info('Using private key authentication');
      try {
        const privateKeyPath = process.env.SNOWFLAKE_PRIVATE_KEY_PATH;
        const privateKeyContent = await readFile(privateKeyPath, 'utf8');
        
        connectionConfig.authenticator = 'SNOWFLAKE_JWT';
        connectionConfig.privateKey = privateKeyContent;
        connectionConfig.privateKeyPass = process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE;
      } catch (keyError) {
        logger.error('Failed to read private key:', { error: keyError });
        throw new Error(`Failed to read private key: ${keyError.message}`);
      }
    } else {
      logger.info('Using password authentication');
      connectionConfig.password = process.env.SNOWFLAKE_PASSWORD;
    }

    // Create connection object
    logger.debug('Creating Snowflake connection with config:', { 
      account: connectionConfig.account,
      username: connectionConfig.username,
      warehouse: connectionConfig.warehouse,
      database: connectionConfig.database,
      schema: connectionConfig.schema,
      role: connectionConfig.role,
      authenticator: connectionConfig.authenticator
    });
    
    const connection = snowflake.createConnection(connectionConfig);
    
    // Promisify the connect method
    const connectAsync = promisify(connection.connect).bind(connection);
    
    logger.info('Connecting to Snowflake...');
    await connectAsync();
    logger.info('Successfully connected to Snowflake');
    
    // Add helper methods for executing queries
    connection.executeQueryAsync = async (query, options = {}) => {
      const queryOptions = {
        sqlText: query,
        complete: (err, stmt) => {
          if (err) {
            logger.error('Error executing query:', { error: err, query });
          }
        },
        ...options
      };
      
      logger.debug('Executing query:', { query: query.substring(0, 100) + (query.length > 100 ? '...' : '') });
      const execute = promisify(connection.execute).bind(connection);
      const statement = await execute(queryOptions);
      
      return statement;
    };
    
    // Add method to close the connection
    connection.closeAsync = async () => {
      logger.info('Closing Snowflake connection...');
      const destroyAsync = promisify(connection.destroy).bind(connection);
      await destroyAsync();
      logger.info('Snowflake connection closed');
    };
    
    return connection;
  } catch (error) {
    logger.error('Failed to connect to Snowflake:', { error });
    throw error;
  }
}

/**
 * Execute a query and return results
 * @param {Object} connection - Snowflake connection object
 * @param {string} query - SQL query to execute
 * @param {Object} options - Additional query options
 * @returns {Promise<Array>} Query results as an array of objects
 */
export async function executeQuery(connection, query, options = {}) {
  try {
    if (!query || typeof query !== 'string') {
      throw new Error('Invalid query: must be a non-empty string');
    }
    
    if (!query.trim().toUpperCase().startsWith('SELECT')) {
      logger.warn('Non-SELECT query passed to executeQuery:', { query: query.substring(0, 100) });
    }
    
    const statement = await connection.executeQueryAsync(query, options);
    const rows = statement.getRows();
    
    logger.info('Query executed successfully', { 
      rowCount: rows.length,
      queryType: getQueryType(query) 
    });
    
    return rows;
  } catch (error) {
    logger.error('Error executing query:', { error, query });
    throw error;
  }
}

/**
 * Execute a write query (INSERT, UPDATE, DELETE) and return affected rows
 * @param {Object} connection - Snowflake connection object
 * @param {string} query - SQL write query to execute
 * @param {Object} options - Additional query options
 * @returns {Promise<Object>} Object containing the number of affected rows
 */
export async function executeWriteQuery(connection, query, options = {}) {
  try {
    if (!query || typeof query !== 'string') {
      throw new Error('Invalid query: must be a non-empty string');
    }
    
    const queryType = getQueryType(query);
    if (queryType === 'SELECT') {
      throw new Error('SELECT queries should use executeQuery instead of executeWriteQuery');
    }
    
    if (['CREATE', 'ALTER', 'DROP'].includes(queryType)) {
      throw new Error(`DDL queries (${queryType}) should use executeDDLQuery instead of executeWriteQuery`);
    }
    
    const statement = await connection.executeQueryAsync(query, options);
    const affectedRows = statement.getNumRows();
    
    logger.info('Write query executed successfully', { 
      affectedRows,
      queryType
    });
    
    return { affected_rows: affectedRows };
  } catch (error) {
    logger.error('Error executing write query:', { error, query });
    throw error;
  }
}

/**
 * Execute a DDL query (CREATE, ALTER, DROP) and return success status
 * @param {Object} connection - Snowflake connection object
 * @param {string} query - SQL DDL query to execute
 * @param {Object} options - Additional query options
 * @returns {Promise<Object>} Object indicating success status
 */
export async function executeDDLQuery(connection, query, options = {}) {
  try {
    if (!query || typeof query !== 'string') {
      throw new Error('Invalid query: must be a non-empty string');
    }
    
    const queryType = getQueryType(query);
    if (!['CREATE', 'ALTER', 'DROP'].includes(queryType)) {
      logger.warn(`Non-DDL query (${queryType}) passed to executeDDLQuery:`, { query: query.substring(0, 100) });
    }
    
    await connection.executeQueryAsync(query, options);
    
    logger.info('DDL query executed successfully', { queryType });
    
    return { success: true, query_type: queryType };
  } catch (error) {
    logger.error('Error executing DDL query:', { error, query });
    throw error;
  }
}

/**
 * Determine the type of SQL query (SELECT, INSERT, UPDATE, etc.)
 * @param {string} query - SQL query to analyze
 * @returns {string} Query type in uppercase
 */
function getQueryType(query) {
  if (!query || typeof query !== 'string') {
    return 'UNKNOWN';
  }
  
  const trimmedQuery = query.trim().toUpperCase();
  
  if (trimmedQuery.startsWith('SELECT')) return 'SELECT';
  if (trimmedQuery.startsWith('INSERT')) return 'INSERT';
  if (trimmedQuery.startsWith('UPDATE')) return 'UPDATE';
  if (trimmedQuery.startsWith('DELETE')) return 'DELETE';
  if (trimmedQuery.startsWith('CREATE')) return 'CREATE';
  if (trimmedQuery.startsWith('ALTER')) return 'ALTER';
  if (trimmedQuery.startsWith('DROP')) return 'DROP';
  if (trimmedQuery.startsWith('MERGE')) return 'MERGE';
  if (trimmedQuery.startsWith('TRUNCATE')) return 'TRUNCATE';
  if (trimmedQuery.startsWith('SHOW')) return 'SHOW';
  if (trimmedQuery.startsWith('DESCRIBE')) return 'DESCRIBE';
  
  return 'UNKNOWN';
}
