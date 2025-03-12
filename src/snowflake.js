// Suppress deprecation warnings for punycode
process.noDeprecation = true;

// Import snowflake SDK
import snowflake from 'snowflake-sdk';
import fs from 'fs';
import { promisify } from 'util';
import logger from './logger.js';

// Constants for connection management
const MAX_CONNECTION_RETRIES = 3;
const RETRY_DELAY_MS = 2000;

// Convert callback-based Snowflake functions to Promise-based
const readFile = promisify(fs.readFile);

// Disable OCSP which can cause punycode warnings
process.env.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED = 'false';

// Create logs directory if it doesn't exist
import path from 'path';
const logsDir = path.join(process.cwd(), 'logs');
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}

// Configure Snowflake with proper logging settings
// The SDK expects specific log levels: error, warn, info, debug, trace
snowflake.configure({
  logLevel: 'error',
  logFilePath: path.join(logsDir, 'snowflake-sdk.log'),
  additionalLogToConsole: false,
  insecureConnect: false
});

logger.info('Configured Snowflake SDK logging to file');

/**
 * Initialize Snowflake connection using credentials from environment variables
 * Supports both password-based and private key authentication
 * @returns {Promise<Object>} Snowflake connection object with helper methods
 */
export async function initializeSnowflakeConnection() {
  let retries = 0;
  let lastError = null;
  
  while (retries <= MAX_CONNECTION_RETRIES) {
    try {
      // Validate required environment variables
      const requiredVars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USERNAME'];
      const missingVars = requiredVars.filter((varName) => !process.env[varName]);

      if (missingVars.length > 0) {
        throw new Error(`Missing required environment variables: ${missingVars.join(', ')}`);
      }

      // Check for authentication method
      if (!process.env.SNOWFLAKE_PASSWORD && !process.env.SNOWFLAKE_PRIVATE_KEY_PATH) {
        throw new Error(
          'No authentication method provided. Set either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH'
        );
      }

      // Create Snowflake connection configuration
      const connectionConfig = {
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USERNAME,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: process.env.SNOWFLAKE_SCHEMA,
        role: process.env.SNOWFLAKE_ROLE,
      };

      // Use private key authentication if configured, otherwise use password
      if (process.env.SNOWFLAKE_PRIVATE_KEY_PATH) {
        logger.info('Using private key authentication');
        try {
          const privateKeyPath = process.env.SNOWFLAKE_PRIVATE_KEY_PATH;
          const privateKeyContent = await readFile(privateKeyPath, 'utf8');

          connectionConfig.authenticator = 'SNOWFLAKE_JWT';
          connectionConfig.privateKey = privateKeyContent;
          
          // Add optional parameters if provided
          if (process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE) {
            connectionConfig.privateKeyPass = process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE;
          }
        } catch (keyError) {
          throw new Error(`Failed to read private key: ${keyError.message}`);
        }
      } else {
        logger.info('Using password authentication');
        connectionConfig.password = process.env.SNOWFLAKE_PASSWORD;
      }

      // Create Snowflake connection
      const connection = snowflake.createConnection(connectionConfig);

      // Promisify connection.connect
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
          ...options,
        };

        logger.debug('Executing query:', {
          query: query.substring(0, 100) + (query.length > 100 ? '...' : ''),
        });
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

      // Add ping method to test connection health
      connection.ping = async () => {
        try {
          const statement = await connection.executeQueryAsync('SELECT 1 AS connection_test');
          return { status: 'connected', message: 'Connection healthy' };
        } catch (error) {
          logger.error('Connection health check failed:', { error: error.message });
          return { status: 'error', message: error.message };
        }
      };
      
      // Add reconnect method
      connection.reconnect = async () => {
        logger.info('Attempting to reconnect to Snowflake...');
        try {
          await connection.closeAsync();
        } catch (e) {
          // Ignore errors when closing connection
          logger.warn('Error while closing connection during reconnect:', { error: e.message });
        }
        // Return a new connection
        return initializeSnowflakeConnection();
      };
      
      return connection;
    } catch (error) {
      logger.error('Failed to connect to Snowflake:', { error: error.message, stack: error.stack });
      
      // Implement retry logic
      if (retries < MAX_CONNECTION_RETRIES) {
        retries++;
        lastError = error;
        
        logger.warn(`Connection attempt failed. Retrying (${retries}/${MAX_CONNECTION_RETRIES})...`, { 
          error: error.message
        });
        
        // Wait before retrying
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS * retries));
        continue;
      }
      
      // If we've exhausted all retries, throw the error
      if (lastError) {
        logger.error('Failed to connect after multiple attempts:', { 
          attempts: retries,
          error: lastError.message
        });
        throw lastError;
      }
      throw error;
    }
  } // Close the while loop here
}

/**
 * Execute a read-only SQL query and return results
 * @param {Object} connection - Snowflake connection object
 * @param {string} query - SQL query string (SELECT statement)
 * @returns {Promise<Object>} Query results
 */
export async function executeQuery(connection, query) {
  try {
    logger.debug('Executing read query:', { query: query.substring(0, 100) });
    const statement = await connection.executeQueryAsync(query);
    return statement;
  } catch (error) {
    logger.error('Error executing query:', { error: error.message, query });
    throw error;
  }
}

/**
 * Execute a SQL query that modifies data (INSERT, UPDATE, DELETE)
 * @param {Object} connection - Snowflake connection object
 * @param {string} query - SQL query string (INSERT, UPDATE, or DELETE statement)
 * @returns {Promise<Object>} Query results
 */
export async function executeWriteQuery(connection, query) {
  try {
    logger.debug('Executing write query:', { query: query.substring(0, 100) });
    const statement = await connection.executeQueryAsync(query);
    return statement;
  } catch (error) {
    logger.error('Error executing write query:', { error: error.message, query });
    throw error;
  }
}

/**
 * Execute a DDL (Data Definition Language) SQL query (CREATE, ALTER, DROP)
 * @param {Object} connection - Snowflake connection object
 * @param {string} query - SQL query string (CREATE, ALTER, or DROP statement)
 * @returns {Promise<Object>} Query results
 */
export async function executeDDLQuery(connection, query) {
  try {
    logger.debug('Executing DDL query:', { query: query.substring(0, 100) });
    const statement = await connection.executeQueryAsync(query);
    return statement;
  } catch (error) {
    logger.error('Error executing DDL query:', { error: error.message, query });
    throw error;
  }
}
