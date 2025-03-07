import snowflake from 'snowflake-sdk';
import fs from 'fs';
import { promisify } from 'util';

// Convert callback-based Snowflake functions to Promise-based
const readFile = promisify(fs.readFile);

/**
 * Initialize Snowflake connection using credentials from environment variables
 * Supports both password-based and private key authentication
 */
export async function initializeSnowflakeConnection() {
  try {
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
      const privateKeyPath = process.env.SNOWFLAKE_PRIVATE_KEY_PATH;
      const privateKeyContent = await readFile(privateKeyPath, 'utf8');
      
      connectionConfig.authenticator = 'SNOWFLAKE_JWT';
      connectionConfig.privateKey = privateKeyContent;
      connectionConfig.privateKeyPass = process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE;
    } else {
      connectionConfig.password = process.env.SNOWFLAKE_PASSWORD;
    }

    // Create connection object
    const connection = snowflake.createConnection(connectionConfig);
    
    // Promisify the connect method
    const connectAsync = promisify(connection.connect).bind(connection);
    await connectAsync();
    
    console.error('Successfully connected to Snowflake');
    
    // Add helper methods for executing queries
    connection.executeQueryAsync = async (query) => {
      const execute = promisify(connection.execute).bind(connection);
      const statement = await execute({
        sqlText: query,
        complete: (err, stmt) => {
          if (err) {
            console.error('Error executing query:', err);
          }
        }
      });
      
      return statement;
    };
    
    return connection;
  } catch (error) {
    console.error('Failed to connect to Snowflake:', error);
    throw error;
  }
}

/**
 * Execute a query and return results
 */
export async function executeQuery(connection, query) {
  try {
    const statement = await connection.executeQueryAsync(query);
    return statement.getRows();
  } catch (error) {
    console.error('Error executing query:', error);
    throw error;
  }
}

/**
 * Execute a write query (INSERT, UPDATE, DELETE) and return affected rows
 */
export async function executeWriteQuery(connection, query) {
  try {
    const statement = await connection.executeQueryAsync(query);
    return { affected_rows: statement.getNumRows() };
  } catch (error) {
    console.error('Error executing write query:', error);
    throw error;
  }
}

/**
 * Execute a DDL query (CREATE, ALTER, DROP) and return success status
 */
export async function executeDDLQuery(connection, query) {
  try {
    await connection.executeQueryAsync(query);
    return { success: true };
  } catch (error) {
    console.error('Error executing DDL query:', error);
    throw error;
  }
}
