// A standalone script to test Snowflake connection
// This doesn't depend on any other modules in the project

// Read environment variables from .env file
const fs = require('fs');
const path = require('path');
const snowflake = require('snowflake-sdk');

// Simple function to read .env file
function loadEnv() {
  const envPath = path.join(__dirname, '.env');
  const envContent = fs.readFileSync(envPath, 'utf8');
  
  const envVars = {};
  envContent.split('\n').forEach(line => {
    // Skip comments and empty lines
    if (!line || line.startsWith('#')) return;
    
    const [key, ...valueParts] = line.split('=');
    if (key && valueParts.length > 0) {
      const value = valueParts.join('=').trim();
      envVars[key.trim()] = value;
    }
  });
  
  return envVars;
}

// Test Snowflake connection
async function testConnection() {
  console.log('Loading environment variables...');
  const env = loadEnv();
  
  console.log('Testing Snowflake connection...');
  
  // Create connection configuration
  const connectionConfig = {
    account: env.SNOWFLAKE_ACCOUNT,
    username: env.SNOWFLAKE_USERNAME,
    warehouse: env.SNOWFLAKE_WAREHOUSE,
    database: env.SNOWFLAKE_DATABASE,
    schema: env.SNOWFLAKE_SCHEMA,
    role: env.SNOWFLAKE_ROLE
  };
  
  // Add authentication method
  if (env.SNOWFLAKE_PRIVATE_KEY_PATH) {
    try {
      console.log('Using private key authentication');
      const privateKeyPath = env.SNOWFLAKE_PRIVATE_KEY_PATH;
      const privateKeyContent = fs.readFileSync(privateKeyPath, 'utf8');
      
      connectionConfig.authenticator = 'SNOWFLAKE_JWT';
      connectionConfig.privateKey = privateKeyContent;
      if (env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE) {
        connectionConfig.privateKeyPass = env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE;
      }
    } catch (keyError) {
      console.error('Failed to read private key:', keyError.message);
      process.exit(1);
    }
  } else if (env.SNOWFLAKE_PASSWORD) {
    console.log('Using password authentication');
    connectionConfig.password = env.SNOWFLAKE_PASSWORD;
  } else {
    console.error('No authentication method provided. Set either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH in .env');
    process.exit(1);
  }
  
  // Create connection
  const connection = snowflake.createConnection(connectionConfig);
  
  // Connect to Snowflake
  connection.connect((err, conn) => {
    if (err) {
      console.error('❌ Failed to connect to Snowflake:');
      console.error(err.message);
      
      // Provide troubleshooting tips
      if (err.message.includes('authentication')) {
        console.log('\nTroubleshooting tips:');
        console.log('1. Check your username and password in the .env file');
        console.log('2. Verify your account identifier is correct');
      } else if (err.message.includes('account')) {
        console.log('\nTroubleshooting tips:');
        console.log('1. Verify your account identifier format (should be like: account-name.region)');
      } else if (err.message.includes('warehouse') || err.message.includes('database') || err.message.includes('schema')) {
        console.log('\nTroubleshooting tips:');
        console.log('1. Verify the warehouse, database, and schema names in your .env file');
      }
      
      process.exit(1);
    }
    
    console.log('✅ Successfully connected to Snowflake!');
    
    // Execute a test query
    console.log('Executing test query...');
    connection.execute({
      sqlText: 'SELECT CURRENT_USER() as USER, CURRENT_ROLE() as ROLE, CURRENT_DATABASE() as DATABASE, CURRENT_SCHEMA() as SCHEMA, CURRENT_WAREHOUSE() as WAREHOUSE',
      complete: (err, stmt) => {
        if (err) {
          console.error('❌ Error executing query:', err.message);
          connection.destroy();
          process.exit(1);
        }
        
        // Get query results
        const rows = stmt.getRows();
        console.log('✅ Query executed successfully!');
        console.log('Connection details:');
        console.log(JSON.stringify(rows[0], null, 2));
        
        // Close connection
        connection.destroy((err) => {
          if (err) {
            console.error('Error closing connection:', err.message);
          } else {
            console.log('Connection closed.');
          }
          process.exit(0);
        });
      }
    });
  });
}

// Run the test
testConnection();
