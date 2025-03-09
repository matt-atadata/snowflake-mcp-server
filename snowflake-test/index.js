const snowflake = require('snowflake-sdk');
const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');

// Load environment variables from parent directory's .env file
dotenv.config({ path: path.join(__dirname, '..', '.env') });

// Configure Snowflake to use application logging
snowflake.configure({ logLevel: 'INFO' });

// Test Snowflake connection
async function testConnection() {
  console.log('Testing Snowflake connection...');
  
  try {
    // Create connection configuration
    const connectionConfig = {
      account: process.env.SNOWFLAKE_ACCOUNT,
      username: process.env.SNOWFLAKE_USERNAME,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE,
      database: process.env.SNOWFLAKE_DATABASE,
      schema: process.env.SNOWFLAKE_SCHEMA,
      role: process.env.SNOWFLAKE_ROLE
    };
    
    // Display connection info (without sensitive data)
    console.log('Connection configuration:');
    console.log({
      account: connectionConfig.account,
      username: connectionConfig.username,
      warehouse: connectionConfig.warehouse,
      database: connectionConfig.database,
      schema: connectionConfig.schema,
      role: connectionConfig.role
    });
    
    // Check if username is still the default value
    if (connectionConfig.username === 'your_username') {
      console.error('❌ Error: You need to set your actual Snowflake username in the .env file');
      console.log('Please update SNOWFLAKE_USERNAME in the .env file with your actual username');
      process.exit(1);
    }

    // Add authentication method
    let usePasswordFallback = false;
    
    // First try private key if available
    if (process.env.SNOWFLAKE_PRIVATE_KEY_PATH && !usePasswordFallback) {
      console.log('Attempting private key authentication');
      try {
        const privateKeyPath = process.env.SNOWFLAKE_PRIVATE_KEY_PATH;
        console.log(`Reading private key from: ${privateKeyPath}`);
        
        // Check if file exists
        if (!fs.existsSync(privateKeyPath)) {
          console.error(`❌ Private key file not found at: ${privateKeyPath}`);
          throw new Error('Private key file not found');
        }
        
        const privateKeyContent = fs.readFileSync(privateKeyPath, 'utf8');
        console.log(`Private key file read successfully (${privateKeyContent.length} bytes)`);
        
        connectionConfig.authenticator = 'SNOWFLAKE_JWT';
        connectionConfig.privateKey = privateKeyContent;
        
        if (process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE) {
          connectionConfig.privateKeyPass = process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE;
        }
      } catch (keyError) {
        console.error('Failed to set up private key authentication:', keyError.message);
        
        // If password is available, fall back to password auth
        if (process.env.SNOWFLAKE_PASSWORD && process.env.SNOWFLAKE_PASSWORD !== 'your_password') {
          console.log('Falling back to password authentication');
          usePasswordFallback = true;
        } else {
          console.error('No fallback authentication method available');
          process.exit(1);
        }
      }
    }
    
    // Use password authentication if specified or as fallback
    if ((process.env.SNOWFLAKE_PASSWORD && !process.env.SNOWFLAKE_PRIVATE_KEY_PATH) || usePasswordFallback) {
      if (process.env.SNOWFLAKE_PASSWORD === 'your_password') {
        console.error('❌ Error: You need to set your actual Snowflake password in the .env file');
        console.log('Please update SNOWFLAKE_PASSWORD in the .env file with your actual password');
        process.exit(1);
      }
      
      console.log('Using password authentication');
      connectionConfig.password = process.env.SNOWFLAKE_PASSWORD;
    } else if (!usePasswordFallback && !process.env.SNOWFLAKE_PRIVATE_KEY_PATH) {
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
        if (err.message.includes('authentication') || err.message.includes('JWT')) {
          console.log('\nTroubleshooting tips:');
          console.log('1. Check your username in the .env file (currently set to: ' + connectionConfig.username + ')');
          console.log('2. If using private key:');
          console.log('   - Ensure the private key file exists and is readable');
          console.log('   - Verify the private key is in the correct format (PKCS#8 DER format)');
          console.log('   - The private key should start with "-----BEGIN PRIVATE KEY-----"');
          console.log('3. If you have a password, update the .env file with your password and comment out the private key path');
          console.log('4. Verify your account identifier is correct (currently: ' + connectionConfig.account + ')');
        } else if (err.message.includes('account')) {
          console.log('\nTroubleshooting tips:');
          console.log('1. Your account identifier might be incorrect: ' + connectionConfig.account);
          console.log('2. The format should typically include the region, e.g.: myaccount.us-east-1');
          console.log('3. Check your Snowflake account URL to confirm the correct identifier');
        } else if (err.message.includes('warehouse') || err.message.includes('database') || err.message.includes('schema')) {
          console.log('\nTroubleshooting tips:');
          console.log('1. Verify these values in your .env file:');
          console.log('   - Warehouse: ' + connectionConfig.warehouse);
          console.log('   - Database: ' + connectionConfig.database);
          console.log('   - Schema: ' + connectionConfig.schema);
          console.log('   - Role: ' + connectionConfig.role);
        } else {
          console.log('\nGeneral troubleshooting tips:');
          console.log('1. Check all your Snowflake credentials in the .env file');
          console.log('2. Verify network connectivity to Snowflake');
          console.log('3. Check if your Snowflake account is active');
        }
        
        process.exit(1);
      }
      
      console.log('✅ Successfully connected to Snowflake!');
      
      // Execute a test query
      console.log('Executing test query...');
      connection.execute({
        sqlText: 'SELECT CURRENT_USER() as USER, CURRENT_ROLE() as ROLE, CURRENT_DATABASE() as DATABASE, CURRENT_SCHEMA() as SCHEMA, CURRENT_WAREHOUSE() as WAREHOUSE',
        complete: (err, stmt, rows) => {
          if (err) {
            console.error('❌ Error executing query:', err.message);
            connection.destroy();
            process.exit(1);
          }
          
          // Get query results - rows are passed directly to the callback in newer SDK versions
          console.log('✅ Query executed successfully!');
          console.log('Connection details:');
          if (rows && rows.length > 0) {
            console.log(JSON.stringify(rows[0], null, 2));
          } else {
            console.log('No rows returned from query');
          }
          
          // Try another query to list tables
          console.log('\nListing tables in current schema...');
          connection.execute({
            sqlText: 'SHOW TABLES',
            complete: (err, stmt, tables) => {
              if (err) {
                console.error('❌ Error listing tables:', err.message);
              } else {
                console.log(`Found ${tables ? tables.length : 0} tables:`);
                if (tables && tables.length > 0) {
                  tables.forEach(table => {
                    console.log(`- ${table.name || table.NAME}`);
                  });
                } else {
                  console.log('No tables found in the current schema.');
                }
              }
              
              // Close connection
              connection.destroy((err) => {
                if (err) {
                  console.error('Error closing connection:', err.message);
                } else {
                  console.log('\nConnection closed successfully.');
                  console.log('✅ Snowflake connection test completed!');
                }
                process.exit(0);
              });
            }
          });
        }
      });
    });
  } catch (error) {
    console.error('Unexpected error:', error);
    process.exit(1);
  }
}

// Run the test
testConnection();
