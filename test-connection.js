import dotenv from 'dotenv';
import { initializeSnowflakeConnection, executeQuery } from './src/snowflake.js';

// Load environment variables from .env file
dotenv.config();

async function testSnowflakeConnection() {
  console.log('Testing Snowflake connection...');
  
  try {
    // Initialize connection
    const connection = await initializeSnowflakeConnection();
    console.log('✅ Successfully connected to Snowflake!');
    
    // Test a simple query
    console.log('Executing test query...');
    const result = await executeQuery(connection, 'SELECT CURRENT_USER() as USER, CURRENT_ROLE() as ROLE, CURRENT_DATABASE() as DATABASE, CURRENT_SCHEMA() as SCHEMA, CURRENT_WAREHOUSE() as WAREHOUSE');
    
    console.log('✅ Query executed successfully!');
    console.log('Connection details:');
    console.log(JSON.stringify(result[0], null, 2));
    
    // Close connection
    await connection.closeAsync();
    console.log('Connection closed.');
    
    return true;
  } catch (error) {
    console.error('❌ Failed to connect to Snowflake:');
    console.error(error.message);
    
    // Provide helpful troubleshooting tips based on the error
    if (error.message.includes('authentication')) {
      console.log('\nTroubleshooting tips:');
      console.log('1. Check your username and password in the .env file');
      console.log('2. Verify your account identifier is correct');
      console.log('3. If using private key authentication, ensure the key file exists and is readable');
    } else if (error.message.includes('account')) {
      console.log('\nTroubleshooting tips:');
      console.log('1. Verify your account identifier format (should be like: account-name.region)');
      console.log('2. Check if your account is active and accessible');
    } else if (error.message.includes('warehouse') || error.message.includes('database') || error.message.includes('schema')) {
      console.log('\nTroubleshooting tips:');
      console.log('1. Verify the warehouse, database, and schema names in your .env file');
      console.log('2. Check if you have access to these resources with your role');
    }
    
    return false;
  }
}

// Run the test
testSnowflakeConnection();
