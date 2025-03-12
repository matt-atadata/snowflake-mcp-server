/**
 * Test script for Snowflake MCP Server tools
 *
 * This script tests all the tools registered with the MCP server
 * by simulating tool calls and validating the responses.
 */

import dotenv from 'dotenv';
import { initializeSnowflakeConnection } from '../src/snowflake.js';
import logger from '../src/logger.js';

// Load environment variables
dotenv.config();

// Mock MCP server for testing
class MockMcpServer {
  constructor() {
    this.tools = new Map();
  }

  tool(name, schema, handler) {
    this.tools.set(name, { schema, handler });
    return this;
  }

  async callTool(name, args) {
    const tool = this.tools.get(name);
    if (!tool) {
      throw new Error(`Tool ${name} not found`);
    }
    return await tool.handler(args);
  }
}

// Test runner
async function runTests() {
  logger.info('Starting tool tests...');

  try {
    // Create Snowflake connection
    logger.info('Connecting to Snowflake...');
    const connection = await initializeSnowflakeConnection();
    logger.info('Connected to Snowflake');

    // Create mock MCP server
    const server = new MockMcpServer();

    // Register tools with mock server
    logger.info('Registering tools...');
    const { registerTools } = await import('../src/tools.js');
    registerTools(server, connection);

    // Run tests for each tool
    await testReadQuery(server);
    await testWriteQuery(server);
    await testCreateTable(server);
    await testListDatabases(server);
    await testListSchemas(server);
    await testListTables(server);
    await testDescribeTable(server);
    await testGetQueryHistory(server);
    await testGetUserRoles(server);
    await testGetTableSample(server);

    logger.info('All tests completed successfully');
  } catch (error) {
    logger.error('Test failed:', { error: error.message, stack: error.stack });
    process.exit(1);
  } finally {
    // Exit process
    process.exit(0);
  }
}

// Test read_query tool
async function testReadQuery(server) {
  logger.info('Testing read_query tool...');

  try {
    const result = await server.callTool('read_query', {
      query: 'SELECT 1 as test',
    });

    if (!result.content || !result.content.length) {
      throw new Error('Invalid response format');
    }

    logger.info('read_query test passed');
  } catch (error) {
    logger.error('read_query test failed:', { error: error.message });
    throw error;
  }
}

// Test write_query tool (with a SELECT to test error handling)
async function testWriteQuery(server) {
  logger.info('Testing write_query tool error handling...');

  try {
    const result = await server.callTool('write_query', {
      query: 'SELECT 1 as test',
    });

    if (!result.content || !result.content.length) {
      throw new Error('Invalid response format');
    }

    // This should return an error since we're using SELECT with write_query
    if (!result.content[0].text.includes('Error')) {
      throw new Error('Expected error response not received');
    }

    logger.info('write_query error handling test passed');
  } catch (error) {
    logger.error('write_query test failed:', { error: error.message });
    throw error;
  }
}

// Test create_table tool
async function testCreateTable(server) {
  logger.info('Testing create_table tool...');

  try {
    // Use a temporary table name with timestamp to avoid conflicts
    const tempTableName = `TEST_TABLE_${Date.now()}`;

    const result = await server.callTool('create_table', {
      query: `CREATE TEMPORARY TABLE ${tempTableName} (id INT, name STRING)`,
    });

    if (!result.content || !result.content.length) {
      throw new Error('Invalid response format');
    }

    logger.info('create_table test passed');
  } catch (error) {
    logger.error('create_table test failed:', { error: error.message });
    throw error;
  }
}

// Test list_databases tool
async function testListDatabases(server) {
  logger.info('Testing list_databases tool...');

  try {
    const result = await server.callTool('list_databases', {});

    if (!result.content || !result.content.length) {
      throw new Error('Invalid response format');
    }

    logger.info('list_databases test passed');
  } catch (error) {
    logger.error('list_databases test failed:', { error: error.message });
    throw error;
  }
}

// Test list_schemas tool
async function testListSchemas(server) {
  logger.info('Testing list_schemas tool...');

  try {
    const result = await server.callTool('list_schemas', {});

    if (!result.content || !result.content.length) {
      throw new Error('Invalid response format');
    }

    logger.info('list_schemas test passed');
  } catch (error) {
    logger.error('list_schemas test failed:', { error: error.message });
    throw error;
  }
}

// Test list_tables tool
async function testListTables(server) {
  logger.info('Testing list_tables tool...');

  try {
    const result = await server.callTool('list_tables', {});

    if (!result.content || !result.content.length) {
      throw new Error('Invalid response format');
    }

    logger.info('list_tables test passed');
  } catch (error) {
    logger.error('list_tables test failed:', { error: error.message });
    throw error;
  }
}

// Test describe_table tool
async function testDescribeTable(server) {
  logger.info('Testing describe_table tool...');

  try {
    // First get a table name from list_tables
    const listTablesResult = await server.callTool('list_tables', {});
    const tablesText = listTablesResult.content[0].text;

    // Try to extract a table name from the result
    let tableName;
    try {
      const tables = JSON.parse(tablesText);
      if (tables && tables.length > 0) {
        // Use the first table name found
        tableName = tables[0].name || tables[0].TABLE_NAME || Object.values(tables[0])[0];
      }
    } catch (e) {
      // If parsing fails, use a default table
      tableName = 'INFORMATION_SCHEMA.TABLES';
    }

    // If no table was found, use a default
    if (!tableName) {
      tableName = 'INFORMATION_SCHEMA.TABLES';
    }

    const result = await server.callTool('describe_table', {
      table_name: tableName,
    });

    if (!result.content || !result.content.length) {
      throw new Error('Invalid response format');
    }

    logger.info('describe_table test passed');
  } catch (error) {
    logger.error('describe_table test failed:', { error: error.message });
    throw error;
  }
}

// Test get_query_history tool
async function testGetQueryHistory(server) {
  logger.info('Testing get_query_history tool...');

  try {
    const result = await server.callTool('get_query_history', {
      limit: 5,
    });

    if (!result.content || !result.content.length) {
      throw new Error('Invalid response format');
    }

    logger.info('get_query_history test passed');
  } catch (error) {
    logger.error('get_query_history test failed:', { error: error.message });
    throw error;
  }
}

// Test get_user_roles tool
async function testGetUserRoles(server) {
  logger.info('Testing get_user_roles tool...');

  try {
    const result = await server.callTool('get_user_roles', {});

    if (!result.content || !result.content.length) {
      throw new Error('Invalid response format');
    }

    logger.info('get_user_roles test passed');
  } catch (error) {
    logger.error('get_user_roles test failed:', { error: error.message });
    throw error;
  }
}

// Test get_table_sample tool
async function testGetTableSample(server) {
  logger.info('Testing get_table_sample tool...');

  try {
    // Use INFORMATION_SCHEMA.TABLES which should exist in any Snowflake account
    const result = await server.callTool('get_table_sample', {
      table_name: 'INFORMATION_SCHEMA.TABLES',
      limit: 3,
    });

    if (!result.content || !result.content.length) {
      throw new Error('Invalid response format');
    }

    logger.info('get_table_sample test passed');
  } catch (error) {
    logger.error('get_table_sample test failed:', { error: error.message });
    throw error;
  }
}

// Run the tests
runTests();
