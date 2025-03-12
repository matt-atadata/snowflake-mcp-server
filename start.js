#!/usr/bin/env node

/**
 * This is a wrapper script that runs the Snowflake MCP server with the necessary
 * Node.js flags to suppress the punycode deprecation warning.
 */

// Suppress the punycode deprecation warning
process.env.NODE_NO_WARNINGS = '1';

// Import and run the actual server
import('./src/index.js').catch(err => {
  console.error('Failed to start Snowflake MCP server:', err);
  process.exit(1);
});
