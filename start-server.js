// Simple script to start the Snowflake MCP server
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

// Check if node_modules exists
if (!fs.existsSync(path.join(__dirname, 'node_modules'))) {
  console.log('❌ node_modules directory not found. Please install dependencies first.');
  console.log('You can try running: npm install --no-package-lock');
  process.exit(1);
}

console.log('🚀 Starting Snowflake MCP Server...');

// Start the server using the index.js file
const server = spawn('node', ['src/index.js'], {
  cwd: __dirname,
  stdio: 'inherit',
  env: {
    ...process.env,
    NODE_ENV: 'development'
  }
});

// Handle server events
server.on('error', (err) => {
  console.error('❌ Failed to start server:', err.message);
});

server.on('exit', (code, signal) => {
  if (code !== 0) {
    console.log(`❌ Server process exited with code ${code} and signal ${signal}`);
  } else {
    console.log('✅ Server stopped');
  }
});

// Handle process termination
process.on('SIGINT', () => {
  console.log('Received SIGINT. Shutting down server...');
  server.kill('SIGINT');
});

process.on('SIGTERM', () => {
  console.log('Received SIGTERM. Shutting down server...');
  server.kill('SIGTERM');
});
