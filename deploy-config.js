/**
 * Deployment configuration for Snowflake MCP Server
 * This file contains configuration for deploying the server to Snowflake image repository
 */

export const deployConfig = {
  // Image configuration
  image: {
    name: 'snowflake-mcp-server',
    registry: 'snowflakecomputing.azurecr.io',
  },
  
  // Deployment environments
  environments: {
    development: {
      tag: 'dev',
      description: 'Development environment for testing',
    },
    staging: {
      tag: 'staging',
      description: 'Staging environment for pre-production validation',
    },
    production: {
      tag: 'latest',
      description: 'Production environment',
    }
  },
  
  // Deployment steps
  steps: [
    'Build Docker image',
    'Run tests',
    'Tag image with version and environment',
    'Push to Snowflake registry',
    'Update deployment documentation'
  ]
};
