#!/usr/bin/env node

/**
 * Deployment script for Snowflake MCP Server
 * This script automates the process of building, testing, and deploying
 * the Snowflake MCP Server to the Snowflake image repository
 */

import { execSync } from 'child_process';
import { deployConfig } from './deploy-config.js';
import fs from 'fs';
import path from 'path';
import readline from 'readline';

// ANSI color codes for terminal output
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
  red: '\x1b[31m'
};

// Get package version
const packageJson = JSON.parse(fs.readFileSync('./package.json', 'utf8'));
const version = packageJson.version;

// Create readline interface for user input
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

// Helper function to execute shell commands
function execute(command, options = {}) {
  console.log(`${colors.dim}> ${command}${colors.reset}`);
  return execSync(command, { stdio: 'inherit', ...options });
}

// Helper function to prompt for confirmation
function confirm(message) {
  return new Promise((resolve) => {
    rl.question(`${colors.yellow}${message} (y/n) ${colors.reset}`, (answer) => {
      resolve(answer.toLowerCase() === 'y' || answer.toLowerCase() === 'yes');
    });
  });
}

// Helper function to prompt for environment selection
function selectEnvironment() {
  return new Promise((resolve) => {
    console.log(`${colors.cyan}Select deployment environment:${colors.reset}`);
    const environments = Object.keys(deployConfig.environments);
    
    environments.forEach((env, index) => {
      const config = deployConfig.environments[env];
      console.log(`${colors.bright}${index + 1}${colors.reset}. ${env} (${config.tag}) - ${config.description}`);
    });
    
    rl.question(`${colors.yellow}Enter environment number (1-${environments.length}): ${colors.reset}`, (answer) => {
      const selection = parseInt(answer, 10);
      if (isNaN(selection) || selection < 1 || selection > environments.length) {
        console.log(`${colors.red}Invalid selection. Defaulting to development.${colors.reset}`);
        resolve('development');
      } else {
        resolve(environments[selection - 1]);
      }
    });
  });
}

// Main deployment function
async function deploy() {
  try {
    console.log(`${colors.bright}${colors.blue}Snowflake MCP Server Deployment${colors.reset}`);
    console.log(`${colors.cyan}Version: ${version}${colors.reset}`);
    
    // Select environment
    const environment = await selectEnvironment();
    const envConfig = deployConfig.environments[environment];
    console.log(`${colors.green}Deploying to ${environment} environment${colors.reset}`);
    
    // Run tests
    console.log(`\n${colors.bright}${colors.cyan}Step 1: Running tests${colors.reset}`);
    if (await confirm('Run tool tests?')) {
      execute('npm run test:tools');
    }
    
    // Build Docker image
    console.log(`\n${colors.bright}${colors.cyan}Step 2: Building Docker image${colors.reset}`);
    const imageName = deployConfig.image.name;
    execute(`docker build -t ${imageName} .`);
    
    // Tag images
    console.log(`\n${colors.bright}${colors.cyan}Step 3: Tagging images${colors.reset}`);
    const registry = deployConfig.image.registry;
    const versionTag = `${registry}/${imageName}:${version}`;
    const envTag = `${registry}/${imageName}:${envConfig.tag}`;
    
    execute(`docker tag ${imageName} ${versionTag}`);
    execute(`docker tag ${imageName} ${envTag}`);
    
    console.log(`${colors.green}Tagged images:${colors.reset}`);
    console.log(`- ${versionTag}`);
    console.log(`- ${envTag}`);
    
    // Push to registry
    console.log(`\n${colors.bright}${colors.cyan}Step 4: Pushing to Snowflake registry${colors.reset}`);
    if (await confirm('Log in to Snowflake registry?')) {
      execute(`docker login ${registry}`);
    }
    
    if (await confirm('Push images to registry?')) {
      execute(`docker push ${versionTag}`);
      execute(`docker push ${envTag}`);
      console.log(`${colors.green}Successfully pushed images to registry${colors.reset}`);
    }
    
    console.log(`\n${colors.bright}${colors.green}Deployment complete!${colors.reset}`);
    console.log(`${colors.cyan}Version: ${version}${colors.reset}`);
    console.log(`${colors.cyan}Environment: ${environment}${colors.reset}`);
    
  } catch (error) {
    console.error(`${colors.red}Deployment failed: ${error.message}${colors.reset}`);
    process.exit(1);
  } finally {
    rl.close();
  }
}

// Run the deployment
deploy();
