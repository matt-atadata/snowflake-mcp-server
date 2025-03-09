import { expect } from 'chai';
import sinon from 'sinon';
import { Server, StdioServerTransport } from '@modelcontextprotocol/server';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';

// Import modules to test
import * as snowflakeModule from '../src/snowflake.js';
import * as toolsModule from '../src/tools.js';
import * as resourcesModule from '../src/resources.js';
import logger from '../src/logger.js';

describe('Snowflake MCP Server', () => {
  let sandbox;
  let mockServer;
  let mockTransport;
  let mockConnection;
  
  beforeEach(() => {
    // Create a sandbox for stubs
    sandbox = sinon.createSandbox();
    
    // Mock environment variables
    process.env.SERVER_NAME = 'test-server';
    process.env.SERVER_VERSION = '1.0.0-test';
    process.env.SNOWFLAKE_ACCOUNT = 'test-account';
    process.env.SNOWFLAKE_USERNAME = 'test-user';
    process.env.SNOWFLAKE_PASSWORD = 'test-password';
    
    // Mock MCP server
    mockServer = {
      connect: sandbox.stub().resolves(),
      loggingNotification: sandbox.stub()
    };
    
    // Mock Stdio transport
    mockTransport = {};
    
    // Mock Snowflake connection
    mockConnection = {
      executeQueryAsync: sandbox.stub().resolves({
        getRows: () => [],
        getNumRows: () => 0
      }),
      closeAsync: sandbox.stub().resolves()
    };
    
    // Stub Server constructor
    sandbox.stub(Server.prototype, 'connect').resolves();
    sandbox.stub(Server.prototype, 'loggingNotification');
    sandbox.stub(global, 'Server').returns(mockServer);
    
    // Stub StdioServerTransport constructor
    sandbox.stub(global, 'StdioServerTransport').returns(mockTransport);
    
    // Stub Snowflake connection
    sandbox.stub(snowflakeModule, 'initializeSnowflakeConnection').resolves(mockConnection);
    
    // Stub tools and resources registration
    sandbox.stub(toolsModule, 'registerTools');
    sandbox.stub(resourcesModule, 'registerResources');
    
    // Stub dotenv
    sandbox.stub(dotenv, 'config').returns({});
    
    // Stub fs functions
    sandbox.stub(fs, 'existsSync').returns(true);
    sandbox.stub(fs, 'mkdirSync');
    
    // Stub logger
    sandbox.stub(logger, 'info');
    sandbox.stub(logger, 'error');
    sandbox.stub(logger, 'warn');
    
    // Stub process.exit
    sandbox.stub(process, 'exit');
  });
  
  afterEach(() => {
    // Restore stubs
    sandbox.restore();
  });
  
  it('should initialize the server correctly', async () => {
    // Import the main module (which will execute immediately)
    const mainModule = await import('../src/index.js');
    
    // Verify environment variables were loaded
    expect(dotenv.config.called).to.be.true;
    
    // Verify Snowflake connection was initialized
    expect(snowflakeModule.initializeSnowflakeConnection.calledOnce).to.be.true;
    
    // Verify tools and resources were registered
    expect(toolsModule.registerTools.calledOnce).to.be.true;
    expect(toolsModule.registerTools.firstCall.args[0]).to.equal(mockServer);
    expect(toolsModule.registerTools.firstCall.args[1]).to.equal(mockConnection);
    
    expect(resourcesModule.registerResources.calledOnce).to.be.true;
    expect(resourcesModule.registerResources.firstCall.args[0]).to.equal(mockServer);
    expect(resourcesModule.registerResources.firstCall.args[1]).to.equal(mockConnection);
    
    // Verify server was connected to transport
    expect(mockServer.connect.calledOnce).to.be.true;
    expect(mockServer.connect.firstCall.args[0]).to.equal(mockTransport);
    
    // Verify logging notification was sent
    expect(mockServer.loggingNotification.calledOnce).to.be.true;
  });
  
  it('should handle errors during initialization', async () => {
    // Make initializeSnowflakeConnection throw an error
    snowflakeModule.initializeSnowflakeConnection.rejects(new Error('Test error'));
    
    try {
      // Import the main module (which will execute immediately)
      const mainModule = await import('../src/index.js');
    } catch (error) {
      // Error should be logged
      expect(logger.error.called).to.be.true;
      expect(process.exit.calledWith(1)).to.be.true;
    }
  });
});
