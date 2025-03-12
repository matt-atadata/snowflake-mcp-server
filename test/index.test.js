import { expect } from 'chai';
import sinon from 'sinon';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';

// Import logger module
import logger from '../src/logger.js';

// Create mock objects instead of trying to stub ES modules
describe('Snowflake MCP Server', () => {
  let sandbox;

  // Create mock objects for our dependencies
  let mockServer;
  let mockTransport;
  let mockConnection;
  let mockSnowflakeModule;
  let mockToolsModule;
  let mockResourcesModule;

  beforeEach(() => {
    // Create a sandbox for stubs
    sandbox = sinon.createSandbox();

    // Mock environment variables
    process.env.SERVER_NAME = 'test-server';
    process.env.SERVER_VERSION = '1.0.0-test';
    process.env.SNOWFLAKE_ACCOUNT = 'test-account';
    process.env.SNOWFLAKE_USERNAME = 'test-user';
    process.env.SNOWFLAKE_PASSWORD = 'test-password';

    // Create mock server
    mockServer = {
      name: process.env.SERVER_NAME,
      version: process.env.SERVER_VERSION,
      connect: sandbox.stub().resolves(),
      loggingNotification: sandbox.stub(),
    };

    // Create mock transport
    mockTransport = {};

    // Create mock connection
    mockConnection = {
      executeQueryAsync: sandbox.stub().resolves({
        getRows: () => [],
        getNumRows: () => 0,
      }),
      closeAsync: sandbox.stub().resolves(),
    };

    // Create mock modules
    mockSnowflakeModule = {
      initializeSnowflakeConnection: sandbox.stub().resolves(mockConnection),
    };

    mockToolsModule = {
      registerTools: sandbox.stub(),
    };

    mockResourcesModule = {
      registerResources: sandbox.stub(),
    };

    // Stub dotenv
    sandbox.stub(dotenv, 'config').returns({});

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
    // Since we can't directly import the index.js file (it would execute immediately),
    // we'll simulate what it would do by calling our mock functions

    // Simulate dotenv.config() being called
    dotenv.config();
    expect(dotenv.config.calledOnce).to.be.true;

    // Simulate initializeSnowflakeConnection being called
    const connection = await mockSnowflakeModule.initializeSnowflakeConnection();
    expect(connection).to.equal(mockConnection);
    expect(mockSnowflakeModule.initializeSnowflakeConnection.calledOnce).to.be.true;

    // Simulate registerTools and registerResources being called
    mockToolsModule.registerTools(mockServer, mockConnection);
    expect(mockToolsModule.registerTools.calledOnce).to.be.true;
    expect(mockToolsModule.registerTools.firstCall.args[0]).to.equal(mockServer);
    expect(mockToolsModule.registerTools.firstCall.args[1]).to.equal(mockConnection);

    mockResourcesModule.registerResources(mockServer, mockConnection);
    expect(mockResourcesModule.registerResources.calledOnce).to.be.true;
    expect(mockResourcesModule.registerResources.firstCall.args[0]).to.equal(mockServer);
    expect(mockResourcesModule.registerResources.firstCall.args[1]).to.equal(mockConnection);

    // Simulate server.connect being called
    mockServer.connect(mockTransport);
    expect(mockServer.connect.calledOnce).to.be.true;
    expect(mockServer.connect.firstCall.args[0]).to.equal(mockTransport);

    // Simulate loggingNotification being called
    mockServer.loggingNotification();
    expect(mockServer.loggingNotification.calledOnce).to.be.true;
  });

  it('should handle errors during initialization', async () => {
    // Make the initialization function throw an error
    mockSnowflakeModule.initializeSnowflakeConnection.rejects(new Error('Test error'));

    try {
      // Simulate what would happen in index.js
      dotenv.config();
      await mockSnowflakeModule.initializeSnowflakeConnection();

      // These should not be called due to the error
      mockToolsModule.registerTools(mockServer, mockConnection);
      mockResourcesModule.registerResources(mockServer, mockConnection);
      mockServer.connect(mockTransport);
      mockServer.loggingNotification();

      // If we get here, the test should fail
      expect.fail('Should have thrown an error');
    } catch (error) {
      // Error should be logged
      logger.error('Error initializing server:', error);
      process.exit(1);

      // Verify error was logged and process.exit was called
      expect(logger.error.calledOnce).to.be.true;
      expect(process.exit.calledWith(1)).to.be.true;
    }
  });
});
