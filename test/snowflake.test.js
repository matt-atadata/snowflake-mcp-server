import { expect } from 'chai';
import sinon from 'sinon';
import snowflake from 'snowflake-sdk';
import fs from 'fs';
import { promisify } from 'util';
import dotenv from 'dotenv';

// Load environment variables for testing
dotenv.config();

// Import functions to test
import {
  initializeSnowflakeConnection,
  executeQuery,
  executeWriteQuery,
  executeDDLQuery
} from '../src/snowflake.js';

describe('Snowflake Connection Module', () => {
  let sandbox;
  let mockConnection;
  let mockStatement;

  beforeEach(() => {
    // Create a sandbox for stubs
    sandbox = sinon.createSandbox();
    
    // Mock environment variables
    process.env.SNOWFLAKE_ACCOUNT = 'test-account';
    process.env.SNOWFLAKE_USERNAME = 'test-user';
    process.env.SNOWFLAKE_PASSWORD = 'test-password';
    process.env.SNOWFLAKE_WAREHOUSE = 'test-warehouse';
    process.env.SNOWFLAKE_DATABASE = 'test-database';
    process.env.SNOWFLAKE_SCHEMA = 'test-schema';
    process.env.SNOWFLAKE_ROLE = 'test-role';
    
    // Mock Snowflake statement
    mockStatement = {
      getRows: sandbox.stub().returns([{ col1: 'value1' }, { col1: 'value2' }]),
      getNumRows: sandbox.stub().returns(2)
    };
    
    // Mock Snowflake connection
    mockConnection = {
      connect: sandbox.stub().callsFake((callback) => callback(null, mockConnection)),
      execute: sandbox.stub().callsFake((options, callback) => callback(null, mockStatement)),
      destroy: sandbox.stub().callsFake((callback) => callback(null))
    };
    
    // Stub Snowflake SDK
    sandbox.stub(snowflake, 'createConnection').returns(mockConnection);
    sandbox.stub(snowflake, 'configure').returns();
    
    // Stub fs.readFile for private key tests
    sandbox.stub(fs, 'readFile').callsFake((path, encoding, callback) => {
      if (callback) {
        callback(null, 'mock-private-key-content');
      } else {
        return Promise.resolve('mock-private-key-content');
      }
    });
  });
  
  afterEach(() => {
    // Restore stubs
    sandbox.restore();
  });
  
  describe('initializeSnowflakeConnection', () => {
    it('should create a connection with password authentication', async () => {
      const connection = await initializeSnowflakeConnection();
      
      expect(snowflake.createConnection.calledOnce).to.be.true;
      expect(snowflake.createConnection.firstCall.args[0]).to.include({
        account: 'test-account',
        username: 'test-user',
        password: 'test-password'
      });
      expect(connection).to.have.property('executeQueryAsync');
      expect(connection).to.have.property('closeAsync');
    });
    
    it('should create a connection with private key authentication', async () => {
      // Set private key path
      process.env.SNOWFLAKE_PRIVATE_KEY_PATH = '/path/to/private/key.p8';
      process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = 'test-passphrase';
      delete process.env.SNOWFLAKE_PASSWORD;
      
      const connection = await initializeSnowflakeConnection();
      
      expect(snowflake.createConnection.calledOnce).to.be.true;
      expect(snowflake.createConnection.firstCall.args[0]).to.include({
        account: 'test-account',
        username: 'test-user',
        authenticator: 'SNOWFLAKE_JWT',
        privateKey: 'mock-private-key-content',
        privateKeyPass: 'test-passphrase'
      });
    });
    
    it('should throw an error if required environment variables are missing', async () => {
      delete process.env.SNOWFLAKE_ACCOUNT;
      
      try {
        await initializeSnowflakeConnection();
        expect.fail('Should have thrown an error');
      } catch (error) {
        expect(error.message).to.include('Missing required environment variables');
      }
    });
    
    it('should throw an error if no authentication method is provided', async () => {
      delete process.env.SNOWFLAKE_PASSWORD;
      
      try {
        await initializeSnowflakeConnection();
        expect.fail('Should have thrown an error');
      } catch (error) {
        expect(error.message).to.include('No authentication method provided');
      }
    });
  });
  
  describe('executeQuery', () => {
    let connection;
    
    beforeEach(async () => {
      connection = await initializeSnowflakeConnection();
    });
    
    it('should execute a SELECT query and return rows', async () => {
      const query = 'SELECT * FROM test_table';
      const result = await executeQuery(connection, query);
      
      expect(connection.executeQueryAsync.calledOnce).to.be.true;
      expect(result).to.deep.equal([{ col1: 'value1' }, { col1: 'value2' }]);
    });
    
    it('should throw an error for invalid queries', async () => {
      try {
        await executeQuery(connection, null);
        expect.fail('Should have thrown an error');
      } catch (error) {
        expect(error.message).to.include('Invalid query');
      }
    });
  });
  
  describe('executeWriteQuery', () => {
    let connection;
    
    beforeEach(async () => {
      connection = await initializeSnowflakeConnection();
    });
    
    it('should execute an INSERT query and return affected rows', async () => {
      const query = 'INSERT INTO test_table VALUES (1, "test")';
      const result = await executeWriteQuery(connection, query);
      
      expect(connection.executeQueryAsync.calledOnce).to.be.true;
      expect(result).to.deep.equal({ affected_rows: 2 });
    });
    
    it('should throw an error for SELECT queries', async () => {
      try {
        await executeWriteQuery(connection, 'SELECT * FROM test_table');
        expect.fail('Should have thrown an error');
      } catch (error) {
        expect(error.message).to.include('SELECT queries should use executeQuery');
      }
    });
    
    it('should throw an error for DDL queries', async () => {
      try {
        await executeWriteQuery(connection, 'CREATE TABLE test_table (id INT)');
        expect.fail('Should have thrown an error');
      } catch (error) {
        expect(error.message).to.include('DDL queries');
      }
    });
  });
  
  describe('executeDDLQuery', () => {
    let connection;
    
    beforeEach(async () => {
      connection = await initializeSnowflakeConnection();
    });
    
    it('should execute a CREATE TABLE query and return success', async () => {
      const query = 'CREATE TABLE test_table (id INT)';
      const result = await executeDDLQuery(connection, query);
      
      expect(connection.executeQueryAsync.calledOnce).to.be.true;
      expect(result).to.deep.equal({ success: true, query_type: 'CREATE' });
    });
  });
});
