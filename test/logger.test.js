import { expect } from 'chai';
import sinon from 'sinon';
import winston from 'winston';

// We need to stub winston before importing our logger
const originalCreateLogger = winston.createLogger;
const mockLogger = {
  info: sinon.stub(),
  warn: sinon.stub(),
  error: sinon.stub(),
  debug: sinon.stub()
};

describe('Logger Module', () => {
  let sandbox;
  let winstonStub;
  let logger;
  
  beforeEach(() => {
    // Create a sandbox for stubs
    sandbox = sinon.createSandbox();
    
    // Stub winston.createLogger to return our mock logger
    winstonStub = sandbox.stub(winston, 'createLogger').returns(mockLogger);
    
    // Reset environment variables
    process.env.LOG_LEVEL = 'info';
    process.env.NODE_ENV = 'development';
    
    // Clear previous import cache to ensure fresh import
    delete require.cache[require.resolve('../src/logger.js')];
    
    // Import the logger after stubbing
    logger = require('../src/logger.js').default;
  });
  
  afterEach(() => {
    // Restore stubs
    sandbox.restore();
    
    // Reset mock logger
    Object.keys(mockLogger).forEach(key => {
      if (typeof mockLogger[key].reset === 'function') {
        mockLogger[key].reset();
      }
    });
  });
  
  it('should create a logger with the correct configuration', () => {
    expect(winstonStub.calledOnce).to.be.true;
    
    const config = winstonStub.firstCall.args[0];
    expect(config.level).to.equal('info');
    expect(config.defaultMeta).to.deep.equal({ service: 'snowflake-mcp-server' });
    
    // Verify transports
    expect(config.transports).to.have.lengthOf(1); // Only Console in development
  });
  
  it('should create file transports in production environment', () => {
    // Reset and recreate with production env
    sandbox.restore();
    winstonStub = sandbox.stub(winston, 'createLogger').returns(mockLogger);
    process.env.NODE_ENV = 'production';
    
    // Reimport logger
    delete require.cache[require.resolve('../src/logger.js')];
    logger = require('../src/logger.js').default;
    
    const config = winstonStub.firstCall.args[0];
    expect(config.transports).to.have.lengthOf(3); // Console + 2 File transports
  });
  
  it('should respect LOG_LEVEL environment variable', () => {
    // Reset and recreate with custom log level
    sandbox.restore();
    winstonStub = sandbox.stub(winston, 'createLogger').returns(mockLogger);
    process.env.LOG_LEVEL = 'debug';
    
    // Reimport logger
    delete require.cache[require.resolve('../src/logger.js')];
    logger = require('../src/logger.js').default;
    
    const config = winstonStub.firstCall.args[0];
    expect(config.level).to.equal('debug');
  });
  
  it('should provide a stream object for Morgan integration', () => {
    expect(logger.stream).to.be.an('object');
    expect(logger.stream.write).to.be.a('function');
    
    // Test the stream.write function
    logger.stream.write('Test message\n');
    expect(mockLogger.info.calledOnce).to.be.true;
    expect(mockLogger.info.firstCall.args[0]).to.equal('Test message');
  });
  
  it('should log messages with the correct level', () => {
    logger.info('Info message', { meta: 'data' });
    expect(mockLogger.info.calledOnce).to.be.true;
    expect(mockLogger.info.firstCall.args[0]).to.equal('Info message');
    expect(mockLogger.info.firstCall.args[1]).to.deep.equal({ meta: 'data' });
    
    logger.error('Error message', { error: new Error('Test error') });
    expect(mockLogger.error.calledOnce).to.be.true;
    
    logger.warn('Warning message');
    expect(mockLogger.warn.calledOnce).to.be.true;
    
    logger.debug('Debug message');
    expect(mockLogger.debug.calledOnce).to.be.true;
  });
});
