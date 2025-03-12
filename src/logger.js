import winston from 'winston';
import fs from 'fs';
import path from 'path';

// Check if running with MCP inspector (stdio mode)
const isInspectorMode = process.argv.includes('--stdio');

// Check if debug mode is enabled via environment variable
const isDebugMode = process.env.DEBUG_MCP === 'true';

// Helper function to check if debug is enabled via file
function isDebugEnabledViaFile() {
  try {
    return fs.existsSync('logs/debug-enabled');
  } catch (e) {
    return false;
  }
}

// In inspector mode, we need to be extremely careful about stdout/stderr
// The MCP Inspector communicates with the server via JSON-RPC over stdio
// Any non-JSON-RPC output will break the communication
if (isInspectorMode) {
  // Save original stdout/stderr write methods
  const originalStdoutWrite = process.stdout.write;
  const originalStderrWrite = process.stderr.write;

  // Create logs directory for debug logs
  try {
    const logsDir = path.resolve(process.cwd(), 'logs');
    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }
  } catch (e) {
    // Silently fail - we can't log errors here
  }

  // Override stdout to only allow JSON-RPC messages
  process.stdout.write = function (chunk, encoding, callback) {
    if (chunk && typeof chunk === 'string') {
      try {
        // Try to parse as JSON
        const data = JSON.parse(chunk);
        // Only allow valid JSON-RPC messages
        if (data && typeof data === 'object' && data.jsonrpc === '2.0') {
          return originalStdoutWrite.apply(this, arguments);
        }
      } catch (e) {
        // Not valid JSON, log to file and suppress
        try {
          fs.appendFileSync(
            path.join('logs', 'inspector-filtered.log'),
            `[${new Date().toISOString()}] Filtered stdout: ${chunk}\n`
          );
        } catch (err) {
          // Silently fail
        }
      }
    }
    // For non-JSON-RPC messages, just pretend we wrote it
    if (callback) {callback();}
    return true;
  };

  // Completely silence stderr
  process.stderr.write = function (chunk, encoding, callback) {
    try {
      fs.appendFileSync(
        path.join('logs', 'inspector-stderr.log'),
        `[${new Date().toISOString()}] Stderr: ${chunk}\n`
      );
    } catch (err) {
      // Silently fail
    }
    if (callback) {callback();}
    return true;
  };
}

// Override console methods to prevent any console output in inspector mode
if (isInspectorMode) {
  // Create a function that logs to file instead of console
  const logToFile = (level, ...args) => {
    try {
      const message = args
        .map((arg) => {
          if (typeof arg === 'object') {
            return JSON.stringify(arg);
          }
          return String(arg);
        })
        .join(' ');

      fs.appendFileSync(
        path.join('logs', 'inspector-console.log'),
        `[${new Date().toISOString()}] ${level}: ${message}\n`
      );
    } catch (e) {
      // Silently fail
    }
  };

  // Override all console methods
  console.log = (...args) => logToFile('log', ...args);
  console.info = (...args) => logToFile('info', ...args);
  console.warn = (...args) => logToFile('warn', ...args);
  console.error = (...args) => logToFile('error', ...args);
  console.debug = (...args) => logToFile('debug', ...args);
}

// Ensure logs directory exists for file transports
if (process.env.NODE_ENV === 'production' || isInspectorMode) {
  try {
    const logsDir = path.resolve(process.cwd(), 'logs');
    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }
  } catch (error) {
    // In inspector mode, we've already disabled console.error
    // In non-inspector mode, log the error
    if (!isInspectorMode) {
      console.error('Failed to create logs directory:', error);
    }
  }
}

// We'll handle logging configuration through Winston only

// Configure the Winston logger - not completely silent in inspector mode if debug is enabled
const logger = winston.createLogger({
  level: (isInspectorMode && (isDebugMode || isDebugEnabledViaFile())) ? 'debug' : (process.env.LOG_LEVEL || 'info'),
  silent: isInspectorMode && !(isDebugMode || isDebugEnabledViaFile()), // Only silent in inspector mode if debug is disabled
  format: winston.format.combine(
    winston.format.timestamp({ format: () => '' }), // Empty timestamp in inspector mode
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  defaultMeta: { service: 'snowflake-mcp-server' },
  transports: [
    // Only write logs to console if not in inspector mode
    // This is critical - in inspector mode, we cannot write to stdout/stderr
    // as it will interfere with the JSON-RPC communication
    ...(!isInspectorMode
      ? [
          new winston.transports.Console({
            format: winston.format.combine(
              winston.format.colorize(),
              winston.format.printf(({ level, message, timestamp, ...meta }) => {
                return `${timestamp} ${level}: ${message} ${
                  Object.keys(meta).length ? JSON.stringify(meta, null, 2) : ''
                }`;
              })
            ),
          }),
        ]
      : []),

    // Add file transport for production environments
    ...(process.env.NODE_ENV === 'production'
      ? [
          new winston.transports.File({
            filename: 'logs/error.log',
            level: 'error',
          }),
          new winston.transports.File({
            filename: 'logs/combined.log',
          }),
        ]
      : []),
  ],
  // Disable exception handling to prevent writing to stderr in inspector mode
  // This is important because uncaught exceptions would write to stderr
  // and interfere with JSON-RPC communication
  handleExceptions: !isInspectorMode,
  handleRejections: !isInspectorMode,
});

// If in inspector mode, add a file transport for debugging
// This ensures we still have logs even when we can't use console
if (isInspectorMode) {
  try {
    // Make sure logs directory exists
    const logsDir = path.resolve(process.cwd(), 'logs');
    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }

    // Add a more detailed format for debugging
    const debugFormat = winston.format.combine(
      winston.format.timestamp(),
      winston.format.printf(({ level, message, timestamp, ...meta }) => {
        return `[${timestamp}] ${level.toUpperCase()}: ${message} ${Object.keys(meta).length ? JSON.stringify(meta, null, 2) : ''}`;
      })
    );

    logger.add(
      new winston.transports.File({
        filename: 'logs/inspector-debug.log',
        level: 'debug', // Use debug level to capture everything
        format: debugFormat,
      })
    );
    
    // Add a special transport just for resource-related logs
    logger.add(
      new winston.transports.File({
        filename: 'logs/inspector-resources.log',
        level: 'debug',
        format: debugFormat,
        // Only log messages related to resources
        filter: (info) => {
          return (
            info.message.includes('resource') ||
            info.message.includes('Resource') ||
            (info.meta && JSON.stringify(info.meta).includes('resource'))
          );
        },
      })
    );
  } catch (error) {
    // Can't log this error since we're in inspector mode
  }
}

// Create a stream object for Morgan integration (if needed)
// In inspector mode, we'll use a no-op stream to avoid writing to stdout
logger.stream = {
  write: (message) => {
    if (!isInspectorMode) {
      logger.info(message.trim());
    }
  },
};

// Create a special logger instance for inspector mode
let exportedLogger;
if (isInspectorMode) {
  // In inspector mode, create a logger that only writes to files, never to stdout/stderr
  exportedLogger = {
    // Basic logging methods that write to file instead of stdout
    info: (message, meta = {}) => {
      try {
        const logMessage =
          JSON.stringify({
            timestamp: new Date().toISOString(),
            level: 'info',
            message,
            ...meta,
          }) + '\n';
        fs.appendFileSync(path.join('logs', 'inspector-debug.log'), logMessage);
      } catch (e) {
        // Silently fail
      }
    },
    warn: (message, meta = {}) => {
      try {
        const logMessage =
          JSON.stringify({
            timestamp: new Date().toISOString(),
            level: 'warn',
            message,
            ...meta,
          }) + '\n';
        fs.appendFileSync(path.join('logs', 'inspector-debug.log'), logMessage);
      } catch (e) {
        // Silently fail
      }
    },
    error: (message, meta = {}) => {
      try {
        const logMessage =
          JSON.stringify({
            timestamp: new Date().toISOString(),
            level: 'error',
            message,
            ...meta,
          }) + '\n';
        fs.appendFileSync(path.join('logs', 'inspector-debug.log'), logMessage);
      } catch (e) {
        // Silently fail
      }
    },
    debug: (message, meta = {}) => {
      try {
        const logMessage =
          JSON.stringify({
            timestamp: new Date().toISOString(),
            level: 'debug',
            message,
            ...meta,
          }) + '\n';
        fs.appendFileSync(path.join('logs', 'inspector-debug.log'), logMessage);
      } catch (e) {
        // Silently fail
      }
    },
    // No-op stream for express middleware
    stream: {
      write: () => {
        /* no-op */
      },
    },
    // Alias for consistency with the rest of the codebase
    logToFile: (level, message, meta = {}) => {
      try {
        const logMessage =
          JSON.stringify({
            timestamp: new Date().toISOString(),
            level,
            message,
            ...meta,
          }) + '\n';
        fs.appendFileSync(path.join('logs', 'inspector-debug.log'), logMessage);
      } catch (e) {
        // Silently fail
      }
    },
  };
} else {
  exportedLogger = logger;
}

// Add special method for resource logging
if (isInspectorMode) {
  // For inspector mode logger
  exportedLogger.logResource = function(message, meta = {}) {
    try {
      const logMessage = JSON.stringify({
        timestamp: new Date().toISOString(),
        level: 'debug',
        type: 'RESOURCE',
        message,
        ...meta,
      }) + '\n';
      fs.appendFileSync(path.join('logs', 'inspector-resources.log'), logMessage);
      fs.appendFileSync(path.join('logs', 'inspector-debug.log'), logMessage);
    } catch (e) {
      // Silently fail
    }
  };
} else {
  // For standard logger
  logger.logResource = function(message, meta = {}) {
    this.debug(`RESOURCE: ${message}`, { ...meta, resourceLog: true });
  };
}

export default exportedLogger;
