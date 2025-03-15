#!/usr/bin/env python3
"""
Snowflake MCP Server using FastMCP

This server provides access to Snowflake data through the Model Context Protocol.
It implements tools for executing queries against Snowflake and exploring database structure.
"""

import os
import logging
import argparse
import datetime
import json
import urllib.parse
import secrets
import webbrowser
import requests
from typing import Dict, List, Optional, Any, Tuple

# Configure logging to file
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "snowflake_mcp_server.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # Keep console output for debugging
    ]
)
logger = logging.getLogger("snowflake_mcp_server")

# Create global args variable with defaults
class DefaultArgs:
    def __init__(self):
        self.allow_write = False
        self.port = 8090
        self.mcp_port = 8091
        self.log_level = "WARNING"
        self.auth_method = "password"  # Options: "password", "oauth"
        self.oauth_client_id = ""
        self.oauth_client_secret = ""
        self.oauth_redirect_uri = "http://localhost:8090/oauth/callback"
        self.oauth_token_cache_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "oauth_tokens.json")

# Initialize with default values, will be updated with command line args later
args = DefaultArgs()

# Set PORT environment variable for the MCP server
# This must be done before importing FastMCP
os.environ["PORT"] = os.environ.get("PORT", "8001")

# Try to import Snowflake connector, but make it optional for testing
try:
    import snowflake.connector

    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    logger.warning(
        "snowflake-connector-python not installed. Running in test mode only."
    )

# Try to import dotenv, but make it optional
try:
    from dotenv import load_dotenv

    load_dotenv()
    logger.info("Loaded environment variables from .env file")
except ImportError:
    logger.warning(
        "python-dotenv not installed. Environment variables must be set manually."
    )

# Import FastMCP after setting PORT
try:
    from mcp.server.fastmcp import FastMCP

    logger.info("FastMCP imported successfully")
except ImportError:
    logger.error("FastMCP not installed. Please install it with 'pip install fastmcp'")
    raise

# Snowflake connection configuration (only if Snowflake is available)
if SNOWFLAKE_AVAILABLE:
    SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
    SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
    SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
    SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Initialize the MCP server with default settings
# The actual server with command-line arguments will be created in the main block
server = FastMCP(
    name="Snowflake MCP Server",
    description="A server that provides access to Snowflake data through MCP",
    allow_write=False,  # Default to not allowing write operations for safety
)

# Connection pool for Snowflake connections
connection_pool = {}
MAX_POOL_SIZE = 5
CONNECTION_TIMEOUT = 60  # seconds


# Helper function to get a Snowflake connection (only if Snowflake is available)
def get_snowflake_connection():
    """Create and return a connection to Snowflake using connection pooling.

    This function implements Snowflake best practices for connection handling:
    1. Uses connection pooling to reduce connection overhead
    2. Implements proper session parameter configuration
    3. Handles warehouse access with fallback options
    4. Validates required environment variables
    5. Provides detailed error messages for troubleshooting

    Returns:
        A configured Snowflake connection object

    Raises:
        ImportError: If Snowflake connector is not installed
        ValueError: If required environment variables are missing
        PermissionError: If there are insufficient privileges
    """
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError(
            "snowflake-connector-python is not installed. Cannot connect to Snowflake."
        )

    # Validate environment variables
    SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
    SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
    SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
    SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
    SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

    # Check required variables
    required_vars = [
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ROLE,
    ]
    if not all(required_vars):
        raise ValueError(
            "Missing required Snowflake environment variables. Ensure SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, and SNOWFLAKE_ROLE are set."
        )

    # Create a unique key for this connection configuration
    conn_key = f"{SNOWFLAKE_ACCOUNT}_{SNOWFLAKE_USER}_{SNOWFLAKE_ROLE}"
    if SNOWFLAKE_WAREHOUSE:
        conn_key += f"_{SNOWFLAKE_WAREHOUSE}"

    # Check if we have a valid connection in the pool
    if conn_key in connection_pool and len(connection_pool[conn_key]) > 0:
        try:
            # Get a connection from the pool
            conn = connection_pool[conn_key].pop()

            # Test if the connection is still valid
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()

            logger.debug(f"Reusing existing connection from pool for {SNOWFLAKE_USER}")
            return conn
        except Exception as e:
            logger.warning(
                f"Pooled connection is no longer valid, creating new one: {e}"
            )
            # Connection is no longer valid, create a new one
            try:
                conn.close()
            except Exception as close_error:
                logger.debug(f"Error closing invalid connection: {close_error}")

    # Create a new connection
    try:
        # Build connection parameters dict with best practices
        connect_params = {
            "account": SNOWFLAKE_ACCOUNT,
            "user": SNOWFLAKE_USER,
            "password": SNOWFLAKE_PASSWORD,
            "autocommit": True,  # Enable autocommit for read-only operations
            "client_session_keep_alive": True,  # Keep session alive
            "application": "Snowflake_MCP_Server",  # Identify application in Snowflake logs
            "client_prefetch_threads": 4,  # Improve performance for large result sets
            "network_timeout": CONNECTION_TIMEOUT,  # Set network timeout
            "login_timeout": CONNECTION_TIMEOUT,  # Set login timeout
        }

        # Add optional parameters if they exist
        # Note: We don't add warehouse here to allow metadata operations to work without a warehouse
        if SNOWFLAKE_ROLE:
            connect_params["role"] = SNOWFLAKE_ROLE
        if SNOWFLAKE_DATABASE:
            connect_params["database"] = SNOWFLAKE_DATABASE
        if SNOWFLAKE_SCHEMA:
            connect_params["schema"] = SNOWFLAKE_SCHEMA

        # Connect to Snowflake
        conn = snowflake.connector.connect(**connect_params)

        # Initialize the connection with proper settings
        cursor = conn.cursor()

        # Set session parameters with error handling for each step
        try:
            cursor.execute(f"USE ROLE {SNOWFLAKE_ROLE}")
            logger.info(f"Successfully set role to {SNOWFLAKE_ROLE}")
        except Exception as e:
            logger.error(f"Error setting role to {SNOWFLAKE_ROLE}: {e}")
            raise PermissionError(f"Cannot set role to {SNOWFLAKE_ROLE}. Error: {e}")

        # Try to use the warehouse if specified, but don't fail if it doesn't work
        # This allows metadata operations to work without a warehouse
        if SNOWFLAKE_WAREHOUSE:
            try:
                cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
                logger.info(f"Successfully set warehouse to {SNOWFLAKE_WAREHOUSE}")
            except Exception as e:
                error_msg = str(e)
                if "Insufficient privileges" in error_msg:
                    logger.warning(
                        f"Insufficient privileges to use warehouse {SNOWFLAKE_WAREHOUSE}. Some operations may fail."
                    )
                    logger.warning(
                        "Consider using a different role or warehouse with appropriate permissions."
                    )
                else:
                    logger.warning(
                        f"Could not set warehouse to {SNOWFLAKE_WAREHOUSE}: {e}"
                    )
                    logger.warning(
                        "Some operations may fail without warehouse access. Consider using a different role."
                    )

                # Try to find an accessible warehouse for operations that require one
                try:
                    cursor.execute("SHOW WAREHOUSES")
                    warehouses = cursor.fetchall()
                    if warehouses:
                        for warehouse_info in warehouses:
                            warehouse_name = warehouse_info[0]
                            if (
                                warehouse_name != SNOWFLAKE_WAREHOUSE
                            ):  # Skip the one we already tried
                                try:
                                    cursor.execute(f"USE WAREHOUSE {warehouse_name}")
                                    logger.info(
                                        f"Found alternative warehouse: {warehouse_name}"
                                    )
                                    break
                                except Exception as warehouse_error:
                                    logger.debug(
                                        f"Could not use warehouse {warehouse_name}: {warehouse_error}"
                                    )
                                    continue  # Try next warehouse
                except Exception as warehouse_error:
                    logger.debug(
                        f"Could not find alternative warehouse: {warehouse_error}"
                    )

        # Set database and schema if provided
        if SNOWFLAKE_DATABASE:
            try:
                cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
                logger.info(f"Successfully set database to {SNOWFLAKE_DATABASE}")
            except Exception as e:
                logger.warning(f"Could not set database to {SNOWFLAKE_DATABASE}: {e}")

        if SNOWFLAKE_SCHEMA:
            try:
                cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
                logger.info(f"Successfully set schema to {SNOWFLAKE_SCHEMA}")
            except Exception as e:
                logger.warning(f"Could not set schema to {SNOWFLAKE_SCHEMA}: {e}")

        # Set session parameters for better performance and reliability
        try:
            # Set query timeout to prevent long-running queries
            cursor.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 300")

            # Set timezone to UTC for consistency
            cursor.execute("ALTER SESSION SET TIMEZONE = 'UTC'")

            # Enable result caching for better performance
            cursor.execute("ALTER SESSION SET USE_CACHED_RESULT = TRUE")

            # Set query tag for tracking in Snowflake history
            cursor.execute("ALTER SESSION SET QUERY_TAG = 'snowflake_mcp_server'")
        except Exception as session_error:
            logger.warning(f"Could not set all session parameters: {session_error}")

        logger.info(f"Connected to Snowflake using role {SNOWFLAKE_ROLE}")
        return conn
    except snowflake.connector.errors.DatabaseError as e:
        error_msg = str(e)
        if "Insufficient privileges" in error_msg:
            logger.error(f"Permission error connecting to Snowflake: {e}")
            raise PermissionError(
                f"Insufficient privileges for role {SNOWFLAKE_ROLE}. Error: {e}"
            )
        elif "Authentication failed" in error_msg:
            logger.error(f"Authentication error connecting to Snowflake: {e}")
            raise ValueError(
                "Authentication failed. Please check your Snowflake credentials."
            )
        else:
            logger.error(f"Database error connecting to Snowflake: {e}")
            raise
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        raise


# Helper function to format output in a readable way
def format_output(data):
    """
    Format output data to be more readable and user-friendly.
    
    Args:
        data: The data to format (dict, list, or other JSON-serializable object)
        
    Returns:
        Formatted string representation of the data
    """
    if data is None:
        return "No data available"
        
    try:
        # For dictionaries and lists, use pretty-printed JSON
        if isinstance(data, (dict, list)):
            return json.dumps(data, indent=2, default=str)
        return str(data)
    except Exception as e:
        logger.warning(f"Error formatting output: {e}")
        return str(data)


# Helper function to return a connection to the pool
def return_connection_to_pool(conn):
    """Return a connection to the pool instead of closing it."""
    if not conn or not SNOWFLAKE_AVAILABLE:
        return

    try:
        # Get connection parameters
        cursor = conn.cursor()
        cursor.execute(
            "SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()"
        )
        account, user, role, warehouse = cursor.fetchone()

        # Create a unique key for this connection
        conn_key = f"{account}_{user}_{role}_{warehouse}"

        # Initialize the pool for this key if it doesn't exist
        if conn_key not in connection_pool:
            connection_pool[conn_key] = []

        # Only add to pool if we haven't reached max size
        if len(connection_pool[conn_key]) < MAX_POOL_SIZE:
            connection_pool[conn_key].append(conn)
            logger.debug(f"Returned connection to pool for {user}")
        else:
            # Pool is full, close the connection
            conn.close()
            logger.debug(f"Connection pool full, closed connection for {user}")
    except Exception as e:
        # If there's any error, just close the connection
        logger.warning(f"Error returning connection to pool: {e}")
        try:
            conn.close()
        except Exception as close_error:
            logger.debug(f"Error closing connection: {close_error}")


# Tool implementations
@server.tool("execute_query")
def execute_query(query: str, limit_rows: Optional[int] = 1000) -> Dict[str, Any]:
    """
    
    Execute a SQL query against Snowflake and return the results.

    Args:
        query: SQL query to execute (Snowflake dialect)
        limit_rows: Maximum number of rows to return (default: 1000)

    Returns:
        Query results including columns, rows, and metadata
    """
    logger.info(f"Executing query: {query}")

    # Check if Snowflake connector is available
    if not SNOWFLAKE_AVAILABLE:
        return {
            "error": "snowflake-connector-python is not installed. Running in test mode only.",
            "test_mode": True,
            "query": query,
        }

    # Check if query contains write operations and server is configured to allow them
    if not args.allow_write and any(
        write_op in query.upper()
        for write_op in ["INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"]
    ):
        raise ValueError(
            "Write operations are not allowed. Start the server with --allow-write to enable them."
        )

    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)

        # Get column names
        columns = [col[0] for col in cursor.description] if cursor.description else []

        # Fetch rows with limit
        rows = cursor.fetchmany(limit_rows) if limit_rows else cursor.fetchall()
        row_count = len(rows)

        # Check if there are more rows
        truncated = cursor.fetchone() is not None
        
        # Convert rows to list of dictionaries for better readability
        formatted_rows = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = row[i]
            formatted_rows.append(row_dict)

        result = {
            "columns": columns,
            "rows": formatted_rows,
            "row_count": row_count,
            "truncated": truncated,
        }
        
        # Return formatted output
        return format_output(result)
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_alerts")
def show_alerts(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all alerts in Snowflake.
    
    Args:
        database: Optional database name to filter alerts (e.g., 'MYDB')
        schema: Optional schema name to filter alerts (e.g., 'PUBLIC')
    
    Returns:
        List of alerts with their details
    """
    logger.info("Listing all alerts")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list alerts.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        # Build the query with optional filters
        query = "SHOW ALERTS"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing alerts: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_application_roles")
def show_application_roles(application_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all application roles in Snowflake.
    
    Args:
        application_name: Optional application name to filter roles (e.g., 'MY_APP')
    
    Returns:
        List of application roles with their details
    """
    logger.info("Listing all application_roles")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list application_roles.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW APPLICATION_ROLES"
        if application_name:
            query += f" IN APPLICATION {application_name}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing application_roles: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_applications")
def show_applications(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all applications in Snowflake.
    
    Args:
        database: Optional database name to filter applications (e.g., 'MYDB')
        schema: Optional schema name to filter applications (e.g., 'PUBLIC')
    
    Returns:
        List of applications with their details
    """
    logger.info("Listing all applications")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list applications.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW APPLICATIONS"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing applications: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_columns")
def show_columns(table_name: Optional[str] = None, database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all columns in a table or across Snowflake.
    
    Args:
        table_name: Name of the table to show columns for (e.g., 'CUSTOMERS')
        database: Database name (e.g., 'MYDB')
        schema: Schema name (e.g., 'PUBLIC')
        
    Note:
        If providing a table_name, you must specify both database and schema.
        Example: show_columns(table_name="CUSTOMERS", database="MYDB", schema="PUBLIC")
    
    Returns:
        List of columns with their details
    """
    logger.info("Listing all columns")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list columns.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW COLUMNS"
        if table_name and database and schema:
            query += f" IN TABLE {database}.{schema}.{table_name}"
        elif database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing columns: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_connections")
def show_connections(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all connections in Snowflake.
    
    Args:
        database: Optional database name to filter connections (e.g., 'MYDB')
        schema: Optional schema name to filter connections (e.g., 'PUBLIC')
    
    Returns:
        List of connections with their details
    """
    logger.info("Listing all connections")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list connections.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW CONNECTIONS"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing connections: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_databases")
def show_databases(pattern: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all databases in Snowflake.
    
    Args:
        pattern: Optional pattern to filter database names (e.g., 'PROD%')
    
    Returns:
        List of databases with their details
    """
    logger.info("Listing all databases")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list databases.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW DATABASES"
        if pattern:
            query += f" LIKE '{pattern}'"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing databases: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_external_functions")
def show_external_functions(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all external functions in Snowflake.
    
    Args:
        database: Optional database name to filter functions (e.g., 'MYDB')
        schema: Optional schema name to filter functions (e.g., 'PUBLIC')
    
    Returns:
        List of external functions with their details
    """
    logger.info("Listing all external_functions")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list external_functions.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW EXTERNAL_FUNCTIONS"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing external_functions: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_external_tables")
def show_external_tables(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all external tables in Snowflake.
    
    Args:
        database: Optional database name to filter tables (e.g., 'MYDB')
        schema: Optional schema name to filter tables (e.g., 'PUBLIC')
    
    Returns:
        List of external tables with their details
    """
    logger.info("Listing all external_tables")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list external_tables.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW EXTERNAL_TABLES"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing external_tables: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_functions")
def show_functions(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all functions in Snowflake.
    
    Args:
        database: Optional database name to filter functions (e.g., 'MYDB')
        schema: Optional schema name to filter functions (e.g., 'PUBLIC')
    
    Returns:
        List of functions with their details
    """
    logger.info("Listing all functions")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list functions.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW FUNCTIONS"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing functions: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_grants")
def show_grants() -> List[Dict[str, Any]]:
    """
    List all grants in Snowflake.
    
    Returns:
        List of grants with their details
    """
    logger.info("Listing all grants")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list grants.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW GRANTS")
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing grants: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_integrations")
def show_integrations() -> List[Dict[str, Any]]:
    """
    List all integrations in Snowflake.
    
    Returns:
        List of integrations with their details
    """
    logger.info("Listing all integrations")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list integrations.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW INTEGRATIONS")
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing integrations: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_locks")
def show_locks() -> List[Dict[str, Any]]:
    """
    List all locks in Snowflake.
    
    Returns:
        List of locks with their details
    """
    logger.info("Listing all locks")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list locks.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW LOCKS")
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing locks: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_managed_accounts")
def show_managed_accounts() -> List[Dict[str, Any]]:
    """
    List all managed_accounts in Snowflake.
    
    Returns:
        List of managed_accounts with their details
    """
    logger.info("Listing all managed_accounts")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list managed_accounts.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW MANAGED_ACCOUNTS")
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing managed_accounts: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_materialized_views")
def show_materialized_views(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all materialized_views in Snowflake.
    
    Args:
        database: Optional database name to filter materialized views (e.g., 'MYDB')
        schema: Optional schema name to filter materialized views (e.g., 'PUBLIC')
    
    Returns:
        List of materialized_views with their details
    """
    logger.info("Listing all materialized_views")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list materialized_views.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW MATERIALIZED_VIEWS"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing materialized_views: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_network_policies")
def show_network_policies() -> List[Dict[str, Any]]:
    """
    List all network_policies in Snowflake.
    
    Returns:
        List of network_policies with their details
    """
    logger.info("Listing all network_policies")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list network_policies.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW NETWORK_POLICIES")
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing network_policies: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_objects")
def show_objects(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all objects in Snowflake.
    
    Args:
        database: Optional database name to filter objects (e.g., 'MYDB')
        schema: Optional schema name to filter objects (e.g., 'PUBLIC')
    
    Returns:
        List of objects with their details
    """
    logger.info("Listing all objects")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list objects.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW OBJECTS"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing objects: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_parameters")
def show_parameters(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all parameters in Snowflake.
    
    Args:
        database: Optional database name to filter parameters (e.g., 'MYDB')
        schema: Optional schema name to filter parameters (e.g., 'PUBLIC')
    
    Returns:
        List of parameters with their details
    """
    logger.info("Listing all parameters")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list parameters.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW PARAMETERS"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing parameters: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_pipes")
def show_pipes(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all pipes in Snowflake.
    
    Args:
        database: Optional database name to filter pipes (e.g., 'MYDB')
        schema: Optional schema name to filter pipes (e.g., 'PUBLIC')
    
    Returns:
        List of pipes with their details
    """
    logger.info("Listing all pipes")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list pipes.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW PIPES"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing pipes: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_procedures")
def show_procedures(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all procedures in Snowflake.
    
    Args:
        database: Optional database name to filter procedures (e.g., 'MYDB')
        schema: Optional schema name to filter procedures (e.g., 'PUBLIC')
    
    Returns:
        List of procedures with their details
    """
    logger.info("Listing all procedures")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list procedures.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW PROCEDURES"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing procedures: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_regions")
def show_regions() -> List[Dict[str, Any]]:
    """
    List all regions in Snowflake.
    
    Returns:
        List of regions with their details
    """
    logger.info("Listing all regions")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list regions.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW REGIONS")
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing regions: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_replication_databases")
def show_replication_databases() -> List[Dict[str, Any]]:
    """
    List all replication_databases in Snowflake.
    
    Returns:
        List of replication_databases with their details
    """
    logger.info("Listing all replication_databases")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list replication_databases.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW REPLICATION_DATABASES")
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing replication_databases: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_replication_groups")
def show_replication_groups() -> List[Dict[str, Any]]:
    """
    List all replication_groups in Snowflake.
    
    Returns:
        List of replication_groups with their details
    """
    logger.info("Listing all replication_groups")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list replication_groups.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW REPLICATION_GROUPS")
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing replication_groups: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_roles")
def show_roles() -> List[Dict[str, Any]]:
    """
    List all roles in Snowflake.
    
    Returns:
        List of roles with their details
    """
    logger.info("Listing all roles")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list roles.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW ROLES")
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing roles: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_schemas")
def show_schemas(database: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all schemas in Snowflake.
    
    Args:
        database: Optional database name to filter schemas (e.g., 'MYDB')
    
    Returns:
        List of schemas with their details
    """
    logger.info("Listing all schemas")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list schemas.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW SCHEMAS"
        if database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_sequences")
def show_sequences(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all sequences in Snowflake.
    
    Args:
        database: Optional database name to filter sequences (e.g., 'MYDB')
        schema: Optional schema name to filter sequences (e.g., 'PUBLIC')
    
    Returns:
        List of sequences with their details
    """
    logger.info("Listing all sequences")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list sequences.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW SEQUENCES"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing sequences: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_services")
def show_services(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all services in Snowflake.
    
    Args:
        database: Optional database name to filter services (e.g., 'MYDB')
        schema: Optional schema name to filter services (e.g., 'PUBLIC')
    
    Returns:
        List of services with their details
    """
    logger.info("Listing all services")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list services.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW SERVICES"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing services: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_shares")
def show_shares(database: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all shares in Snowflake.
    
    Args:
        database: Optional database name to filter shares (e.g., 'MYDB')
    
    Returns:
        List of shares with their details
    """
    logger.info("Listing all shares")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list shares.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW SHARES"
        if database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing shares: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_stages")
def show_stages(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all stages in Snowflake.
    
    Args:
        database: Optional database name to filter stages (e.g., 'MYDB')
        schema: Optional schema name to filter stages (e.g., 'PUBLIC')
    
    Returns:
        List of stages with their details
    """
    logger.info("Listing all stages")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list stages.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW STAGES"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing stages: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_streams")
def show_streams(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all streams in Snowflake.
    
    Args:
        database: Optional database name to filter streams (e.g., 'MYDB')
        schema: Optional schema name to filter streams (e.g., 'PUBLIC')
    
    Returns:
        List of streams with their details
    """
    logger.info("Listing all streams")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list streams.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW STREAMS"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing streams: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_tables")
def show_tables(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all tables in Snowflake.
    
    Args:
        database: Optional database name to filter tables (e.g., 'MYDB')
        schema: Optional schema name to filter tables (e.g., 'PUBLIC')
    
    Returns:
        List of tables with their details
    """
    logger.info("Listing all tables")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list tables.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW TABLES"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_tasks")
def show_tasks(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all tasks in Snowflake.
    
    Args:
        database: Optional database name to filter tasks (e.g., 'MYDB')
        schema: Optional schema name to filter tasks (e.g., 'PUBLIC')
    
    Returns:
        List of tasks with their details
    """
    logger.info("Listing all tasks")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list tasks.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW TASKS"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing tasks: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_transactions")
def show_transactions() -> List[Dict[str, Any]]:
    """
    List all transactions in Snowflake.
    
    Returns:
        List of transactions with their details
    """
    logger.info("Listing all transactions")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list transactions.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW TRANSACTIONS")
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing transactions: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_users")
def show_users() -> List[Dict[str, Any]]:
    """
    List all users in Snowflake.
    
    Returns:
        List of users with their details
    """
    logger.info("Listing all users")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list users.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW USERS")
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing users: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_views")
def show_views(database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all views in Snowflake.
    
    Args:
        database: Optional database name to filter views (e.g., 'MYDB')
        schema: Optional schema name to filter views (e.g., 'PUBLIC')
    
    Returns:
        List of views with their details
    """
    logger.info("Listing all views")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list views.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "SHOW VIEWS"
        if database and schema:
            query += f" IN {database}.{schema}"
        elif database:
            query += f" IN DATABASE {database}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing views: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("show_warehouses")
def show_warehouses() -> List[Dict[str, Any]]:
    """
    List all warehouses in Snowflake.
    
    Returns:
        List of warehouses with their details
    """
    logger.info("Listing all warehouses")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot list warehouses.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW WAREHOUSES")
        results = cursor.fetchall()
        
        # Convert to list of dictionaries with column names
        columns = [col[0] for col in cursor.description]
        return format_output([dict(zip(columns, row)) for row in results])
    except Exception as e:
        logger.error(f"Error listing warehouses: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_account")
def describe_account(name: str) -> Dict[str, Any]:
    """
    Get details about a specific account.
    
    Args:
        name: Name of the account to describe
        
    Returns:
        Detailed information about the account
    """
    logger.info(f"Describing account: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe account.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE ACCOUNT {name}")
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing account {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_alert")
def describe_alert(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific alert.
    
    Args:
        name: Name of the alert to describe
        database: Optional database name where the alert is located (e.g., 'MYDB')
        schema: Optional schema name where the alert is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the alert
    """
    logger.info(f"Describing alert: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe alert.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE ALERT "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing alert {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_application")
def describe_application(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific application.
    
    Args:
        name: Name of the application to describe
        database: Optional database name where the application is located (e.g., 'MYDB')
        schema: Optional schema name where the application is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the application
    """
    logger.info(f"Describing application: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe application.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE APPLICATION "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing application {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_application_role")
def describe_application_role(name: str, application_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific application_role.
    
    Args:
        name: Name of the application_role to describe
        application_name: Optional name of the application the role belongs to
        
    Returns:
        Detailed information about the application_role
    """
    logger.info(f"Describing application_role: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe application_role.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE APPLICATION_ROLE "
        if application_name:
            query += f"{application_name}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing application_role {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_column")
def describe_column(name: str, table: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific column.
    
    Args:
        name: Name of the column to describe
        table: Name of the table containing the column
        database: Optional database name where the table is located (e.g., 'MYDB')
        schema: Optional schema name where the table is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the column
    """
    logger.info(f"Describing column: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe column.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE COLUMN "
        if database and schema:
            query += f"{database}.{schema}.{table}.{name}"
        elif schema:
            query += f"{schema}.{table}.{name}"
        else:
            query += f"{table}.{name}"
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing column {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_connection")
def describe_connection(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific connection.
    
    Args:
        name: Name of the connection to describe
        database: Optional database name where the connection is located (e.g., 'MYDB')
        schema: Optional schema name where the connection is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the connection
    """
    logger.info(f"Describing connection: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe connection.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE CONNECTION "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing connection {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_database")
def describe_database(name: str) -> Dict[str, Any]:
    """
    Get details about a specific database.
    
    Args:
        name: Name of the database to describe
        
    Returns:
        Detailed information about the database
    """
    logger.info(f"Describing database: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe database.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE DATABASE "
        query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing database {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_external_function")
def describe_external_function(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific external_function.
    
    Args:
        name: Name of the external_function to describe
        database: Optional database name where the external function is located (e.g., 'MYDB')
        schema: Optional schema name where the external function is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the external_function
    """
    logger.info(f"Describing external_function: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe external_function.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE EXTERNAL_FUNCTION "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing external_function {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_external_table")
def describe_external_table(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific external_table.
    
    Args:
        name: Name of the external_table to describe
        database: Optional database name where the external table is located (e.g., 'MYDB')
        schema: Optional schema name where the external table is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the external_table
    """
    logger.info(f"Describing external_table: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe external_table.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE EXTERNAL_TABLE "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing external_table {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_function")
def describe_function(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific function.
    
    Args:
        name: Name of the function to describe
        database: Optional database name where the function is located (e.g., 'MYDB')
        schema: Optional schema name where the function is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the function
    """
    logger.info(f"Describing function: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe function.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE FUNCTION "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing function {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_integration")
def describe_integration(name: str) -> Dict[str, Any]:
    """
    Get details about a specific integration.
    
    Args:
        name: Name of the integration to describe
        
    Returns:
        Detailed information about the integration
    """
    logger.info(f"Describing integration: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe integration.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE INTEGRATION "
        query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing integration {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_materialized_view")
def describe_materialized_view(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific materialized_view.
    
    Args:
        name: Name of the materialized_view to describe
        database: Optional database name where the materialized view is located (e.g., 'MYDB')
        schema: Optional schema name where the materialized view is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the materialized_view
    """
    logger.info(f"Describing materialized_view: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe materialized_view.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE MATERIALIZED_VIEW "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing materialized_view {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_network_policy")
def describe_network_policy(name: str) -> Dict[str, Any]:
    """
    Get details about a specific network_policy.
    
    Args:
        name: Name of the network_policy to describe
        
    Returns:
        Detailed information about the network_policy
    """
    logger.info(f"Describing network_policy: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe network_policy.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE NETWORK_POLICY {name}")
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing network_policy {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_pipe")
def describe_pipe(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific pipe.
    
    Args:
        name: Name of the pipe to describe
        database: Optional database name where the pipe is located (e.g., 'MYDB')
        schema: Optional schema name where the pipe is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the pipe
    """
    logger.info(f"Describing pipe: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe pipe.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE PIPE "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing pipe {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_procedure")
def describe_procedure(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific procedure.
    
    Args:
        name: Name of the procedure to describe
        database: Optional database name where the procedure is located (e.g., 'MYDB')
        schema: Optional schema name where the procedure is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the procedure
    """
    logger.info(f"Describing procedure: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe procedure.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE PROCEDURE "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing procedure {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_replication_database")
def describe_replication_database(name: str) -> Dict[str, Any]:
    """
    Get details about a specific replication_database.
    
    Args:
        name: Name of the replication_database to describe
        
    Returns:
        Detailed information about the replication_database
    """
    logger.info(f"Describing replication_database: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe replication_database.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE REPLICATION_DATABASE {name}")
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing replication_database {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_replication_group")
def describe_replication_group(name: str) -> Dict[str, Any]:
    """
    Get details about a specific replication_group.
    
    Args:
        name: Name of the replication_group to describe
        
    Returns:
        Detailed information about the replication_group
    """
    logger.info(f"Describing replication_group: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe replication_group.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE REPLICATION_GROUP {name}")
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing replication_group {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_role")
def describe_role(name: str) -> Dict[str, Any]:
    """
    Get details about a specific role.
    
    Args:
        name: Name of the role to describe
        
    Returns:
        Detailed information about the role
    """
    logger.info(f"Describing role: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe role.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE ROLE "
        query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing role {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_schema")
def describe_schema(name: str, database: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific schema.
    
    Args:
        name: Name of the schema to describe
        database: Optional database name where the schema is located
        
    Returns:
        Detailed information about the schema
    """
    logger.info(f"Describing schema: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe schema.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE SCHEMA "
        if database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing schema {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_sequence")
def describe_sequence(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific sequence.
    
    Args:
        name: Name of the sequence to describe
        database: Optional database name where the sequence is located (e.g., 'MYDB')
        schema: Optional schema name where the sequence is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the sequence
    """
    logger.info(f"Describing sequence: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe sequence.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE SEQUENCE "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing sequence {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_service")
def describe_service(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific service.
    
    Args:
        name: Name of the service to describe
        database: Optional database name where the service is located (e.g., 'MYDB')
        schema: Optional schema name where the service is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the service
    """
    logger.info(f"Describing service: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe service.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE SERVICE "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing service {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_share")
def describe_share(name: str) -> Dict[str, Any]:
    """
    Get details about a specific share.
    
    Args:
        name: Name of the share to describe
        
    Returns:
        Detailed information about the share
    """
    logger.info(f"Describing share: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe share.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE SHARE "
        query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing share {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_stage")
def describe_stage(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific stage.
    
    Args:
        name: Name of the stage to describe
        database: Optional database name where the stage is located (e.g., 'MYDB')
        schema: Optional schema name where the stage is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the stage
    """
    logger.info(f"Describing stage: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe stage.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE STAGE "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing stage {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_stream")
def describe_stream(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific stream.
    
    Args:
        name: Name of the stream to describe
        database: Optional database name where the stream is located (e.g., 'MYDB')
        schema: Optional schema name where the stream is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the stream
    """
    logger.info(f"Describing stream: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe stream.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE STREAM "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing stream {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)



@server.tool("describe_table")
def describe_table(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific table.
    
    Args:
        name: Name of the table to describe
        database: Optional database name where the table is located (e.g., 'MYDB')
        schema: Optional schema name where the table is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the table
    """
    logger.info(f"Describing table: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe table.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE TABLE "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing table {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)


@server.tool("describe_task")
def describe_task(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific task.
    
    Args:
        name: Name of the task to describe
        database: Optional database name where the task is located (e.g., 'MYDB')
        schema: Optional schema name where the task is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the task
    """
    logger.info(f"Describing task: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe task.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE TASK "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing task {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)


@server.tool("describe_user")
def describe_user(name: str) -> Dict[str, Any]:
    """
    Get details about a specific user.
    
    Args:
        name: Name of the user to describe
        
    Returns:
        Detailed information about the user
    """
    logger.info(f"Describing user: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe user.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE USER "
        query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing user {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)


@server.tool("describe_view")
def describe_view(name: str, database: Optional[str] = None, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Get details about a specific view.
    
    Args:
        name: Name of the view to describe
        database: Optional database name where the view is located (e.g., 'MYDB')
        schema: Optional schema name where the view is located (e.g., 'PUBLIC')
        
    Returns:
        Detailed information about the view
    """
    logger.info(f"Describing view: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe view.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE VIEW "
        if database and schema:
            query += f"{database}.{schema}.{name}"
        elif database:
            query += f"{database}.{name}"
        else:
            query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing view {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)


@server.tool("describe_warehouse")
def describe_warehouse(name: str) -> Dict[str, Any]:
    """
    Get details about a specific warehouse.
    
    Args:
        name: Name of the warehouse to describe
        
    Returns:
        Detailed information about the warehouse
    """
    logger.info(f"Describing warehouse: {name}")
    
    if not SNOWFLAKE_AVAILABLE:
        raise ImportError("Snowflake connector is not installed. Cannot describe warehouse.")
    
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        query = "DESCRIBE WAREHOUSE "
        query += name
            
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to dictionary with column names
        columns = [col[0] for col in cursor.description]
        return format_output(dict(zip(columns, results[0])))
    except Exception as e:
        logger.error(f"Error describing warehouse {name}: {e}")
        raise
    finally:
        return_connection_to_pool(conn)


@server.tool("server_info")
def server_info() -> Dict[str, Any]:
    """
    Get detailed information about the Snowflake MCP server and its connection status.

    Returns:
        Comprehensive server configuration and connection information including
        active session details, port configuration, and connection status.
    """
    logger.info("Getting server info")

    # Basic server information
    info = {
        "server_name": "Snowflake MCP Server",
        "version": "1.0.0",
        "timestamp": datetime.datetime.now().isoformat(),
        "port_configuration": {
            "fastapi_port": int(os.environ.get("FASTAPI_PORT", "8090")),
            "mcp_port": int(os.environ.get("PORT", "8091")),
        },
        "snowflake_available": SNOWFLAKE_AVAILABLE,
        "allow_write": args.allow_write,
    }

    # Add Snowflake connection info if available
    if SNOWFLAKE_AVAILABLE:
        # Basic connection configuration from environment
        info["snowflake_configuration"] = {
            "account": SNOWFLAKE_ACCOUNT,
            "user": SNOWFLAKE_USER,
            "role": SNOWFLAKE_ROLE,
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA,
        }

        # Try to get active session details
        try:
            conn = get_snowflake_connection()
            try:
                cursor = conn.cursor()

                # Get current session details
                cursor.execute("""
                SELECT 
                    CURRENT_ACCOUNT(),
                    CURRENT_USER(),
                    CURRENT_ROLE(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_DATABASE(),
                    CURRENT_SCHEMA(),
                    CURRENT_SESSION()
                """)

                account, user, role, warehouse, database, schema, session_id = (
                    cursor.fetchone()
                )

                # Get warehouse status if available
                warehouse_status = "Not available"
                warehouse_size = "Not available"
                try:
                    if warehouse:
                        cursor.execute(f"SHOW WAREHOUSES LIKE '{warehouse}'")
                        warehouse_info = cursor.fetchone()
                        if warehouse_info:
                            warehouse_status = warehouse_info[3]  # State column
                            warehouse_size = warehouse_info[2]  # Size column
                except Exception as e:
                    logger.warning(f"Could not get warehouse status: {e}")

                # Add active session information
                info["active_session"] = {
                    "account": account,
                    "user": user,
                    "role": role,
                    "warehouse": warehouse,
                    "warehouse_status": warehouse_status,
                    "warehouse_size": warehouse_size,
                    "database": database,
                    "schema": schema,
                    "session_id": session_id,
                }

                # Get role permissions summary
                try:
                    cursor.execute("SHOW GRANTS TO ROLE " + role)
                    grants = cursor.fetchall()

                    # Summarize permissions
                    permission_summary = {
                        "databases": set(),
                        "warehouses": set(),
                        "schemas": set(),
                        "has_warehouse_usage": False,
                    }

                    for grant in grants:
                        privilege = grant[1].upper() if grant[1] else ""
                        granted_on = grant[2].upper() if grant[2] else ""
                        name = grant[3] if grant[3] else ""

                        if granted_on == "DATABASE":
                            permission_summary["databases"].add(name)
                        elif granted_on == "WAREHOUSE":
                            permission_summary["warehouses"].add(name)
                            if privilege == "USAGE":
                                permission_summary["has_warehouse_usage"] = True
                        elif granted_on == "SCHEMA":
                            permission_summary["schemas"].add(name)

                    # Convert sets to lists for JSON serialization
                    permission_summary["databases"] = list(
                        permission_summary["databases"]
                    )
                    permission_summary["warehouses"] = list(
                        permission_summary["warehouses"]
                    )
                    permission_summary["schemas"] = list(permission_summary["schemas"])

                    info["role_permissions"] = permission_summary
                except Exception as e:
                    logger.warning(f"Could not get role permissions: {e}")
                    info["role_permissions"] = {"error": str(e)}

            except Exception as e:
                logger.warning(f"Error getting session details: {e}")
                info["session_error"] = str(e)
            finally:
                return_connection_to_pool(conn)
        except Exception as e:
            logger.warning(f"Could not connect to Snowflake: {e}")
            info["connection_error"] = str(e)
    else:
        info["test_mode"] = True
        info["message"] = "Running in test mode without Snowflake connector"

    return format_output(info)


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Snowflake MCP Server")
    parser.add_argument(
        "--allow-write",
        action="store_true",
        help="Allow write operations (not recommended for production)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8090,
        help="Port to run the FastAPI app on (default: 8090 to avoid conflicts)",
    )
    parser.add_argument(
        "--mcp-port",
        type=int,
        default=8091,
        help="Port for the MCP server (default: 8091 to avoid conflicts)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set logging level (default: WARNING to silence most logs)",
    )
    parser.add_argument(
        "--auth-method",
        type=str,
        default="password",
        choices=["password", "oauth"],
        help="Authentication method to use (default: password)",
    )
    parser.add_argument(
        "--oauth-client-id",
        type=str,
        help="OAuth client ID for Snowflake authentication",
    )
    parser.add_argument(
        "--oauth-client-secret",
        type=str,
        help="OAuth client secret for Snowflake authentication",
    )
    parser.add_argument(
        "--oauth-redirect-uri",
        type=str,
        default="http://localhost:8090/oauth/callback",
        help="OAuth redirect URI for Snowflake authentication",
    )
    parser.add_argument(
        "--oauth-token-cache-file",
        type=str,
        help="File to cache OAuth tokens (default: oauth_tokens.json in the same directory as the script)",
    )

    # Parse command line arguments into our global args
    parsed_args = parser.parse_args()
    args.allow_write = parsed_args.allow_write
    args.port = parsed_args.port
    args.mcp_port = parsed_args.mcp_port
    args.log_level = parsed_args.log_level
    args.auth_method = parsed_args.auth_method
    args.oauth_client_id = parsed_args.oauth_client_id
    args.oauth_client_secret = parsed_args.oauth_client_secret
    args.oauth_redirect_uri = parsed_args.oauth_redirect_uri
    
    # Set the OAuth token cache file if provided, otherwise use default
    if parsed_args.oauth_token_cache_file:
        args.oauth_token_cache_file = parsed_args.oauth_token_cache_file
    
    # Update logging level based on command line argument
    logger.setLevel(getattr(logging, args.log_level))
    # Set all other loggers to WARNING to silence them
    for log_name, log_obj in logging.Logger.manager.loggerDict.items():
        if isinstance(log_obj, logging.Logger) and log_name != "snowflake_mcp_server":
            log_obj.setLevel(logging.WARNING)

    # Update server configuration based on command line arguments
    # Always use the specified MCP port
    port = args.mcp_port
    os.environ["PORT"] = str(port)
    logger.info(f"Setting MCP server port to {port}")

    # Set FastAPI port separately
    os.environ["FASTAPI_PORT"] = str(args.port)
    logger.info(f"Setting FastAPI app port to {args.port}")

    # Re-initialize the server with command-line arguments
    server = FastMCP(
        name="Snowflake MCP Server",
        description="A server that provides access to Snowflake data through MCP",
        allow_write=args.allow_write,  # Set from command line arguments
        port=port,
    )
    
    # Server has been initialized with updated args

    if args.allow_write:
        logger.warning(
            "Write operations are enabled. This is not recommended for production use."
        )

    # Start the server
    logger.info(
        f"Running Snowflake MCP server on port {os.environ.get('PORT', '8091')}"
    )
    logger.info(f"FastAPI app running on port {os.environ.get('FASTAPI_PORT', '8090')}")
    logger.info(
        "Use 'fastmcp dev snowflake_mcp_inspector.py' to connect with the MCP Inspector"
    )
    logger.info(
        "For Windsurf integration, ensure mcp_config.json specifies the full Python path and matching ports"
    )

    if SNOWFLAKE_AVAILABLE:
        logger.info(
            "Snowflake connector is available. Server can connect to Snowflake."
        )
    else:
        logger.warning(
            "Snowflake connector is not available. Running in test mode only."
        )
