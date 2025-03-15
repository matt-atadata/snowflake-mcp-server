# Snowflake MCP Server (Python Implementation)

A Model Context Protocol (MCP) server for Snowflake using FastMCP. This server provides access to Snowflake data through a standardized interface that can be used by LLM applications like Claude, Windsurf, and other MCP-compatible clients.

## Features

- **Native MCP Implementation**: Clean, simplified implementation using FastMCP without FastAPI dependencies
- **Snowflake Integration**: Execute SQL queries against Snowflake and explore database structure
- **MCP Inspector Support**: Built-in support for the FastMCP Inspector for testing and debugging
- **Secure**: Uses environment variables for sensitive credentials
- **Modular Design**: Clean separation of concerns for easy maintenance and extension
- **Wide Client Compatibility**: Works with various MCP clients including Claude Desktop, Windsurf, and other MCP-compatible applications

## Prerequisites

- Python 3.8+
- Snowflake account with appropriate credentials
- FastMCP library

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/snowflake-mcp-server.git
   cd snowflake-mcp-server/python_implementation
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create a `.env` file with your Snowflake credentials:
   ```
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_ROLE=your_role
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_SCHEMA=your_schema
   ```

## Usage

### Running the Snowflake MCP Server

You can run the server in two modes:

#### 1. Standalone Mode

This mode runs just the Snowflake MCP server:

```bash
python snowflake_mcp_server.py [--allow-write] [--port PORT]
```

Options:
- `--allow-write`: Allow write operations (not recommended for production)
- `--port`: Port for the MCP server (default: 8001)

#### 2. FastMCP Inspector Mode

This mode runs the Snowflake MCP server with the FastMCP Inspector for testing and debugging:

```bash
./run_snowflake_inspector.sh
```

This will:
1. Check and kill any processes using ports 3000, 5173, and 8001
2. Set the `FASTMCP_PYTHON_COMMAND=python` environment variable to avoid `uv` errors
3. Start the MCP server on port 8001
4. Launch the FastMCP Inspector UI at http://localhost:5173

The Inspector UI provides a user-friendly interface to:
- View available tools and resources
- Test tool execution
- Explore server capabilities
- Debug MCP interactions

## Available Tools

The Snowflake MCP server provides the following tools:

### 1. Query Tool

Execute SQL queries against Snowflake:

```
execute_query(query: str, limit_rows: int = 1000) -> QueryResult
```

Example:
```sql
SELECT * FROM my_database.my_schema.my_table LIMIT 10
```

### 2. Schema Tools

Explore the Snowflake database structure:

```
list_databases() -> List[str]
list_schemas(database: str) -> List[str]
list_tables(database: str, schema: str) -> List[TableInfo]
get_table_schema(database_name: str, schema_name: str, table_name: str) -> TableDescription
```

### 3. Server Info Tool

Get information about the server:

```
server_info() -> Dict[str, Any]
```

## Project Structure

```
snowflake-mcp-server/
├── python_implementation/
│   ├── .env                         # Environment variables (create this file)
│   ├── .env.example                 # Example environment variables
│   ├── requirements.txt             # Python dependencies
│   ├── setup.py                     # Package setup
│   ├── README.md                    # This documentation
│   ├── snowflake_mcp_server.py      # Main Snowflake MCP server implementation
│   ├── snowflake_mcp_inspector.py   # Entry point for the MCP Inspector
│   ├── run_snowflake_inspector.sh   # Script to run the MCP Inspector
│   ├── archive/                     # Archived files (old implementations)
│   └── snowflake_mcp/               # Package directory
```

## Development

### Adding New Tools

To add a new tool to the server, add a new function in the `snowflake_mcp_server.py` file and decorate it with `@server.tool()`:

```python
@server.tool("my_new_tool")
def my_new_tool(param1: str, param2: int) -> Dict[str, Any]:
    """
    Description of my new tool.
    
    Args:
        param1: Description of param1
        param2: Description of param2
    
    Returns:
        Description of the return value
    """
    # Implementation
    return {"result": "success"}
```

## Integrating with Windsurf

To use this server with Windsurf:

1. Configure Windsurf to use this server in the MCP configuration:
   ```json
   {
     "name": "Snowflake MCP",
     "command": "/path/to/python",
     "args": ["/path/to/snowflake-mcp-server/python_implementation/snowflake_mcp_server.py", "--port", "8091"],
     "env": {},
     "mcp_url": "http://localhost:8091"
   }
   ```

2. Make sure to use non-default ports (e.g., 8091) to avoid conflicts with other services.

3. Ensure the full path to Python is specified (use `which python` to find it).

#### Installation and Configuration

1. **Start by configuring your Snowflake credentials**:

   Create a `.env` file in the `python_implementation` directory with your Snowflake credentials:

   ```
   SNOWFLAKE_ACCOUNT=your_account_id
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_ROLE=your_role
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_SCHEMA=your_schema
   ```

2. **Configure MCP Servers in Windsurf**:

   a. Open Windsurf
   
   b. Open Cascade by pressing `⌘L`
   
   c. Access the MCP configuration by clicking the hammer icon (🔨) in the toolbar above Cascade, then click "Configure"
   
   d. This opens `~/.codeium/windsurf/mcp_config.json` in the editor
   
   e. Add the Snowflake MCP server configuration:

   ```json
   {
     "mcpServers": {
       "snowflake": {
         "command": "/path/to/python/venv/bin/python",
         "args": [
           "/path/to/snowflake-mcp-server/python_implementation/snowflake_server.py",
           "--port", "9090",
           "--mcp-port", "8000"
         ],
         "env": {
           "PORT": "8000"
         },
         "cwd": "/path/to/snowflake-mcp-server/python_implementation"
       }
     }
   }
   ```

   > **Important**: Use the full path to your Python executable, as Windsurf may not inherit your shell's PATH environment. You can find your Python path by running `which python` in your terminal.

   > **Note**: Replace `/path/to/snowflake-mcp-server` with the actual path to your cloned repository

3. **Save the configuration file**

4. **Click "Refresh" (🔄) in the MCP toolbar in Cascade**

   If successful, you should see "1 available MCP server" (or more if you have other servers configured)

### Using Snowflake MCP Server with Cascade

Once configured, you can interact with your Snowflake data using natural language through Cascade. Here are some example interactions:

#### Example: Exploring Database Schema

```
User: List all tables in my Snowflake database

Cascade: I'll help you list all the tables in your Snowflake database.
> MCP Tool: snowflake / list_tables
> I found the following tables in your Snowflake database:
> - customers (in the public schema)
> - orders (in the public schema)
> - products (in the public schema)
> Would you like to see more details about any of these tables?

User: Show me the schema of the customers table

Cascade: I'll retrieve the schema information for the customers table.
> MCP Tool: snowflake / describe_table
> Here's the schema for the customers table:
> - customer_id: INTEGER (Primary Key)
> - name: VARCHAR(100)
> - email: VARCHAR(100)
> - signup_date: DATE
> - last_order_date: DATE
> Is there anything specific you'd like to know about this table?
```

#### Example: Running a Query

```
User: Find the top 5 customers by order value

Cascade: I'll run a query to find the top 5 customers by total order value.
> MCP Tool: snowflake / read_query
> Here are the top 5 customers by order value:
> | customer_id | name | total_order_value |
> |------------|------|-------------------|
> | 1042 | John Smith | $12,450.75 |
> | 985 | Sarah Johnson | $8,720.50 |
> | 1137 | Michael Brown | $7,890.25 |
> | 763 | Emily Davis | $6,540.80 |
> | 1256 | Robert Wilson | $5,980.15 |
> Would you like to see more information about any of these customers?
```

### Available Tools

The Snowflake MCP server exposes the following tools for interacting with your Snowflake database:

#### Query Tools
- `read_query`: Execute SELECT queries to read data
- `write_query`: Execute INSERT, UPDATE, or DELETE queries (only with --allow-write flag)
- `create_table`: Create new tables (only with --allow-write flag)

#### Schema Tools
- `list_tables`: Get a list of all tables in the database
- `describe_table`: View column information for a specific table

#### Analysis Tools
- `append_insight`: Add new data insights to the memo resource

### Security Considerations

- Keep your Snowflake credentials secure and never share them publicly
- Consider using the least privileged role necessary for your tasks
- For production use, consider implementing additional authentication mechanisms

## Claude Desktop Integration

To use this server with Claude Desktop, add the following to your Claude Desktop configuration file:

```json
"mcpServers": {
  "snowflake_local": {
      "command": "python",
      "args": [
          "/absolute/path/to/snowflake_server.py",
          "--port",
          "8080",
          "--mcp-port",
          "8000",
          "--account",
          "the_account",
          "--warehouse",
          "the_warehouse",
          "--user",
          "the_user",
          "--password",
          "their_password",
          "--role",
          "the_role",
          "--database",
          "the_database",
          "--schema",
          "the_schema"
          # Optionally: "--allow-write" (but not recommended)
      ]
  }
}
```

Note: The server uses a dual-server approach with FastAPI serving the index page on port 8080 and FastMCP handling the Model Context Protocol on port 8000.
