# Snowflake MCP Server for Windsurf

A Model Context Protocol (MCP) Server implementation that provides database interaction with Snowflake, tailored for Windsurf. This server enables running SQL queries and retrieving metadata from Snowflake databases.

## Features

- **Stdio Transport**: Implements the required transport mechanism for Windsurf
- **Keypair Authentication**: Supports secure connection to Snowflake using private key authentication
- **Metadata Exposure**: Provides comprehensive metadata needed for generating SQL & Python in Windsurf
- **Query Execution**: Enables running SQL queries on Snowflake and returning structured results
- **Insights Memo**: Maintains a memo of data insights discovered during analysis

## Tools

### Query Tools
- `read_query`: Execute SELECT queries to read data from Snowflake
- `write_query`: Execute INSERT, UPDATE, or DELETE queries in Snowflake
- `create_table`: Create new tables in Snowflake

### Schema Tools
- `list_databases`: Get a list of all accessible databases in Snowflake
- `list_schemas`: Get a list of all schemas in the current or specified database
- `list_tables`: Get a list of all tables in the current schema or specified schema
- `describe_table`: View column information for a specific table
- `get_query_history`: Retrieve recent query history for the current user
- `get_user_roles`: Get all roles assigned to the current user
- `get_table_sample`: Get a sample of data from a table

### Analysis Tools
- `append_insight`: Add new data insights to the memo resource

## Resources

- `memo://insights`: A continuously updated data insights memo that aggregates discovered insights during analysis
- `snowflake://metadata/databases`: List of all accessible Snowflake databases
- `snowflake://metadata/schemas`: List of all schemas in the current database
- `snowflake://metadata/tables`: List of all tables in the current schema
- `snowflake://metadata/user_info`: Information about the current Snowflake user, including roles and privileges

## Installation

1. Clone this repository:
```
git clone https://github.com/yourusername/snowflake-mcp-server.git
cd snowflake-mcp-server
```

2. Install dependencies:
```
npm install
```

3. Configure your Snowflake connection:
```
cp .env.example .env
```
Edit the `.env` file with your Snowflake credentials.

## Usage

Start the server:
```
npm start
```

## Testing with MCP Inspector

You can test the server using the [MCP Inspector](https://github.com/modelcontextprotocol/inspector).

## License

MIT
