# Snowflake MCP Server for Windsurf

A Model Context Protocol (MCP) Server implementation that provides database interaction with Snowflake, tailored for Windsurf. This server enables running SQL queries and retrieving metadata from Snowflake databases through a standardized protocol interface. Built using the `@modelcontextprotocol/sdk` package.

## Features

- **MCP SDK Integration**: Built using the official `@modelcontextprotocol/sdk` package
- **Stdio Transport**: Implements the required transport mechanism for Windsurf
- **Keypair Authentication**: Supports secure connection to Snowflake using private key authentication
- **Metadata Exposure**: Provides comprehensive metadata needed for generating SQL & Python in Windsurf
- **Query Execution**: Enables running SQL queries on Snowflake and returning structured results
- **Insights Memo**: Maintains a memo of data insights discovered during analysis
- **Structured Logging**: Uses Winston for comprehensive logging with configurable levels
- **Error Handling**: Robust error handling with helpful suggestions for troubleshooting
- **Schema Validation**: Uses Zod for input validation of tool parameters

## Tools

### Query Tools
- `read_query`: Execute SELECT queries to read data from Snowflake
- `write_query`: Execute INSERT, UPDATE, or DELETE queries in Snowflake
- `create_table`: Create new tables in Snowflake
- `execute_ddl`: Execute DDL statements like CREATE VIEW, ALTER TABLE, etc.

### Schema Tools
- `list_databases`: Get a list of all accessible databases in Snowflake
- `list_schemas`: Get a list of all schemas in the current or specified database
- `list_tables`: Get a list of all tables in the current schema or specified schema
- `describe_table`: View column information for a specific table
- `get_query_history`: Retrieve recent query history for the current user
- `get_user_roles`: Get all roles assigned to the current user
- `get_table_sample`: Get a sample of data from a table

### Analysis Tools
- `append_insight`: Add new data insights to the memo resource with optional categorization
- `clear_insights`: Clear all insights from the memo resource

## Resources

- `memo://insights`: A continuously updated data insights memo that aggregates discovered insights during analysis
- `snowflake://metadata/databases`: List of all accessible Snowflake databases
- `snowflake://metadata/schemas`: List of all schemas in the current database
- `snowflake://metadata/tables`: List of all tables in the current schema
- `snowflake://metadata/user_info`: Information about the current Snowflake user, including roles and privileges

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/snowflake-mcp-server.git
cd snowflake-mcp-server
```

2. Install dependencies:
```bash
npm install
```

This will install all required dependencies including `@modelcontextprotocol/sdk`, `snowflake-sdk`, `winston`, and `zod`.

3. Configure your Snowflake connection:
```bash
cp .env.example .env
```

4. Edit the `.env` file with your Snowflake credentials:
```
# Snowflake Connection Parameters
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USERNAME=your_username
SNOWFLAKE_PASSWORD=your_password  # Optional if using private key
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role

# Optional: Path to private key for keypair authentication
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/your/private/key.p8

# Logging Configuration
LOG_LEVEL=info  # Options: error, warn, info, http, verbose, debug, silly
```

## Usage

### Starting the Server

Start the server in development mode:
```bash
npm run dev
```

Start the server in production mode:
```bash
npm start
```

### Using with Windsurf

To use this server with Windsurf:

1. Start the server as described above
2. Configure Windsurf to connect to this MCP server
3. Use the provided tools and resources in your Windsurf sessions

### Example Tool Usage

#### Running a SELECT Query

```json
{
  "name": "read_query",
  "args": {
    "query": "SELECT * FROM my_table LIMIT 10"
  }
}
```

#### Adding an Insight

```json
{
  "name": "append_insight",
  "args": {
    "insight": "The sales data shows a 15% increase in Q4 compared to Q3",
    "category": "Sales Analysis"
  }
}
```

## Configuration

### Environment Variables

The server can be configured using the following environment variables:

| Variable | Description | Required | Default |
|----------|-------------|----------|--------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | Yes | - |
| `SNOWFLAKE_USERNAME` | Snowflake username | Yes | - |
| `SNOWFLAKE_PASSWORD` | Snowflake password | No* | - |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse | Yes | - |
| `SNOWFLAKE_DATABASE` | Default database | Yes | - |
| `SNOWFLAKE_SCHEMA` | Default schema | Yes | - |
| `SNOWFLAKE_ROLE` | Snowflake role | Yes | - |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | Path to private key file | No* | - |
| `LOG_LEVEL` | Logging level | No | info |

*Either `SNOWFLAKE_PASSWORD` or `SNOWFLAKE_PRIVATE_KEY_PATH` must be provided.

## Development

### Running Tests

Run all tests:
```bash
npm test
```

Run tests with coverage:
```bash
npm run test:coverage
```

### Linting

Lint the codebase:
```bash
npm run lint
```

Fix linting issues automatically:
```bash
npm run lint:fix
```

### Code Formatting

Format the code using Prettier:
```bash
npm run format
```

## Continuous Integration

This project uses GitHub Actions for continuous integration. The CI pipeline runs on each push and pull request to the main branch, performing the following checks:

- Linting with ESLint
- Unit and integration tests with Mocha
- Code coverage reporting with c8

## Testing with MCP Inspector

You can test the server using the [MCP Inspector](https://github.com/modelcontextprotocol/inspector):

```bash
npx @modelcontextprotocol/inspector
```

Then connect to your running server and explore the available tools and resources.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT
