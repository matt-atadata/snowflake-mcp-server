# Snowflake OAuth Authentication Guide

This guide explains how to set up and use OAuth authentication with the Snowflake MCP Server.

## Overview

OAuth authentication provides a more secure way to connect to Snowflake by:
- Eliminating the need to store passwords in configuration files or environment variables
- Supporting Single Sign-On (SSO) through your identity provider
- Providing token-based authentication with automatic refresh capabilities
- Enforcing stronger security controls through token expiration

## Prerequisites

Before setting up OAuth authentication, ensure you have:

1. A Snowflake account with administrator privileges (ACCOUNTADMIN role) or a role with CREATE INTEGRATION privilege
2. Access to execute SQL commands in Snowflake
3. The Snowflake MCP Server installed and configured

## Step 1: Create a Security Integration in Snowflake

Only Snowflake account administrators or users with the CREATE INTEGRATION privilege can execute this step.

Execute the following SQL command in Snowflake to create a security integration:

```sql
CREATE OR REPLACE SECURITY INTEGRATION SNOWFLAKE_MCP_SERVER
  TYPE = OAUTH
  ENABLED = TRUE
  OAUTH_CLIENT = CUSTOM
  OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'
  OAUTH_REDIRECT_URI = 'http://localhost:8090/oauth/callback'
  OAUTH_ISSUE_REFRESH_TOKENS = TRUE
  OAUTH_USE_SECONDARY_ROLES = IMPLICIT
  OAUTH_REFRESH_TOKEN_VALIDITY = 7776000;
```

> **Note**: The `OAUTH_REDIRECT_URI` must match the redirect URI configured in your Snowflake MCP Server. The default is `http://localhost:8090/oauth/callback`.

## Step 2: Retrieve Client ID and Secret

After creating the security integration, retrieve the Client ID and Secret by executing:

```sql
WITH integration_secrets AS (
  SELECT parse_json(system$show_oauth_client_secrets('SNOWFLAKE_MCP_SERVER')) AS secrets
)

SELECT
  secrets:"OAUTH_CLIENT_ID"::string     AS client_id,
  secrets:"OAUTH_CLIENT_SECRET"::string AS client_secret
FROM 
  integration_secrets;
```

Save these values securely as you'll need them for the next step.

## Step 3: Configure the Snowflake MCP Server

Set the following environment variables or provide them as command-line arguments when starting the server:

```bash
# Required environment variables
export SNOWFLAKE_ACCOUNT="your_account_identifier"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_ROLE="your_role"
export SNOWFLAKE_CLIENT_ID="client_id_from_step_2"
export SNOWFLAKE_CLIENT_SECRET="client_secret_from_step_2"

# Optional environment variables
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema"
```

Alternatively, you can provide these values as command-line arguments:

```bash
python snowflake_mcp_server.py \
  --oauth-client-id "client_id_from_step_2" \
  --oauth-client-secret "client_secret_from_step_2" \
  --oauth-redirect-uri "http://localhost:8090/oauth/callback" \
  --oauth-token-cache-file "oauth_tokens.json"
```

## Step 4: Start the Server and Authenticate

1. Start the Snowflake MCP Server:
   ```bash
   python snowflake_mcp_server.py --port 8090 --mcp-port 8091
   ```

2. The first time you connect to Snowflake, the server will initiate the OAuth flow:
   - A browser window will open automatically
   - You'll be redirected to the Snowflake login page
   - After successful authentication, you'll be redirected back to the server
   - The server will store the OAuth tokens for future use

3. Once authenticated, the server will use the stored tokens for subsequent connections to Snowflake.

## Managing OAuth Tokens

The Snowflake MCP Server provides the `oauth_manager` tool to manage OAuth tokens:

- **Check token status**: 
  ```bash
  curl -X POST http://localhost:8090/api/v1/invoke -H "Content-Type: application/json" -d '{"name": "oauth_manager", "arguments": {"action": "status"}}'
  ```

- **Start OAuth flow manually**:
  ```bash
  curl -X POST http://localhost:8090/api/v1/invoke -H "Content-Type: application/json" -d '{"name": "oauth_manager", "arguments": {"action": "start"}}'
  ```

- **Refresh tokens**:
  ```bash
  curl -X POST http://localhost:8090/api/v1/invoke -H "Content-Type: application/json" -d '{"name": "oauth_manager", "arguments": {"action": "refresh"}}'
  ```

- **Clear stored tokens**:
  ```bash
  curl -X POST http://localhost:8090/api/v1/invoke -H "Content-Type: application/json" -d '{"name": "oauth_manager", "arguments": {"action": "clear"}}'
  ```

## Security Considerations

1. **Token Storage**: OAuth tokens are stored in the file specified by `--oauth-token-cache-file`. Ensure this file has appropriate permissions.

2. **Client Secret**: The client secret should be treated as a sensitive credential. Do not expose it in public repositories or insecure locations.

3. **Token Expiration**: Access tokens typically expire after a short period (e.g., 1 hour). The server will automatically refresh tokens when needed.

4. **Refresh Token Validity**: The `OAUTH_REFRESH_TOKEN_VALIDITY` parameter in the security integration determines how long refresh tokens are valid. After this period, users will need to re-authenticate.

## Troubleshooting

1. **Authentication Failures**:
   - Verify that the client ID and secret are correct
   - Ensure the redirect URI matches the one configured in the security integration
   - Check that your Snowflake user has the necessary permissions

2. **Token Refresh Issues**:
   - If tokens cannot be refreshed, clear the stored tokens and re-authenticate
   - Verify that `OAUTH_ISSUE_REFRESH_TOKENS` is set to TRUE in the security integration

3. **Connection Problems**:
   - Use the `server_info` tool to check the server configuration and connection status
   - Verify that the required environment variables are set correctly

## Additional Resources

- [Snowflake OAuth Documentation](https://docs.snowflake.com/en/user-guide/oauth-custom.html)
- [Snowflake Security Integrations](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration.html)
