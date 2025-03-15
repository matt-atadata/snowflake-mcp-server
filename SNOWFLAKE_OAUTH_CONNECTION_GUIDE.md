# Snowflake OAuth Connection Guide

This guide provides detailed instructions for setting up and using OAuth authentication with Snowflake in the MCP Server. It incorporates best practices from Paradime's documentation and enterprise OAuth configurations.

## Overview

OAuth provides several advantages over traditional username/password authentication:

- **Enhanced Security**: No need to store passwords in configuration files or environment variables
- **Single Sign-On (SSO)**: Seamless integration with your organization's identity provider
- **Automated Token Refresh**: Tokens are automatically refreshed without user intervention
- **Granular Access Control**: Precise control over permissions and token lifetimes

## Step 1: Create a Security Integration in Snowflake

Only Snowflake account administrators (users with the ACCOUNTADMIN role) or users with the global CREATE INTEGRATION privilege can perform this step.

Execute the following SQL command in Snowflake:

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

### Important Parameters:

| Parameter | Description |
|-----------|-------------|
| `TYPE` | Must be set to `OAUTH` |
| `ENABLED` | Must be set to `TRUE` |
| `OAUTH_CLIENT` | Must be set to `CUSTOM` for external applications |
| `OAUTH_CLIENT_TYPE` | Use `'CONFIDENTIAL'` for server-based applications |
| `OAUTH_REDIRECT_URI` | Must match the URI configured in your MCP Server (default: `http://localhost:8090/oauth/callback`) |
| `OAUTH_ISSUE_REFRESH_TOKENS` | Set to `TRUE` to enable automatic token refresh |
| `OAUTH_USE_SECONDARY_ROLES` | `IMPLICIT` allows default secondary roles to be set in OAuth sessions |
| `OAUTH_REFRESH_TOKEN_VALIDITY` | Number of seconds a refresh token is valid (default: 90 days) |

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

Store these credentials securely as they will be needed to configure the MCP Server.

## Step 3: Configure Environment Variables

Set the following environment variables:

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

### Account Identifier Format

The account identifier format depends on your Snowflake region and account type:

- **Standard accounts**: `orgname-accountname` (e.g., `mycompany-analytics`)
- **US accounts**: `orgname-accountname` (e.g., `mycompany-analytics`)
- **EU accounts**: `orgname-accountname.eu-central-1` or `orgname-accountname.eu-west-1`
- **GCP accounts**: `orgname-accountname.gcp-region`
- **Azure accounts**: `orgname-accountname.region.azure`

## Step 4: Start the MCP Server with OAuth Configuration

Start the server with the appropriate OAuth configuration:

```bash
python snowflake_mcp_server.py \
  --port 8090 \
  --mcp-port 8091 \
  --oauth-redirect-uri "http://localhost:8090/oauth/callback" \
  --oauth-token-cache-file "oauth_tokens.json"
```

### Port Configuration

When integrating with Windsurf, use the recommended ports to avoid conflicts:
- FastAPI app port: 8090 (configurable with `--port`)
- FastMCP server port: 8091 (configurable with `--mcp-port`)

## Step 5: Complete the OAuth Flow

The first time you connect to Snowflake, the server will initiate the OAuth flow:

1. A browser window will open automatically to the Snowflake login page
2. Log in with your Snowflake credentials or SSO provider
3. Authorize the application to access your Snowflake account
4. After successful authentication, you'll be redirected back to the MCP Server
5. The server will store the OAuth tokens for future use

## Managing OAuth Tokens

The MCP Server provides several commands to manage OAuth tokens:

### Check Token Status

```bash
curl -X POST http://localhost:8090/api/v1/invoke -H "Content-Type: application/json" -d '{"name": "oauth_manager", "arguments": {"action": "status"}}'
```

### Start OAuth Flow Manually

```bash
curl -X POST http://localhost:8090/api/v1/invoke -H "Content-Type: application/json" -d '{"name": "oauth_manager", "arguments": {"action": "start"}}'
```

### Refresh Tokens

```bash
curl -X POST http://localhost:8090/api/v1/invoke -H "Content-Type: application/json" -d '{"name": "oauth_manager", "arguments": {"action": "refresh"}}'
```

### Clear Stored Tokens

```bash
curl -X POST http://localhost:8090/api/v1/invoke -H "Content-Type: application/json" -d '{"name": "oauth_manager", "arguments": {"action": "clear"}}'
```

## Troubleshooting

### Common Issues

1. **"Failed to obtain OAuth connection parameters"**
   - Verify that the client ID and secret are correct
   - Check that the redirect URI matches the one configured in the security integration
   - Ensure your Snowflake user has the necessary permissions

2. **"OAuth is not available"**
   - Make sure the oauth_handler module is installed and accessible

3. **Token Refresh Failures**
   - If tokens cannot be refreshed, clear the stored tokens and re-authenticate
   - Verify that `OAUTH_ISSUE_REFRESH_TOKENS` is set to TRUE in the security integration

4. **Connection Errors**
   - Check that the required environment variables are set correctly
   - Verify that your Snowflake account is accessible

## Security Best Practices

1. **Token Storage**: OAuth tokens are stored in the file specified by `--oauth-token-cache-file`. Ensure this file has appropriate permissions (readable only by the server process).

2. **Client Secret**: The client secret should be treated as a sensitive credential. Do not expose it in public repositories or insecure locations.

3. **Token Expiration**: Configure an appropriate `OAUTH_REFRESH_TOKEN_VALIDITY` value based on your security requirements. Shorter values enhance security but require more frequent re-authentication.

4. **Environment Variables**: Store sensitive credentials in environment variables rather than command-line arguments to prevent exposure in process listings.

## Integration with Identity Providers

If your Snowflake account is configured with SSO through a third-party identity provider (IdP), the OAuth flow will automatically redirect to your IdP's login page. This allows users to authenticate using your organization's existing identity system.

## Additional Resources

- [Snowflake OAuth Documentation](https://docs.snowflake.com/en/user-guide/oauth-custom.html)
- [Snowflake Security Integrations](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration.html)
- [Paradime Snowflake OAuth Guide](https://docs.paradime.io/app-help/documentation/settings/connections/connection-security/snowflake-oauth)
