"""
OAuth handler for Snowflake MCP Server

This module provides functions for OAuth authentication with Snowflake.
It implements the OAuth 2.0 Authorization Code flow for secure authentication.
"""

import os
import json
import time
import secrets
import logging
import webbrowser
import urllib.parse
from typing import Dict, Optional, Tuple, Any

import requests

# Configure logging
logger = logging.getLogger("snowflake_mcp_server.oauth")

class OAuthTokenManager:
    """Manages OAuth tokens for Snowflake authentication."""
    
    def __init__(self, 
                 account: str,
                 client_id: str, 
                 client_secret: str, 
                 redirect_uri: str,
                 token_cache_file: str):
        """
        Initialize the OAuth token manager.
        
        Args:
            account: Snowflake account identifier
            client_id: OAuth client ID
            client_secret: OAuth client secret
            redirect_uri: OAuth redirect URI
            token_cache_file: File to cache OAuth tokens
        """
        self.account = account
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.token_cache_file = token_cache_file
        self.tokens = self._load_cached_tokens()
        
    def _load_cached_tokens(self) -> Dict[str, Any]:
        """Load cached tokens from file."""
        if not os.path.exists(self.token_cache_file):
            return {}
            
        try:
            with open(self.token_cache_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Error loading cached tokens: {e}")
            return {}
            
    def _save_tokens(self) -> None:
        """Save tokens to cache file."""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.token_cache_file), exist_ok=True)
            
            with open(self.token_cache_file, 'w') as f:
                json.dump(self.tokens, f)
        except IOError as e:
            logger.warning(f"Error saving tokens to cache: {e}")
            
    def get_authorization_url(self) -> Tuple[str, str]:
        """
        Generate the authorization URL for the OAuth flow.
        
        Returns:
            Tuple containing the authorization URL and state parameter
        """
        # Generate a random state parameter to prevent CSRF
        state = secrets.token_urlsafe(32)
        
        # Build the authorization URL
        params = {
            'response_type': 'code',
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'state': state,
            'scope': 'session:role:*',  # Request access to all roles
            'prompt': 'login'  # Always prompt for login to ensure fresh authentication
        }
        
        auth_url = f"https://{self.account}.snowflakecomputing.com/oauth/authorize"
        full_url = f"{auth_url}?{urllib.parse.urlencode(params)}"
        
        return full_url, state
        
    def exchange_code_for_token(self, code: str) -> bool:
        """
        Exchange an authorization code for access and refresh tokens.
        
        Args:
            code: The authorization code received from the OAuth redirect
            
        Returns:
            True if token exchange was successful, False otherwise
        """
        token_url = f"https://{self.account}.snowflakecomputing.com/oauth/token"
        
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': self.redirect_uri
        }
        
        try:
            response = requests.post(
                token_url,
                data=data,
                auth=(self.client_id, self.client_secret),
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            
            response.raise_for_status()
            token_data = response.json()
            
            # Add expiration time based on expires_in
            token_data['expires_at'] = time.time() + token_data.get('expires_in', 3600)
            
            # Store tokens
            self.tokens = token_data
            self._save_tokens()
            
            logger.info("Successfully obtained OAuth tokens")
            return True
            
        except requests.RequestException as e:
            logger.error(f"Error exchanging code for token: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response content: {e.response.text}")
            return False
            
    def refresh_token(self) -> bool:
        """
        Refresh the access token using the refresh token.
        
        Returns:
            True if token refresh was successful, False otherwise
        """
        if 'refresh_token' not in self.tokens:
            logger.error("No refresh token available")
            return False
            
        token_url = f"https://{self.account}.snowflakecomputing.com/oauth/token"
        
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.tokens['refresh_token']
        }
        
        try:
            response = requests.post(
                token_url,
                data=data,
                auth=(self.client_id, self.client_secret),
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            
            response.raise_for_status()
            token_data = response.json()
            
            # Add expiration time based on expires_in
            token_data['expires_at'] = time.time() + token_data.get('expires_in', 3600)
            
            # Update tokens, keeping the refresh token if not provided in response
            if 'refresh_token' not in token_data and 'refresh_token' in self.tokens:
                token_data['refresh_token'] = self.tokens['refresh_token']
                
            self.tokens = token_data
            self._save_tokens()
            
            logger.info("Successfully refreshed OAuth tokens")
            return True
            
        except requests.RequestException as e:
            logger.error(f"Error refreshing token: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response content: {e.response.text}")
            return False
            
    def get_access_token(self) -> Optional[str]:
        """
        Get a valid access token, refreshing if necessary.
        
        Returns:
            A valid access token or None if no token is available or refresh fails
        """
        # Check if we have tokens
        if not self.tokens or 'access_token' not in self.tokens:
            logger.warning("No access token available")
            return None
            
        # Check if token is expired
        if time.time() > self.tokens.get('expires_at', 0):
            logger.info("Access token expired, attempting to refresh")
            if not self.refresh_token():
                return None
                
        return self.tokens.get('access_token')
        
    def clear_tokens(self) -> None:
        """Clear all stored tokens."""
        self.tokens = {}
        if os.path.exists(self.token_cache_file):
            try:
                os.remove(self.token_cache_file)
                logger.info("Cleared token cache file")
            except IOError as e:
                logger.warning(f"Error removing token cache file: {e}")
                
    def has_valid_tokens(self) -> bool:
        """
        Check if we have valid tokens.
        
        Returns:
            True if we have valid tokens, False otherwise
        """
        if not self.tokens or 'access_token' not in self.tokens:
            return False
            
        # If token is expired but we have a refresh token, we consider it valid
        if time.time() > self.tokens.get('expires_at', 0):
            return 'refresh_token' in self.tokens
            
        return True
        
    def get_token_info(self) -> Dict[str, Any]:
        """
        Get information about the current tokens.
        
        Returns:
            Dictionary containing token information
        """
        if not self.tokens:
            return {"status": "no_tokens"}
            
        info = {
            "status": "valid" if self.has_valid_tokens() else "expired",
            "token_type": self.tokens.get("token_type"),
            "has_refresh_token": "refresh_token" in self.tokens
        }
        
        # Add expiration information
        if "expires_at" in self.tokens:
            expires_in = self.tokens["expires_at"] - time.time()
            info["expires_in_seconds"] = max(0, int(expires_in))
            info["expires_in_minutes"] = max(0, int(expires_in / 60))
            info["expires_in_hours"] = max(0, int(expires_in / 3600))
            
        return info
        
    def get_connection_parameters(self) -> Dict[str, str]:
        """
        Get Snowflake connection parameters for OAuth.
        
        Returns:
            Dictionary containing connection parameters for Snowflake
        """
        if not self.has_valid_tokens():
            if 'refresh_token' in self.tokens:
                # Try to refresh the token
                if not self.refresh_token():
                    return {}
            else:
                # No valid tokens and can't refresh
                return {}
                
        return {
            "token": self.tokens.get("access_token"),
            "authenticator": "oauth"
        }


def start_oauth_flow(token_manager: OAuthTokenManager) -> None:
    """
    Start the OAuth flow by opening the browser to the authorization URL.
    
    Args:
        token_manager: The OAuth token manager
    """
    auth_url, state = token_manager.get_authorization_url()
    
    print("\n=== Snowflake OAuth Authentication ===")
    print("Opening browser to authenticate with Snowflake...")
    print(f"If the browser doesn't open automatically, visit this URL:\n{auth_url}\n")
    
    # Try to open the browser automatically
    webbrowser.open(auth_url)
    
    return state


def setup_oauth_callback_server(token_manager: OAuthTokenManager, state: str, port: int = 8090) -> None:
    """
    Set up a simple HTTP server to handle the OAuth callback.
    
    This function is meant to be called after start_oauth_flow() to handle the callback.
    It will start a simple HTTP server on the specified port and wait for the callback.
    
    Args:
        token_manager: The OAuth token manager
        state: The state parameter used in the authorization request
        port: The port to listen on (default: 8090)
    """
    from http.server import HTTPServer, BaseHTTPRequestHandler
    import threading
    
    class OAuthCallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            """Handle GET requests to the callback URL."""
            try:
                # Parse the query parameters
                query = urllib.parse.urlparse(self.path).query
                params = urllib.parse.parse_qs(query)
                
                # Check if this is the OAuth callback
                if self.path.startswith('/oauth/callback'):
                    # Verify state to prevent CSRF
                    if 'state' not in params or params['state'][0] != state:
                        self._send_response("Error: Invalid state parameter", 400)
                        return
                        
                    # Check for error
                    if 'error' in params:
                        error_msg = params['error'][0]
                        error_desc = params.get('error_description', ['Unknown error'])[0]
                        self._send_response(f"Error: {error_msg} - {error_desc}", 400)
                        return
                        
                    # Check for authorization code
                    if 'code' not in params:
                        self._send_response("Error: No authorization code received", 400)
                        return
                        
                    # Exchange code for token
                    code = params['code'][0]
                    if token_manager.exchange_code_for_token(code):
                        self._send_response("Authentication successful! You can close this window and return to the application.")
                    else:
                        self._send_response("Error: Failed to exchange code for token", 500)
                else:
                    self._send_response("Not Found", 404)
            except Exception as e:
                logger.error(f"Error handling OAuth callback: {e}")
                self._send_response(f"Server Error: {str(e)}", 500)
                
        def _send_response(self, message: str, status_code: int = 200):
            """Send an HTTP response with the given message and status code."""
            self.send_response(status_code)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Snowflake OAuth</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }}
                    .container {{ max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 5px; }}
                    h1 {{ color: #0066cc; }}
                    .success {{ color: green; }}
                    .error {{ color: red; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>Snowflake OAuth Authentication</h1>
                    <p class="{('error' if status_code >= 400 else 'success')}">{message}</p>
                </div>
            </body>
            </html>
            """
            
            self.wfile.write(html.encode())
            
        def log_message(self, format, *args):
            """Override to use our logger instead of printing to stderr."""
            logger.info(f"OAuthCallbackHandler: {format % args}")
    
    # Create and start the server
    server = HTTPServer(('localhost', port), OAuthCallbackHandler)
    
    # Run the server in a separate thread
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True  # So the thread will exit when the main thread exits
    server_thread.start()
    
    logger.info(f"OAuth callback server started on port {port}")
    print(f"Waiting for authentication callback on http://localhost:{port}/oauth/callback")
    
    return server
