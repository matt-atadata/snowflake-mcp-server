#!/bin/bash
# Script to build and deploy the Snowflake MCP Server to the Snowflake image repository

# Exit on any error
set -e

# Configuration
IMAGE_NAME="snowflake-mcp-server"
VERSION=$(node -e "console.log(require('./package.json').version)")
REGISTRY="snowflakecomputing.azurecr.io"
FULL_IMAGE_NAME="$REGISTRY/$IMAGE_NAME:$VERSION"
LATEST_IMAGE_NAME="$REGISTRY/$IMAGE_NAME:latest"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Building and deploying Snowflake MCP Server v$VERSION${NC}"

# Step 1: Run tests
echo -e "\n${GREEN}Running tests...${NC}"
npm test

# Step 2: Build Docker image
echo -e "\n${GREEN}Building Docker image...${NC}"
docker build -t $IMAGE_NAME .
docker tag $IMAGE_NAME $FULL_IMAGE_NAME
docker tag $IMAGE_NAME $LATEST_IMAGE_NAME

# Step 3: Log in to Snowflake registry
echo -e "\n${GREEN}Logging in to Snowflake registry...${NC}"
echo "Please enter your Snowflake registry credentials when prompted"
docker login $REGISTRY

# Step 4: Push images to registry
echo -e "\n${GREEN}Pushing images to registry...${NC}"
docker push $FULL_IMAGE_NAME
docker push $LATEST_IMAGE_NAME

echo -e "\n${GREEN}Deployment complete!${NC}"
echo -e "Version image: ${YELLOW}$FULL_IMAGE_NAME${NC}"
echo -e "Latest image: ${YELLOW}$LATEST_IMAGE_NAME${NC}"
