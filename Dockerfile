FROM node:18-alpine

WORKDIR /app

# Install dependencies for Snowflake SDK
RUN apk add --no-cache python3 make g++ openssl-dev

# Copy source code first
COPY src/ ./src/
COPY .env.example ./.env
COPY package*.json ./

# Install only production dependencies
RUN npm install --no-package-lock --only=production

# Create a directory for logs
RUN mkdir -p logs

# Set environment variables
ENV NODE_ENV=production

# Expose the port the server runs on
EXPOSE 3000

# Command to run the server
CMD ["node", "src/index.js"]
