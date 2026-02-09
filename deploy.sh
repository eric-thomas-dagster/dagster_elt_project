#!/bin/bash
set -e

# Dagster+ Deployment Script
# Deploys the ELT project to Dagster Cloud using Docker

# Configuration
ORGANIZATION="ericthomas-dagster"
DEPLOYMENT="prod"
LOCATION_NAME="elt-pipelines"
MODULE_NAME="dagster_elt_project.definitions"
ATTRIBUTE="defs"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}  Dagster+ Deployment - ELT Pipelines${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# Check if API token is set
if [ -z "$DAGSTER_CLOUD_API_TOKEN" ]; then
    echo -e "${RED}Error: DAGSTER_CLOUD_API_TOKEN environment variable not set${NC}"
    echo "Please set it with: export DAGSTER_CLOUD_API_TOKEN=your_token"
    exit 1
fi

echo -e "${YELLOW}Step 1: Building and deploying Docker image...${NC}"
echo ""

# Deploy using Docker
dagster-cloud serverless deploy-docker \
  --organization "$ORGANIZATION" \
  --api-token "$DAGSTER_CLOUD_API_TOKEN" \
  --deployment "$DEPLOYMENT" \
  --location-name "$LOCATION_NAME" \
  --module-name "$MODULE_NAME" \
  --attribute "$ATTRIBUTE"

DEPLOY_EXIT_CODE=$?

if [ $DEPLOY_EXIT_CODE -ne 0 ]; then
    echo -e "${RED}Deployment failed with exit code $DEPLOY_EXIT_CODE${NC}"
    exit $DEPLOY_EXIT_CODE
fi

echo ""
echo -e "${GREEN}✓ Deployment successful!${NC}"
echo ""

# Ask if user wants to refresh defs state
read -p "Would you like to refresh the definitions state (clone GitHub repo and discover pipelines)? (y/n) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    echo -e "${YELLOW}Step 2: Refreshing definitions state...${NC}"
    echo ""
    echo -e "${BLUE}This will clone your GitHub repo and discover all pipelines.${NC}"
    echo ""

    # Refresh definitions state to trigger component sync
    # This runs locally and syncs to Dagster Cloud
    uv run dg plus deploy refresh-defs-state

    REFRESH_EXIT_CODE=$?

    if [ $REFRESH_EXIT_CODE -ne 0 ]; then
        echo ""
        echo -e "${RED}Refresh failed with exit code $REFRESH_EXIT_CODE${NC}"
        echo -e "${YELLOW}You can manually refresh later with: uv run dg plus deploy refresh-defs-state${NC}"
    else
        echo ""
        echo -e "${GREEN}✓ Definitions state refreshed!${NC}"
        echo -e "${GREEN}✓ Pipelines discovered and synced to Dagster Cloud!${NC}"
    fi
fi

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  Deployment Complete!${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo -e "View your deployment at:"
echo -e "${BLUE}https://$ORGANIZATION.dagster.cloud/$DEPLOYMENT/locations${NC}"
echo ""
