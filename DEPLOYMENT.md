# Deploying to Dagster Cloud

This project includes a deployment script to easily deploy to Dagster Cloud.

## Quick Start

### Option 1: Using the deployment script (recommended)

```bash
# Set your API token
export DAGSTER_CLOUD_API_TOKEN=user:your_token_here

# Or source the .env.deploy file
source .env.deploy

# Run the deployment script
./deploy.sh
```

The script will:
1. Build and deploy a Docker image to Dagster Cloud
2. Ask if you want to refresh definitions state (recommended)
3. Display the deployment URL

### Option 2: Manual deployment

```bash
# Deploy Docker image
dagster-cloud serverless deploy-docker \
  --organization ericthomas-dagster \
  --api-token user:your_token_here \
  --deployment prod \
  --location-name elt-pipelines \
  --module-name dagster_elt_project.definitions \
  --attribute defs

# Refresh definitions state (optional but recommended)
uv run dg plus deploy refresh-defs-state \
  --organization ericthomas-dagster \
  --deployment prod \
  --api-token user:your_token_here
```

## What is `refresh-defs-state`?

The `refresh-defs-state` command triggers the `EltGithubComponent` to:
- Clone your GitHub repository
- Discover all dlt and Sling pipelines
- Update the Dagster definitions with the latest pipeline configurations

**When to use it:**
- After deploying for the first time
- After adding/modifying pipelines in your GitHub repository
- When you want to force a sync from GitHub

## Environment Variables for Dagster Cloud

Make sure these are set in Dagster Cloud (not in the deployment script):
- `ELT_REPO_URL` - Your GitHub repository URL
- `ELT_REPO_BRANCH` - Branch to use (default: main)
- `GITHUB_TOKEN` - GitHub personal access token (for private repos)
- `ELT_PIPELINES_DIR` - Directory containing pipelines (default: pipelines)

Set these at: https://ericthomas-dagster.dagster.cloud/prod/locations

## Deployment Architecture

- **Method**: Docker-based serverless deployment
- **Base Image**: Python 3.11 slim with git installed
- **Entry Point**: `dagster_elt_project.definitions:defs`
- **Component**: `EltGithubComponent` (state-backed)

## Troubleshooting

### "Command not found: dagster-cloud"
Install the Dagster Cloud CLI:
```bash
pip install dagster-cloud
```

### "API token not set"
Make sure you've exported the token:
```bash
export DAGSTER_CLOUD_API_TOKEN=user:your_token_here
```

### Deployment succeeds but no pipelines show up
Run the refresh command:
```bash
uv run dg plus deploy refresh-defs-state \
  --organization ericthomas-dagster \
  --deployment prod \
  --api-token $DAGSTER_CLOUD_API_TOKEN
```

### Git errors in Dagster Cloud
Make sure `GITHUB_TOKEN` environment variable is set in Dagster Cloud and has access to your repository.
