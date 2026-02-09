"""Dagster definitions."""

import os
import yaml
import json
from pathlib import Path
from dotenv import load_dotenv
from dagster import Definitions, AssetSelection, asset, AssetExecutionContext, MetadataValue, Output
from dagster_elt_project.components.elt_github_component import EltGithubComponent, GIT_AVAILABLE
from dagster_elt_project.sensors import refresh_pipeline_state_sensor

# Load environment variables from .env file
load_dotenv()

# Load component configuration from YAML
defs_path = Path(__file__).parent / "defs" / "elt_pipelines"
yaml_path = defs_path / "defs.yaml"

with open(yaml_path) as f:
    config = yaml.safe_load(f)

# Resolve environment variables in attributes
params = {}
attributes = config.get("attributes", {})
for key, value in attributes.items():
    # Resolve ${VAR} syntax
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        env_var = value[2:-1]
        resolved = os.getenv(env_var)
        # Handle boolean strings
        if resolved and resolved.lower() in ("true", "false"):
            params[key] = resolved.lower() == "true"
        else:
            params[key] = resolved
    else:
        params[key] = value

# Create component instance with parameters
component = EltGithubComponent(**params)

# Build definitions using a minimal context-like object
class MinimalContext:
    def __init__(self, path):
        self.path = path

context = MinimalContext(path=defs_path)

# Helper function to load component definitions
def _load_component_defs():
    import logging
    logger = logging.getLogger(__name__)

    state_file = defs_path / "pipelines_state.json"

    try:
        # First, write state (clone repo and discover pipelines)
        logger.info("Attempting to clone GitHub repo and discover pipelines...")
        component.write_state_to_path(context, defs_path)
        logger.info("Successfully wrote pipeline state")
        # Then build definitions from state
        return component.build_defs_from_state(context, defs_path)
    except Exception as e:
        logger.error(f"Error writing pipeline state: {e}")
        # If state already exists, just load from it
        if state_file.exists():
            logger.info("Loading from existing pipeline state")
            return component.build_defs_from_state(context, defs_path)
        else:
            logger.error("No existing state found. Returning empty definitions.")
            logger.error("Make sure environment variables are set: ELT_REPO_URL, GITHUB_TOKEN (if private repo)")
            return Definitions()

# Create a diagnostic asset to show component status
@asset(
    name="elt_component_diagnostics",
    description="Shows the status of the ELT component and any errors",
    group_name="diagnostics"
)
def elt_component_diagnostics(context: AssetExecutionContext):
    """Diagnostic asset that reports on the ELT component state."""
    state_file = defs_path / "pipelines_state.json"

    # Read component parameters
    params_info = {
        "repo_url": component.params.repo_url or "NOT SET",
        "repo_branch": component.params.repo_branch,
        "pipelines_directory": component.params.pipelines_directory,
        "github_token_set": "Yes" if component.params.github_token else "No",
        "git_available": str(GIT_AVAILABLE),
    }

    # Read state file if it exists
    if state_file.exists():
        try:
            state_data = json.loads(state_file.read_text())
            state_info = {
                "state_file_exists": True,
                "dlt_pipelines": len(state_data.get("dlt_pipelines", [])),
                "sling_replications": len(state_data.get("sling_replications", [])),
                "repo_commit": state_data.get("repo_commit", "N/A"),
                "error": state_data.get("error", "None"),
            }
        except Exception as e:
            state_info = {"state_file_exists": True, "error": f"Failed to read state: {e}"}
    else:
        state_info = {
            "state_file_exists": False,
            "error": "State file not found - component may have failed to initialize"
        }

    # Log all info
    context.log.info(f"Component Parameters: {json.dumps(params_info, indent=2)}")
    context.log.info(f"Component State: {json.dumps(state_info, indent=2)}")

    # Add as metadata
    return Output(
        value={"status": "checked"},
        metadata={
            "Parameters": MetadataValue.json(params_info),
            "State": MetadataValue.json(state_info),
            "Instructions": MetadataValue.md(
                "**If you see issues:**\n\n"
                "1. Check that `ELT_REPO_URL` environment variable is set in Dagster Cloud\n"
                "2. If repo is private, set `GITHUB_TOKEN` environment variable\n"
                "3. Verify Git is available (should be 'True' above)\n\n"
                "**Environment variables should be set at:**\n"
                "Dagster Cloud > Deployments > prod > Code Locations > elt-pipelines > Environment variables"
            )
        }
    )

# Merge component definitions with sensors and diagnostics
defs = Definitions.merge(
    _load_component_defs(),
    Definitions(
        assets=[elt_component_diagnostics],
        sensors=[refresh_pipeline_state_sensor],
    )
)
