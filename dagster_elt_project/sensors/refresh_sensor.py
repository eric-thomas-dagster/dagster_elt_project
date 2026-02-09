"""Sensor to automatically refresh component state and discover new pipelines."""

import os
from pathlib import Path
from dagster import sensor, RunRequest, SkipReason, SensorEvaluationContext
from dagster_elt_project.components.elt_github_component import EltGithubComponent


@sensor(
    name="refresh_pipeline_state",
    minimum_interval_seconds=300,  # Run at most every 5 minutes
    description="Refreshes component state to discover new pipelines from GitHub"
)
def refresh_pipeline_state_sensor(context: SensorEvaluationContext):
    """
    Periodically refresh the component state to discover new or updated pipelines.

    This sensor:
    1. Checks if auto_refresh is enabled
    2. Pulls latest changes from GitHub
    3. Discovers any new/updated pipelines
    4. Updates the pipelines_state.json file
    """

    # Check if auto-refresh is enabled
    auto_refresh = os.getenv("ELT_AUTO_REFRESH", "true").lower() == "true"

    if not auto_refresh:
        return SkipReason("Auto-refresh is disabled (ELT_AUTO_REFRESH=false)")

    try:
        # Create component instance
        component = EltGithubComponent()

        # Define state path
        state_path = Path(__file__).parent.parent / "defs" / "elt_pipelines"

        # Create minimal context
        class MinimalContext:
            def __init__(self, path):
                self.path = path

        context_obj = MinimalContext(path=state_path)

        # Refresh state (pulls from GitHub and discovers pipelines)
        component.write_state_to_path(context_obj, state_path)

        context.log.info(
            f"Successfully refreshed pipeline state from {component.params.repo_url}"
        )

        # Check state file to see how many pipelines were discovered
        state_file = state_path / "pipelines_state.json"
        pipeline_count = 0
        if state_file.exists():
            import json
            with open(state_file) as f:
                state = json.load(f)
                dlt_count = len(state.get("dlt_pipelines", []))
                sling_count = len(state.get("sling_replications", []))
                pipeline_count = dlt_count + sling_count
                context.log.info(
                    f"Discovered {dlt_count} dlt pipelines and {sling_count} sling replications"
                )

        # Note: In dev mode with `dagster dev`, definitions auto-reload on file changes
        # In production, you need to either:
        # 1. Click "Reload definitions" in the Dagster UI
        # 2. Configure your deployment to auto-reload (e.g., Dagster+ auto-reloads)
        # 3. Set up a file watcher to restart the code server

        context.log.info(
            "📝 New pipelines discovered! To see them in Dagster:"
        )
        context.log.info(
            "   • Dev mode: Touch definitions.py to trigger auto-reload"
        )
        context.log.info(
            "   • Or: Click 'Reload definitions' in the UI"
        )
        context.log.info(
            "   • Dagster+: Auto-reloads within 60 seconds"
        )

        # Touch the definitions file to trigger auto-reload in dev mode
        try:
            definitions_file = Path(__file__).parent.parent / "definitions.py"
            if definitions_file.exists():
                definitions_file.touch()
                context.log.info("✅ Touched definitions.py to trigger reload")
        except Exception as touch_error:
            context.log.warning(f"Could not touch definitions.py: {touch_error}")

        return SkipReason(
            f"State refreshed successfully. Found {pipeline_count} pipelines."
        )

    except Exception as e:
        context.log.error(f"Failed to refresh pipeline state: {str(e)}")
        return SkipReason(f"State refresh failed: {str(e)}")
