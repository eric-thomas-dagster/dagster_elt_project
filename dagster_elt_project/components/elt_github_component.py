"""State-backed component for ELT pipeline orchestration.

Features:
- Automatic schedule creation from YAML
- Partition support for incremental pipelines
- Rich metadata emission
- Graceful environment variable handling
- Discovers dlt and Sling pipelines from GitHub
"""

import importlib.util
import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)
from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Backoff,
    DefaultScheduleStatus,
    Definitions,
    Jitter,
    MetadataValue,
    Output,
    RetryPolicy,
    ScheduleDefinition,
    TimeWindowPartitionsDefinition,
    StaticPartitionsDefinition,
    asset,
)
from dagster.components import Component, ComponentLoadContext
from pydantic import BaseModel, Field

# Make git optional for environments where it's not available (e.g., Dagster Cloud)
try:
    from git import Repo
    GIT_AVAILABLE = True
except (ImportError, Exception):
    GIT_AVAILABLE = False
    Repo = None  # type: ignore

from ..schemas.dagster_metadata import DagsterMetadata


class EltGithubComponentParams(BaseModel):
    """Parameters for the EltGithubComponent.

    All parameters can be set via environment variables.
    """

    repo_url: Optional[str] = Field(
        default=None,
        description="GitHub repository URL (env: ELT_REPO_URL)"
    )
    repo_branch: str = Field(
        default="main",
        description="Branch to clone/pull (env: ELT_REPO_BRANCH)"
    )
    github_token: Optional[str] = Field(
        default=None,
        description="GitHub token for private repos (env: GITHUB_TOKEN)"
    )
    pipelines_directory: str = Field(
        default="pipelines",
        description="Directory containing pipeline subdirectories (env: ELT_PIPELINES_DIR)"
    )
    auto_refresh: bool = Field(
        default=True,
        description="Automatically refresh state in dev mode (env: ELT_AUTO_REFRESH)"
    )

    @classmethod
    def from_env(cls, **overrides):
        """Load parameters from environment variables with optional overrides."""
        import os
        params = {}

        # Only add params if they have actual values (not None)
        repo_url = os.getenv("ELT_REPO_URL")
        if repo_url:
            params["repo_url"] = repo_url

        repo_branch = os.getenv("ELT_REPO_BRANCH")
        if repo_branch:
            params["repo_branch"] = repo_branch

        github_token = os.getenv("GITHUB_TOKEN")
        if github_token:
            params["github_token"] = github_token

        pipelines_dir = os.getenv("ELT_PIPELINES_DIR")
        if pipelines_dir:
            params["pipelines_directory"] = pipelines_dir

        auto_refresh_str = os.getenv("ELT_AUTO_REFRESH")
        if auto_refresh_str:
            params["auto_refresh"] = auto_refresh_str.lower() == "true"

        # Apply overrides, but only if they're not None
        for key, value in overrides.items():
            if value is not None:
                params[key] = value

        return cls(**params)


class DltPipelineInfo(BaseModel):
    """Discovered dlt pipeline information."""

    name: str
    directory: Path
    pipeline_py: Path
    dagster_yaml: Optional[Path] = None
    metadata: Optional[DagsterMetadata] = None

    class Config:
        arbitrary_types_allowed = True


class SlingReplicationInfo(BaseModel):
    """Discovered Sling replication information."""

    name: str
    directory: Path
    replication_yaml: Path
    dagster_yaml: Optional[Path] = None
    metadata: Optional[DagsterMetadata] = None

    class Config:
        arbitrary_types_allowed = True


class PipelineState(BaseModel):
    """State data for discovered pipelines."""

    dlt_pipelines: List[DltPipelineInfo] = Field(default_factory=list)
    sling_replications: List[SlingReplicationInfo] = Field(default_factory=list)
    repo_commit: Optional[str] = None
    repo_path: Optional[str] = None
    error: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True


class EltGithubComponent(Component):
    """Enhanced state-backed component with scheduling, partitions, and rich metadata."""

    @classmethod
    def get_schema(cls):
        """Return the params schema for this component."""
        return EltGithubComponentParams

    def __init__(self, **params):
        super().__init__()
        # Load from environment variables if no parameters provided
        if not params or not params.get("repo_url"):
            self.params = EltGithubComponentParams.from_env(**params)
        else:
            self.params = EltGithubComponentParams(**params)

    @staticmethod
    def get_component_key_for_params(params: Dict[str, Any]) -> str:
        """Generate a unique key for this component instance."""
        repo_url = params.get("repo_url", "")
        safe_url = repo_url.replace("https://", "").replace("http://", "").replace("/", "_")
        return f"EltGithubComponent[{safe_url}]"

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Required abstract method - delegates to build_defs_from_state for state-backed components."""
        # For state-backed components, this method is called after write_state_to_path
        # The actual implementation is in build_defs_from_state
        state_path = context.path
        return self.build_defs_from_state(context, state_path)

    def write_state_to_path(self, context: ComponentLoadContext, path: Path) -> None:
        """Clone/pull the GitHub repo and discover pipeline directories."""
        state = PipelineState()

        try:
            # Check if git is available
            if not GIT_AVAILABLE:
                raise ValueError(
                    "Git is not available in this environment. "
                    "This component requires git to clone and sync repositories. "
                    "Consider using a hybrid deployment or pre-populating the repository."
                )

            # Validate required parameters
            if not self.params.repo_url:
                raise ValueError(
                    "ELT_REPO_URL environment variable is required. "
                    "Please set it to your GitHub repository URL."
                )
            # Create a directory for cloning
            clone_dir = path / "repo_clone"
            clone_dir.mkdir(parents=True, exist_ok=True)

            # GitHub token is already resolved by Dagster's EnvVar system
            github_token = self.params.github_token

            # Clone or pull the repository
            repo = self._clone_or_pull_repo(clone_dir, github_token)
            state.repo_commit = repo.head.commit.hexsha
            state.repo_path = str(clone_dir)

            # Discover dlt pipelines
            dlt_dir = clone_dir / self.params.pipelines_directory / "dlt"
            if dlt_dir.exists():
                state.dlt_pipelines = self._discover_dlt_pipelines(dlt_dir)
                logger.info(f"Discovered {len(state.dlt_pipelines)} dlt pipelines")

            # Discover Sling replications
            sling_dir = clone_dir / self.params.pipelines_directory / "sling"
            if sling_dir.exists():
                state.sling_replications = self._discover_sling_replications(sling_dir)
                logger.info(
                    f"Discovered {len(state.sling_replications)} Sling replications"
                )

        except Exception as e:
            state.error = str(e)
            logger.error(f"Error discovering pipelines: {e}")

        # Write state to disk
        state_file = path / "pipelines_state.json"
        state_file.write_text(state.model_dump_json(indent=2))

    def build_defs_from_state(self, context: ComponentLoadContext, state_path: Path) -> Definitions:
        """Build Dagster definitions with schedules, partitions, and rich metadata."""
        # Read state from disk
        state_file = state_path / "pipelines_state.json"
        if not state_file.exists():
            logger.warning("No pipeline state found. Run refresh to discover pipelines.")
            return Definitions()

        state = PipelineState.model_validate_json(state_file.read_text())

        if state.error:
            logger.error(f"Error in pipeline state: {state.error}")
            return Definitions()

        # Build assets and schedules
        all_assets = []
        all_schedules = []

        # Build dlt assets
        for pipeline_info in state.dlt_pipelines:
            if pipeline_info.metadata and not pipeline_info.metadata.enabled:
                continue

            asset_def = self._build_dlt_asset(pipeline_info, state.repo_path)
            all_assets.append(asset_def)

            # Create schedule if configured
            if pipeline_info.metadata and pipeline_info.metadata.schedule:
                schedule = self._build_schedule(
                    f"dlt_{pipeline_info.name}",
                    pipeline_info.metadata.schedule,
                    f"dlt_{pipeline_info.name}",
                )
                all_schedules.append(schedule)

        # Build Sling assets
        for replication_info in state.sling_replications:
            if replication_info.metadata and not replication_info.metadata.enabled:
                continue

            asset_def = self._build_sling_asset(replication_info, state.repo_path)
            all_assets.append(asset_def)

            # Create schedule if configured
            if replication_info.metadata and replication_info.metadata.schedule:
                schedule = self._build_schedule(
                    f"sling_{replication_info.name}",
                    replication_info.metadata.schedule,
                    f"sling_{replication_info.name}",
                )
                all_schedules.append(schedule)

        logger.info(
            f"Created {len(all_assets)} assets and {len(all_schedules)} schedules "
            f"(commit: {state.repo_commit[:8] if state.repo_commit else 'unknown'})"
        )

        return Definitions(assets=all_assets, schedules=all_schedules)

    def _clone_or_pull_repo(self, clone_dir: Path, github_token: Optional[str]) -> Repo:
        """Clone or pull the GitHub repository."""
        repo_url = self.params.repo_url

        if github_token:
            if "github.com" in repo_url:
                repo_url = repo_url.replace("https://", f"https://{github_token}@")

        if (clone_dir / ".git").exists():
            repo = Repo(clone_dir)
            repo.remotes.origin.pull(self.params.repo_branch)
        else:
            repo = Repo.clone_from(repo_url, clone_dir, branch=self.params.repo_branch)

        return repo

    def _discover_dlt_pipelines(self, dlt_dir: Path) -> List[DltPipelineInfo]:
        """Discover all dlt pipeline directories."""
        pipelines = []

        for pipeline_dir in dlt_dir.iterdir():
            if not pipeline_dir.is_dir():
                continue

            pipeline_py = pipeline_dir / "pipeline.py"
            if not pipeline_py.exists():
                continue

            dagster_yaml = pipeline_dir / "dagster.yaml"
            metadata = None
            if dagster_yaml.exists():
                try:
                    metadata_dict = yaml.safe_load(dagster_yaml.read_text())
                    metadata = DagsterMetadata(**metadata_dict)
                except Exception as e:
                    print(f"Warning: Could not parse {dagster_yaml}: {e}")

            pipelines.append(
                DltPipelineInfo(
                    name=pipeline_dir.name,
                    directory=pipeline_dir,
                    pipeline_py=pipeline_py,
                    dagster_yaml=dagster_yaml if dagster_yaml.exists() else None,
                    metadata=metadata,
                )
            )

        return pipelines

    def _discover_sling_replications(self, sling_dir: Path) -> List[SlingReplicationInfo]:
        """Discover all Sling replication directories."""
        replications = []

        for replication_dir in sling_dir.iterdir():
            if not replication_dir.is_dir():
                continue

            replication_yaml = replication_dir / "replication.yaml"
            if not replication_yaml.exists():
                continue

            dagster_yaml = replication_dir / "dagster.yaml"
            metadata = None
            if dagster_yaml.exists():
                try:
                    metadata_dict = yaml.safe_load(dagster_yaml.read_text())
                    metadata = DagsterMetadata(**metadata_dict)
                except Exception as e:
                    print(f"Warning: Could not parse {dagster_yaml}: {e}")

            replications.append(
                SlingReplicationInfo(
                    name=replication_dir.name,
                    directory=replication_dir,
                    replication_yaml=replication_yaml,
                    dagster_yaml=dagster_yaml if dagster_yaml.exists() else None,
                    metadata=metadata,
                )
            )

        return replications

    def _build_partitions_def(self, partitions_config):
        """Build a Dagster partitions definition from config."""
        if partitions_config.type == "time":
            time_config = partitions_config.time
            return TimeWindowPartitionsDefinition(
                start=time_config.start,
                end=time_config.end,
                cron_schedule=time_config.cron_schedule,
                timezone=time_config.timezone,
                fmt=time_config.fmt,
            )
        elif partitions_config.type == "static":
            return StaticPartitionsDefinition(partitions_config.static.partition_keys)
        else:
            raise ValueError(f"Unknown partition type: {partitions_config.type}")

    def _build_dlt_asset(self, pipeline_info: DltPipelineInfo, repo_path: str):
        """Build a Dagster asset for a dlt pipeline with rich metadata."""
        metadata = pipeline_info.metadata or DagsterMetadata()

        # Build partitions if configured
        partitions_def = None
        if metadata.partitions:
            partitions_def = self._build_partitions_def(metadata.partitions)

        # Build custom metadata
        custom_metadata = {}
        if metadata.metadata:
            for entry in metadata.metadata:
                if entry.type == "url":
                    custom_metadata[entry.key] = MetadataValue.url(str(entry.value))
                elif entry.type == "path":
                    custom_metadata[entry.key] = MetadataValue.path(str(entry.value))
                elif entry.type == "md":
                    custom_metadata[entry.key] = MetadataValue.md(str(entry.value))
                elif entry.type == "json":
                    custom_metadata[entry.key] = MetadataValue.json(entry.value)
                else:
                    custom_metadata[entry.key] = MetadataValue.text(str(entry.value))

        # Build retry policy from metadata (new format with fallback to legacy)
        retry_policy = None
        if metadata.retry_policy:
            # New format with backoff and jitter
            policy = metadata.retry_policy
            backoff = Backoff.EXPONENTIAL if policy.backoff == "EXPONENTIAL" else Backoff.LINEAR
            jitter = None
            if policy.jitter == "FULL":
                jitter = Jitter.FULL
            elif policy.jitter == "PLUS_MINUS":
                jitter = Jitter.PLUS_MINUS

            retry_policy = RetryPolicy(
                max_retries=policy.max_retries,
                delay=policy.delay,
                backoff=backoff,
                jitter=jitter,
            )
        elif metadata.retries and metadata.retries > 0:
            # Legacy format
            retry_policy = RetryPolicy(
                max_retries=metadata.retries,
                delay=metadata.retry_delay or 60,
                backoff=Backoff.LINEAR,  # Default to linear for legacy
            )

        # Build asset tags including kinds
        asset_tags = {
            **metadata.tags,
            "elt_type": "dlt",
            "pipeline": pipeline_info.name
        }
        # Add kinds to tags (Dagster uses tag prefix "dagster/kind/" for kinds)
        for kind in metadata.kinds:
            asset_tags[f"dagster/kind/{kind}"] = ""

        @asset(
            name=f"dlt_{pipeline_info.name}",
            group_name=metadata.group_name,
            tags=asset_tags,
            description=metadata.description
            or f"dlt pipeline: {pipeline_info.name}",
            metadata=custom_metadata,
            partitions_def=partitions_def,
            owners=metadata.owners or [],
            retry_policy=retry_policy,
        )
        def dlt_pipeline_asset(context: AssetExecutionContext):
            """Execute standalone dlt pipeline with rich metadata."""
            logger.info(f"Running dlt pipeline: {pipeline_info.name}")

            # Environment variables are passed through automatically
            # dlt will fail with clear errors if required credentials are missing

            # Add repo to Python path
            repo_path_obj = Path(repo_path)
            if str(repo_path_obj) not in sys.path:
                sys.path.insert(0, str(repo_path_obj))

            try:
                # Import the pipeline module dynamically
                module_name = f"pipelines.dlt.{pipeline_info.name}.pipeline"
                spec = importlib.util.spec_from_file_location(
                    module_name,
                    pipeline_info.pipeline_py,
                )
                module = importlib.util.module_from_spec(spec)
                # Add to sys.modules before executing so dlt can resolve module names
                sys.modules[module_name] = module
                spec.loader.exec_module(module)

                # Call the run function
                if not hasattr(module, "run"):
                    raise AttributeError(
                        f"Pipeline {pipeline_info.pipeline_py} must have a run() function"
                    )

                # Get partition key if partitioned
                partition_key = None
                if context.has_partition_key:
                    partition_key = context.partition_key
                    logger.info(f"Running for partition: {partition_key}")

                # Run the pipeline with partition key
                start_time = datetime.now()
                result = module.run(partition_key=partition_key)
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()

                # Emit comprehensive metadata for Dagster+ observability
                output_metadata = {
                    "pipeline_name": MetadataValue.text(pipeline_info.name),
                    "pipeline_type": MetadataValue.text("dlt"),
                    "duration_seconds": MetadataValue.float(duration),
                    "execution_time": MetadataValue.timestamp(end_time.timestamp()),
                    "start_time": MetadataValue.timestamp(start_time.timestamp()),
                    "pipeline_path": MetadataValue.path(str(pipeline_info.pipeline_py)),
                }

                # Add source code link if GitHub repo is available
                if self.params.repo_url and self.params.repo_branch:
                    # Construct GitHub URL for the pipeline file
                    repo_url = self.params.repo_url.rstrip('.git')
                    pipeline_rel_path = pipeline_info.pipeline_py.relative_to(
                        pipeline_info.pipeline_py.parent.parent.parent
                    )
                    source_url = f"{repo_url}/blob/{self.params.repo_branch}/{pipeline_rel_path}"
                    output_metadata["dagster/code_references"] = MetadataValue.url(source_url)

                # Extract comprehensive dlt-specific metadata
                if hasattr(result, "dataset_name"):
                    output_metadata["dataset_name"] = MetadataValue.text(result.dataset_name)

                if hasattr(result, "load_packages") and result.load_packages:
                    load_package = result.load_packages[0]

                    # Table information
                    if hasattr(load_package, "schema") and hasattr(load_package.schema, "tables"):
                        tables = list(load_package.schema.tables.keys())
                        output_metadata["tables_created"] = MetadataValue.md(
                            "\n".join([f"- `{table}`" for table in tables])
                        )
                        output_metadata["table_count"] = MetadataValue.int(len(tables))

                    # Row counts and data volumes
                    if hasattr(load_package, "jobs") and load_package.jobs:
                        total_rows = 0
                        total_bytes = 0
                        job_details = []

                        for job in load_package.jobs:
                            if hasattr(job, "metrics"):
                                metrics = job.metrics
                                rows = getattr(metrics, "rows", 0) or 0
                                bytes_val = getattr(metrics, "bytes", 0) or 0
                                total_rows += rows
                                total_bytes += bytes_val

                                if rows > 0:
                                    job_details.append(
                                        f"- {getattr(job, 'job_file_info', {}).get('table_name', 'unknown')}: "
                                        f"{rows:,} rows, {bytes_val:,} bytes"
                                    )

                        if total_rows > 0:
                            # Standard Dagster metadata
                            output_metadata["dagster/row_count"] = MetadataValue.int(total_rows)
                            # Custom metadata
                            output_metadata["total_rows_loaded"] = MetadataValue.int(total_rows)
                            output_metadata["total_bytes_loaded"] = MetadataValue.int(total_bytes)
                            output_metadata["rows_per_second"] = MetadataValue.float(
                                total_rows / duration if duration > 0 else 0
                            )
                            # Format bytes in human-readable form
                            if total_bytes > 0:
                                mb = total_bytes / (1024 * 1024)
                                output_metadata["data_size_mb"] = MetadataValue.float(round(mb, 2))

                        if job_details:
                            output_metadata["load_details"] = MetadataValue.md("\n".join(job_details))

                    # Pipeline state and trace
                    if hasattr(load_package, "state"):
                        state = load_package.state
                        output_metadata["pipeline_state"] = MetadataValue.text(str(state))

                if partition_key:
                    output_metadata["partition_key"] = MetadataValue.text(partition_key)

                # Add source configuration metadata
                if pipeline_info.metadata:
                    if pipeline_info.metadata.group_name:
                        output_metadata["group"] = MetadataValue.text(pipeline_info.metadata.group_name)
                    if pipeline_info.metadata.tags:
                        output_metadata["tags"] = MetadataValue.json(pipeline_info.metadata.tags)

                logger.info(f"✅ Pipeline completed in {duration:.2f}s")

                return Output(
                    value={"status": "success", "result": str(result)},
                    metadata=output_metadata,
                )

            except Exception as e:
                logger.error(f"❌ Error running dlt pipeline: {e}")
                raise

        return dlt_pipeline_asset

    def _build_sling_asset(self, replication_info: SlingReplicationInfo, repo_path: str):
        """Build a Dagster asset for a Sling replication with rich metadata."""
        metadata = replication_info.metadata or DagsterMetadata()

        # Build partitions if configured
        partitions_def = None
        if metadata.partitions:
            partitions_def = self._build_partitions_def(metadata.partitions)

        # Build custom metadata
        custom_metadata = {}
        if metadata.metadata:
            for entry in metadata.metadata:
                if entry.type == "url":
                    custom_metadata[entry.key] = MetadataValue.url(str(entry.value))
                elif entry.type == "path":
                    custom_metadata[entry.key] = MetadataValue.path(str(entry.value))
                elif entry.type == "md":
                    custom_metadata[entry.key] = MetadataValue.md(str(entry.value))
                elif entry.type == "json":
                    custom_metadata[entry.key] = MetadataValue.json(entry.value)
                else:
                    custom_metadata[entry.key] = MetadataValue.text(str(entry.value))

        # Build retry policy from metadata (new format with fallback to legacy)
        retry_policy = None
        if metadata.retry_policy:
            # New format with backoff and jitter
            policy = metadata.retry_policy
            backoff = Backoff.EXPONENTIAL if policy.backoff == "EXPONENTIAL" else Backoff.LINEAR
            jitter = None
            if policy.jitter == "FULL":
                jitter = Jitter.FULL
            elif policy.jitter == "PLUS_MINUS":
                jitter = Jitter.PLUS_MINUS

            retry_policy = RetryPolicy(
                max_retries=policy.max_retries,
                delay=policy.delay,
                backoff=backoff,
                jitter=jitter,
            )
        elif metadata.retries and metadata.retries > 0:
            # Legacy format
            retry_policy = RetryPolicy(
                max_retries=metadata.retries,
                delay=metadata.retry_delay or 60,
                backoff=Backoff.LINEAR,  # Default to linear for legacy
            )

        # Build asset tags including kinds
        asset_tags = {
            **metadata.tags,
            "elt_type": "sling",
            "replication": replication_info.name
        }
        # Add kinds to tags (Dagster uses tag prefix "dagster/kind/" for kinds)
        for kind in metadata.kinds:
            asset_tags[f"dagster/kind/{kind}"] = ""

        @asset(
            name=f"sling_{replication_info.name}",
            group_name=metadata.group_name,
            tags=asset_tags,
            description=metadata.description
            or f"Sling replication: {replication_info.name}",
            metadata=custom_metadata,
            partitions_def=partitions_def,
            owners=metadata.owners or [],
            retry_policy=retry_policy,
        )
        def sling_replication_asset(context: AssetExecutionContext):
            """Execute standalone Sling replication with rich metadata."""
            logger.info(f"Running Sling replication: {replication_info.name}")

            # Environment variables are passed through automatically
            # Sling will fail with clear errors if required credentials are missing

            # Get partition key if partitioned
            partition_key = None
            if context.has_partition_key:
                partition_key = context.partition_key
                logger.info(f"Running for partition: {partition_key}")

            try:
                start_time = datetime.now()

                # Run sling CLI directly
                result = subprocess.run(
                    ["sling", "run", "-r", str(replication_info.replication_yaml)],
                    cwd=repo_path,
                    capture_output=True,
                    text=True,
                    check=True,
                )

                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()

                # Parse comprehensive Sling output for metadata
                output_metadata = {
                    "replication_name": MetadataValue.text(replication_info.name),
                    "replication_type": MetadataValue.text("sling"),
                    "duration_seconds": MetadataValue.float(duration),
                    "execution_time": MetadataValue.timestamp(end_time.timestamp()),
                    "start_time": MetadataValue.timestamp(start_time.timestamp()),
                    "replication_path": MetadataValue.path(str(replication_info.replication_yaml)),
                    "sling_output": MetadataValue.md(f"```\n{result.stdout}\n```"),
                }

                # Add source code link if GitHub repo is available
                if self.params.repo_url and self.params.repo_branch:
                    repo_url = self.params.repo_url.rstrip('.git')
                    replication_rel_path = replication_info.replication_yaml.relative_to(
                        replication_info.replication_yaml.parent.parent.parent
                    )
                    source_url = f"{repo_url}/blob/{self.params.repo_branch}/{replication_rel_path}"
                    output_metadata["dagster/code_references"] = MetadataValue.url(source_url)

                # Parse Sling output for statistics
                stdout_lines = result.stdout.split("\n")
                total_rows = 0
                total_bytes = 0
                streams_processed = []

                for line in stdout_lines:
                    # Extract row counts (Sling typically outputs "Loaded X rows")
                    if "rows" in line.lower() and any(char.isdigit() for char in line):
                        import re
                        numbers = re.findall(r"[\d,]+", line)
                        if numbers:
                            try:
                                rows = int(numbers[0].replace(",", ""))
                                total_rows += rows
                            except (ValueError, IndexError):
                                pass

                    # Extract stream names
                    if "->" in line or "stream:" in line.lower():
                        streams_processed.append(line.strip())

                if total_rows > 0:
                    # Standard Dagster metadata
                    output_metadata["dagster/row_count"] = MetadataValue.int(total_rows)
                    # Custom metadata
                    output_metadata["total_rows_replicated"] = MetadataValue.int(total_rows)
                    output_metadata["rows_per_second"] = MetadataValue.float(
                        total_rows / duration if duration > 0 else 0
                    )

                if streams_processed:
                    output_metadata["streams_replicated"] = MetadataValue.md(
                        "\n".join([f"- {stream}" for stream in streams_processed[:10]])  # Limit to 10
                    )
                    output_metadata["stream_count"] = MetadataValue.int(len(streams_processed))

                if partition_key:
                    output_metadata["partition_key"] = MetadataValue.text(partition_key)

                # Add replication configuration metadata
                if replication_info.metadata:
                    if replication_info.metadata.group_name:
                        output_metadata["group"] = MetadataValue.text(replication_info.metadata.group_name)
                    if replication_info.metadata.tags:
                        output_metadata["tags"] = MetadataValue.json(replication_info.metadata.tags)

                logger.info(f"✅ Replication completed in {duration:.2f}s")
                logger.info(f"Sling output:\n{result.stdout}")

                return Output(
                    value={"status": "success", "output": result.stdout},
                    metadata=output_metadata,
                )

            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Sling failed: {e.stderr}")
                raise
            except Exception as e:
                logger.error(f"❌ Error running Sling replication: {e}")
                raise

        return sling_replication_asset

    def _build_schedule(self, schedule_name: str, schedule_config, asset_name: str):
        """Build a Dagster schedule from configuration."""
        # Convert string status to enum
        status_str = schedule_config.default_status.upper()
        default_status = DefaultScheduleStatus.RUNNING if status_str == "RUNNING" else DefaultScheduleStatus.STOPPED

        return ScheduleDefinition(
            name=schedule_name,
            target=AssetSelection.keys(asset_name),
            cron_schedule=schedule_config.cron_schedule,
            execution_timezone=schedule_config.execution_timezone or schedule_config.timezone,
            default_status=default_status,
        )
