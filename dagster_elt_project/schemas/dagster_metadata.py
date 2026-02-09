"""Pydantic models for dagster.yaml metadata configuration."""

from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field


class ScheduleConfig(BaseModel):
    """Schedule configuration for pipelines."""

    enabled: bool = Field(default=False, description="Whether scheduling is enabled")
    cron_schedule: str = Field(..., description="Cron expression for the schedule")
    timezone: str = Field(default="UTC", description="Timezone for the schedule")
    execution_timezone: Optional[str] = Field(
        default=None,
        description="Override timezone for execution (defaults to timezone)"
    )
    default_status: str = Field(
        default="RUNNING",
        description="Default status for the schedule (RUNNING or STOPPED)"
    )


class TimePartitionConfig(BaseModel):
    """Time-based partition configuration."""

    start: str = Field(..., description="Start date/time for partitions")
    end: Optional[str] = Field(default=None, description="End date/time for partitions")
    cron_schedule: str = Field(..., description="Cron expression for partition cadence")
    timezone: str = Field(default="UTC", description="Timezone for partitions")
    fmt: str = Field(default="%Y-%m-%d", description="Date format for partition keys")


class StaticPartitionConfig(BaseModel):
    """Static partition configuration."""

    partition_keys: List[str] = Field(..., description="List of static partition keys")


class PartitionsConfig(BaseModel):
    """Partitions configuration for pipelines."""

    enabled: bool = Field(default=False, description="Whether partitioning is enabled")
    type: str = Field(..., description="Type of partitioning (time or static)")
    time: Optional[TimePartitionConfig] = Field(
        default=None,
        description="Time partition configuration"
    )
    static: Optional[StaticPartitionConfig] = Field(
        default=None,
        description="Static partition configuration"
    )


class MetadataEntry(BaseModel):
    """Custom metadata entry for assets."""

    key: str = Field(..., description="Metadata key")
    value: Any = Field(..., description="Metadata value")
    type: str = Field(
        default="text",
        description="Metadata type (text, url, path, md, json, int, float)"
    )


class RetryPolicyConfig(BaseModel):
    """Retry policy configuration."""

    max_retries: int = Field(default=3, description="Maximum number of retries", ge=0, le=10)
    delay: int = Field(default=60, description="Initial delay in seconds", ge=0)
    backoff: str = Field(default="LINEAR", description="Backoff strategy (LINEAR or EXPONENTIAL)")
    jitter: Optional[str] = Field(default=None, description="Jitter strategy (FULL or PLUS_MINUS)")


class DagsterMetadata(BaseModel):
    """Complete Dagster metadata configuration from dagster.yaml."""

    enabled: bool = Field(default=True, description="Whether this pipeline is enabled")
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of the pipeline"
    )
    group_name: str = Field(
        default="default",
        description="Dagster asset group name",
        alias="group"  # Accept 'group' in YAML
    )
    owners: List[str] = Field(default_factory=list, description="List of owner emails")
    tags: Dict[str, str] = Field(default_factory=dict, description="Custom tags")
    kinds: List[str] = Field(default_factory=list, description="Asset kinds for categorization and icons")

    # Scheduling
    schedule: Optional[ScheduleConfig] = Field(
        default=None,
        description="Schedule configuration"
    )

    # Partitioning
    partitions: Optional[PartitionsConfig] = Field(
        default=None,
        description="Partitions configuration"
    )

    # Custom metadata
    metadata: List[MetadataEntry] = Field(
        default_factory=list,
        description="Custom metadata entries to display in Dagster UI"
    )

    # Retry configuration (new format)
    retry_policy: Optional[RetryPolicyConfig] = Field(
        default=None,
        description="Retry policy configuration with backoff and jitter"
    )

    # Legacy retry fields (for backward compatibility)
    retries: Optional[int] = Field(default=None, description="(Legacy) Number of retries", ge=0, le=10)
    retry_delay: Optional[int] = Field(default=None, description="(Legacy) Delay in seconds", ge=0)

    class Config:
        extra = "allow"  # Allow additional fields for extensibility
        populate_by_name = True  # Allow field name or alias
