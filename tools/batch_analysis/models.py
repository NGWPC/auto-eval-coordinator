"""Data models for batch analysis."""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class JobStatus(Enum):
    """Job status enumeration aligned with Nomad JobStatus values."""
    DISPATCHED = "dispatched"
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LOST = "lost"
    STOPPED = "stopped"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


@dataclass
class DebugConfig:
    """Configuration for pipeline debugging."""

    batch_name: str
    time_range_days: int
    output_dir: str
    pipeline_log_group: str
    job_log_group: str  
    s3_output_root: Optional[str] = None
    generate_html: bool = False
    aoi_list_path: Optional[str] = None
    collection: Optional[str] = None


@dataclass
class JobInfo:
    """Information about a job with any status."""

    pipeline_log_stream: str
    job_log_stream: str
    job_status: JobStatus
    timestamp: Optional[str] = None
    error_messages: Optional[List[str]] = None
    exit_code: Optional[int] = None


@dataclass
class FailedJobInfo:
    """Information about a failed or lost job (legacy compatibility)."""

    pipeline_log_stream: str
    job_log_stream: str
    error_messages: List[str]
    timestamp: Optional[str] = None
    job_status: str = "FAILED"  # Can be "FAILED" or "LOST"


@dataclass
class UniqueErrorInfo:
    """Information about a unique error pattern with occurrence details."""

    error_pattern: str
    error_type: str  # "json_error", "unhandled_exception", etc.
    occurrence_count: int
    first_occurrence_timestamp: str
    first_occurrence_job_stream: str
    first_occurrence_pipeline_stream: str
    sample_raw_message: str
    affected_job_streams: List[str]


@dataclass
class FailedPipelineInfo:
    """Information about a pipeline that failed for non-job reasons."""

    pipeline_log_stream: str
    aoi_name: str
    batch_name: str
    collection: Optional[str]
    timestamp: Optional[str] = None
    failure_reason: str = "No 'INFO Pipeline SUCCESS' message found"


@dataclass
class JobStatusAnalysisResult:
    """Results from comprehensive job status analysis."""

    all_jobs: List[JobInfo]
    jobs_by_status: Dict[JobStatus, List[JobInfo]]
    status_counts: Dict[JobStatus, int]
    
    # Legacy compatibility
    failed_jobs: List[FailedJobInfo]
    unique_errors: List[UniqueErrorInfo]
    unhandled_exceptions: List[FailedJobInfo]
    unique_unhandled_exceptions: List[UniqueErrorInfo]


@dataclass
class ErrorAnalysisResult:
    """Results from error analysis with both detailed and deduplicated information."""

    failed_jobs: List[FailedJobInfo]
    unique_errors: List[UniqueErrorInfo]
    unhandled_exceptions: List[FailedJobInfo]
    unique_unhandled_exceptions: List[UniqueErrorInfo]