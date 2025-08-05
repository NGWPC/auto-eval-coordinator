"""CloudWatch Logs analysis for pipeline debugging."""

import logging
import re
import time
from collections import defaultdict
from typing import Any, Dict, List, Set

import boto3

from .error_patterns import ErrorPatternExtractor
from .models import (
    DebugConfig,
    ErrorAnalysisResult,
    FailedJobInfo,
    FailedPipelineInfo,
    JobInfo,
    JobStatus,
    JobStatusAnalysisResult,
    UniqueErrorInfo,
)

logger = logging.getLogger(__name__)


class CloudWatchAnalyzer:
    """Handles CloudWatch Logs analysis for pipeline debugging."""

    def __init__(self, config: DebugConfig):
        self.config = config
        # Use cloudwatch profile for CloudWatch
        session = boto3.Session(
            profile_name="cloudwatch", region_name="us-east-1"
        )
        self.client = session.client("logs")
        self.error_extractor = ErrorPatternExtractor()

    def run_query(self, log_group: str, query_string: str) -> List[Dict]:
        """Execute a CloudWatch Logs Insights query and return results."""
        try:
            start_time = int(
                (time.time() - self.config.time_range_days * 86400) * 1000
            )
            end_time = int(time.time() * 1000)

            logger.info(
                f"Running query on {log_group}: {query_string[:100]}..."
            )

            start_query_response = self.client.start_query(
                logGroupName=log_group,
                startTime=start_time,
                endTime=end_time,
                queryString=query_string,
            )
            query_id = start_query_response["queryId"]

            # Poll for completion
            response = None
            while response is None or response["status"] in [
                "Running",
                "Scheduled",
            ]:
                logger.info("Waiting for query to complete...")
                time.sleep(2)
                response = self.client.get_query_results(queryId=query_id)

            logger.info(
                f"Query completed with {len(response['results'])} results"
            )
            return response["results"]

        except Exception as e:
            logger.error(f"Error running query on {log_group}: {e}")
            return []

    def find_submitted_pipelines(self) -> Set[str]:
        """Find all pipelines that were submitted for this batch."""
        logger.info("Finding submitted pipelines...")

        # Build filter pattern based on whether collection is specified
        if self.config.collection:
            stream_filter = f"/pipeline\\/dispatch-\\[batch_name={self.config.batch_name},.*collection={self.config.collection}.*\\]/"
        else:
            stream_filter = f"/pipeline\\/dispatch-\\[batch_name={self.config.batch_name},.*\\]/"

        submitted_query = f"""
        fields @logStream
        | filter @logStream like {stream_filter}
        | stats count() by @logStream
        """

        results = self.run_query(
            self.config.pipeline_log_group, submitted_query
        )

        submitted_streams = set()
        for result in results:
            stream = self._get_field_value(result, "@logStream")
            if stream:
                submitted_streams.add(stream)

        logger.info(
            f"Found {len(submitted_streams)} submitted pipeline streams"
        )
        return submitted_streams

    def find_missing_pipelines(self, expected_aois: List[str]) -> List[str]:
        """Find AOIs that were expected but have no corresponding pipeline log streams."""
        logger.info(
            f"Checking for missing pipelines from {len(expected_aois)} expected AOIs..."
        )

        # Get all submitted pipeline streams
        submitted_streams = self.find_submitted_pipelines()

        # Extract AOI names from log stream names
        # Pattern: pipeline/dispatch-[batch_name=X,aoi_name=Y,collection=Z]
        if self.config.collection:
            # If collection is specified, include it in the pattern
            pattern = (
                r"pipeline/dispatch-\[batch_name="
                + re.escape(self.config.batch_name)
                + r",aoi_name=([^,\]]+),collection="
                + re.escape(self.config.collection)
                + r"\]"
            )
        else:
            # Otherwise, match any collection
            pattern = (
                r"pipeline/dispatch-\[batch_name="
                + re.escape(self.config.batch_name)
                + r",aoi_name=([^,\]]+)(?:,collection=[^\]]+)?\]"
            )

        actual_aois = set()
        for stream in submitted_streams:
            match = re.search(pattern, stream)
            if match:
                aoi_name = match.group(1)
                actual_aois.add(aoi_name)

        # Find missing AOIs
        expected_aois_set = set(expected_aois)
        missing_aois = expected_aois_set - actual_aois

        logger.info(f"Expected AOIs: {len(expected_aois_set)}")
        if self.config.collection:
            logger.info(
                f"Found AOIs with logs (collection={self.config.collection}): {len(actual_aois)}"
            )
        else:
            logger.info(f"Found AOIs with logs: {len(actual_aois)}")
        logger.info(f"Missing AOIs: {len(missing_aois)}")

        return sorted(list(missing_aois))

    def _get_field_value(
        self, result: Dict, field_name: str, default: str = "N/A"
    ) -> str:
        """Extract field value from CloudWatch Logs Insights result."""
        for field in result:
            if field.get("field") == field_name:
                return field.get("value", default)
        return default

    def find_failed_pipelines(self) -> List[FailedPipelineInfo]:
        """Find pipelines that failed for non-job reasons (missing 'INFO Pipeline SUCCESS' message)."""
        logger.info("Finding pipelines that failed for non-job reasons...")

        # Build filter pattern based on whether collection is specified
        if self.config.collection:
            stream_filter = f"/pipeline\\/dispatch-\\[batch_name={self.config.batch_name},.*collection={self.config.collection}.*\\]/"
        else:
            stream_filter = f"/pipeline\\/dispatch-\\[batch_name={self.config.batch_name},.*\\]/"

        # Get all submitted pipeline streams
        submitted_streams = self.find_submitted_pipelines()

        if not submitted_streams:
            logger.warning("No submitted pipeline streams found")
            return []

        logger.info(
            f"Checking {len(submitted_streams)} pipeline streams for success messages..."
        )

        failed_pipelines = []

        # Process streams in chunks to avoid query size limits
        chunk_size = 20
        stream_list = list(submitted_streams)

        for i in range(0, len(stream_list), chunk_size):
            chunk = stream_list[i : i + chunk_size]

            # Query for SUCCESS messages in pipeline logs
            streams_filter = " or ".join(
                [f'@logStream = "{stream}"' for stream in chunk]
            )

            success_query = f"""
            fields @logStream, @timestamp
            | filter ({streams_filter}) and @message like /INFO Pipeline SUCCESS/
            | stats count() by @logStream
            """

            success_results = self.run_query(
                self.config.pipeline_log_group, success_query
            )

            # Track which streams have success messages
            successful_streams = set()
            for result in success_results:
                stream = self._get_field_value(result, "@logStream")
                if stream:
                    successful_streams.add(stream)

            # Check for job failures in these streams to distinguish failure types
            job_failure_streams = set()
            if chunk:
                streams_filter = " or ".join(
                    [f'@logStream = "{stream}"' for stream in chunk]
                )
                job_failure_query = f"""
                fields @logStream
                | filter ({streams_filter}) and @message like /JobStatus.(FAILED|LOST)/
                | stats count() by @logStream
                """

                job_failure_results = self.run_query(
                    self.config.pipeline_log_group, job_failure_query
                )
                for result in job_failure_results:
                    stream = self._get_field_value(result, "@logStream")
                    if stream:
                        job_failure_streams.add(stream)

            # Find streams in this chunk that don't have success messages
            for stream in chunk:
                if stream not in successful_streams:
                    # Extract AOI name from stream name
                    aoi_name = self._extract_aoi_from_stream(stream)

                    # Get the latest timestamp for this stream
                    timestamp_query = f"""
                    fields @timestamp
                    | filter @logStream = "{stream}"
                    | sort @timestamp desc
                    | limit 1
                    """

                    timestamp_results = self.run_query(
                        self.config.pipeline_log_group, timestamp_query
                    )
                    timestamp = None
                    if timestamp_results:
                        timestamp = self._get_field_value(
                            timestamp_results[0], "@timestamp"
                        )

                    # Determine failure reason based on whether there were job failures
                    if stream in job_failure_streams:
                        failure_reason = "Pipeline failed with job failures but no SUCCESS message"
                    else:
                        failure_reason = "Pipeline failed without any job failures (silent failure)"

                    failed_pipelines.append(
                        FailedPipelineInfo(
                            pipeline_log_stream=stream,
                            aoi_name=aoi_name,
                            batch_name=self.config.batch_name,
                            collection=self.config.collection,
                            timestamp=timestamp,
                            failure_reason=failure_reason,
                        )
                    )

        logger.info(
            f"Found {len(failed_pipelines)} pipelines that failed for non-job reasons"
        )
        return failed_pipelines

    def _extract_aoi_from_stream(self, stream: str) -> str:
        """Extract AOI name from pipeline log stream name."""
        # Pattern: pipeline/dispatch-[batch_name=X,aoi_name=Y,collection=Z]
        pattern = r"aoi_name=([^,\]]+)"
        match = re.search(pattern, stream)
        if match:
            return match.group(1)
        return "unknown"

    def get_errors_for_failed_jobs_bulk(
        self, failed_job_streams: List[str]
    ) -> Dict[str, List[str]]:
        """
        Retrieve errors for multiple failed job streams in bulk.
        Returns a dictionary mapping job stream to error messages.
        """
        if not failed_job_streams:
            return {}

        logger.info(
            f"Retrieving errors for {len(failed_job_streams)} failed jobs in bulk..."
        )

        # Process in chunks to avoid query size limits
        chunk_size = 25
        all_errors = {}

        for i in range(0, len(failed_job_streams), chunk_size):
            chunk = failed_job_streams[i : i + chunk_size]
            job_streams_filter = " or ".join(
                [f'@logStream = "{stream}"' for stream in chunk]
            )

            # Query for structured JSON errors
            json_errors_query = f"""
            fields @timestamp, @logStream, @message
            | filter ({job_streams_filter})
            | filter @message like /"level": "ERROR"/ or @message like /"level": "CRITICAL"/ or @message like /"level": "FATAL"/
            | sort @logStream, @timestamp asc
            """

            json_results = self.run_query(
                self.config.job_log_group, json_errors_query
            )

            # Query for unhandled exceptions (non-JSON)
            unhandled_errors_query = f"""
            fields @timestamp, @logStream, @message
            | filter ({job_streams_filter})
            | filter @message not like /^{{/
            | filter @message like /(?i)traceback|exception|error:|fail:|fatal:|critical:/
            | sort @logStream, @timestamp asc
            """

            unhandled_results = self.run_query(
                self.config.job_log_group, unhandled_errors_query
            )

            # Combine and process results
            for result in json_results + unhandled_results:
                job_stream = self._get_field_value(result, "@logStream")
                message = self._get_field_value(result, "@message")

                if message and message != "Parse Error":
                    if job_stream not in all_errors:
                        all_errors[job_stream] = []
                    if (
                        message not in all_errors[job_stream]
                    ):  # Avoid duplicates
                        all_errors[job_stream].append(message)

        # Ensure all requested streams have an entry
        for stream in failed_job_streams:
            if stream not in all_errors:
                all_errors[stream] = [
                    "No error messages found in job log stream"
                ]

        return all_errors

    def find_all_jobs_consolidated(self) -> Dict[str, Any]:
        """
        Find all jobs and their statuses in a single consolidated query.
        Returns comprehensive job information with better status grouping.
        """
        logger.info("Running consolidated job status query...")

        # Build filter pattern based on whether collection is specified
        if self.config.collection:
            stream_filter = f"/pipeline\\/dispatch-\\[batch_name={self.config.batch_name},.*collection={self.config.collection}.*\\]/"
        else:
            stream_filter = f"/pipeline\\/dispatch-\\[batch_name={self.config.batch_name},.*\\]/"

        # Single comprehensive query to get all job statuses and error details
        consolidated_query = f"""
        fields @timestamp, @logStream as pipeline_log_stream, @message
        | parse @message 'Job * ' as job_stream_name
        | parse @message 'JobStatus.*' as job_status_raw
        | parse @message 'exit_code=*' as exit_code_str
        | filter @logStream like {stream_filter}
        | filter @message like /JobStatus\\./
        | sort @timestamp desc
        | limit 100 #REMOVE after working!
        """

        results = self.run_query(
            self.config.pipeline_log_group, consolidated_query
        )

        # Process results into structured format
        jobs_by_stream = {}
        all_jobs = []

        for result in results:
            timestamp = self._get_field_value(result, "@timestamp")
            pipeline_stream = self._get_field_value(
                result, "pipeline_log_stream"
            )
            job_stream = self._get_field_value(
                result, "job_stream_name", ""
            ).strip()
            job_status_raw = self._get_field_value(result, "job_status_raw", "")
            exit_code_str = self._get_field_value(result, "exit_code_str", "")

            if not job_stream or job_stream == "N/A":
                continue

            # Extract status from raw string
            job_status = "UNKNOWN"
            for status in [
                "DISPATCHED",
                "PENDING",
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "LOST",
                "STOPPED",
                "CANCELLED",
            ]:
                if status in job_status_raw:
                    job_status = status
                    break

            # Extract exit code if available
            exit_code = None
            if exit_code_str and exit_code_str != "N/A":
                try:
                    exit_code = int(exit_code_str.split(",")[0].strip())
                except:
                    pass

            job_info = {
                "timestamp": timestamp,
                "pipeline_log_stream": pipeline_stream,
                "job_log_stream": job_stream,
                "job_status": job_status,
                "exit_code": exit_code,
                "job_status_raw": job_status_raw,
            }

            # Keep only the latest status for each job
            if (
                job_stream not in jobs_by_stream
                or timestamp > jobs_by_stream[job_stream]["timestamp"]
            ):
                jobs_by_stream[job_stream] = job_info

        # Convert to list
        all_jobs = list(jobs_by_stream.values())

        # Group by status categories
        status_groups = {
            "success": [],
            "application_failures": [],  # FAILED (exit code != 0)
            "infrastructure_issues": [],  # LOST (timeout/node failures)
            "intentional_stops": [],  # STOPPED, CANCELLED
            "in_progress": [],  # PENDING, RUNNING, DISPATCHED
            "unknown": [],
        }

        # Also maintain raw status groups for compatibility
        jobs_by_status = {
            status: []
            for status in [
                "DISPATCHED",
                "PENDING",
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "LOST",
                "STOPPED",
                "CANCELLED",
                "UNKNOWN",
            ]
        }

        for job in all_jobs:
            status = job["job_status"]
            jobs_by_status[status].append(job)

            # Categorize into groups
            if status == "SUCCEEDED":
                status_groups["success"].append(job)
            elif status == "FAILED":
                status_groups["application_failures"].append(job)
            elif status == "LOST":
                status_groups["infrastructure_issues"].append(job)
            elif status in ["STOPPED", "CANCELLED"]:
                status_groups["intentional_stops"].append(job)
            elif status in ["PENDING", "RUNNING", "DISPATCHED"]:
                status_groups["in_progress"].append(job)
            else:
                status_groups["unknown"].append(job)

        # Calculate summary statistics
        total_jobs = len(all_jobs)
        terminal_jobs = (
            len(status_groups["success"])
            + len(status_groups["application_failures"])
            + len(status_groups["infrastructure_issues"])
            + len(status_groups["intentional_stops"])
        )

        summary = {
            "total_jobs": total_jobs,
            "terminal_jobs": terminal_jobs,
            "in_progress_jobs": len(status_groups["in_progress"]),
            "success_rate": round(
                (len(status_groups["success"]) / total_jobs * 100), 2
            )
            if total_jobs > 0
            else 0,
            "failure_rate": round(
                (len(status_groups["application_failures"]) / total_jobs * 100),
                2,
            )
            if total_jobs > 0
            else 0,
            "infrastructure_failure_rate": round(
                (
                    len(status_groups["infrastructure_issues"])
                    / total_jobs
                    * 100
                ),
                2,
            )
            if total_jobs > 0
            else 0,
        }

        logger.info(
            f"Consolidated query found {total_jobs} jobs: "
            f"{len(status_groups['success'])} succeeded, "
            f"{len(status_groups['application_failures'])} failed, "
            f"{len(status_groups['infrastructure_issues'])} lost, "
            f"{len(status_groups['in_progress'])} in progress"
        )

        return {
            "all_jobs": all_jobs,
            "jobs_by_status": jobs_by_status,
            "status_groups": status_groups,
            "summary": summary,
        }

    def analyze_batch_comprehensive(self) -> Dict[str, Any]:
        """
        Comprehensive batch analysis with consolidated queries and improved grouping.
        This replaces multiple separate queries with efficient bulk operations.
        """
        logger.info("Starting comprehensive batch analysis...")

        # Step 1: Get all jobs and their statuses in one query
        job_analysis = self.find_all_jobs_consolidated()
        all_jobs = job_analysis["all_jobs"]
        status_groups = job_analysis["status_groups"]
        summary = job_analysis["summary"]

        # Step 2: Get errors for all failed/lost jobs in bulk
        failed_jobs = (
            status_groups["application_failures"]
            + status_groups["infrastructure_issues"]
        )
        failed_job_streams = [job["job_log_stream"] for job in failed_jobs]

        error_messages_by_stream = {}
        if failed_job_streams:
            error_messages_by_stream = self.get_errors_for_failed_jobs_bulk(
                failed_job_streams
            )

        # Step 3: Analyze error patterns and create unique error analysis
        error_patterns = defaultdict(list)

        for job in failed_jobs:
            job_stream = job["job_log_stream"]
            error_messages = error_messages_by_stream.get(job_stream, [])

            for message in error_messages:
                pattern, error_type = (
                    self.error_extractor.extract_json_error_pattern(message)
                )

                error_patterns[pattern].append(
                    {
                        "job_stream": job_stream,
                        "pipeline_stream": job["pipeline_log_stream"],
                        "timestamp": job["timestamp"],
                        "raw_message": message,
                        "error_type": error_type,
                        "job_status": job["job_status"],
                        "exit_code": job.get("exit_code"),
                    }
                )

        # Create unique error summary
        unique_errors = []
        for pattern, occurrences in error_patterns.items():
            # Sort by timestamp to get first occurrence
            occurrences.sort(key=lambda x: x["timestamp"])
            first_occurrence = occurrences[0]

            # Group affected jobs by status
            failed_count = len(
                [o for o in occurrences if o["job_status"] == "FAILED"]
            )
            lost_count = len(
                [o for o in occurrences if o["job_status"] == "LOST"]
            )

            unique_errors.append(
                UniqueErrorInfo(
                    error_pattern=pattern,
                    error_type=first_occurrence["error_type"],
                    occurrence_count=len(occurrences),
                    first_occurrence_timestamp=first_occurrence["timestamp"],
                    first_occurrence_job_stream=first_occurrence["job_stream"],
                    first_occurrence_pipeline_stream=first_occurrence[
                        "pipeline_stream"
                    ],
                    sample_raw_message=first_occurrence["raw_message"],
                    affected_job_streams=[o["job_stream"] for o in occurrences],
                )
            )

        # Sort by total occurrence count
        unique_errors.sort(key=lambda x: x.occurrence_count, reverse=True)

        # Step 4: Check pipeline health
        submitted_pipelines = self.find_submitted_pipelines()
        failed_pipelines = self.find_failed_pipelines()

        # Step 5: Create actionable insights
        action_items = []

        # High-priority: Infrastructure issues
        if len(status_groups["infrastructure_issues"]) > 0:
            action_items.append(
                {
                    "priority": "HIGH",
                    "category": "Infrastructure",
                    "issue": f"{len(status_groups['infrastructure_issues'])} jobs lost due to infrastructure issues",
                    "recommendation": "Check Nomad cluster health, node availability, and timeout settings",
                    "affected_jobs": len(
                        status_groups["infrastructure_issues"]
                    ),
                }
            )

        # High-priority: Common application errors
        for error in unique_errors[:3]:  # Top 3 errors
            if error.occurrence_count >= 5:
                action_items.append(
                    {
                        "priority": "HIGH",
                        "category": "Application Error",
                        "issue": f"Error pattern affecting {error.occurrence_count} jobs: {error.error_pattern[:100]}",
                        "recommendation": "Fix this common error pattern to improve success rate",
                        "affected_jobs": error.occurrence_count,
                    }
                )

        # Medium-priority: Failed pipelines
        if len(failed_pipelines) > 0:
            action_items.append(
                {
                    "priority": "MEDIUM",
                    "category": "Pipeline Health",
                    "issue": f"{len(failed_pipelines)} pipelines failed without job failures",
                    "recommendation": "Check pipeline orchestration logic and error handling",
                    "affected_jobs": 0,
                }
            )

        logger.info(
            f"Comprehensive analysis complete: {summary['total_jobs']} jobs analyzed, "
            f"{len(unique_errors)} unique error patterns found, "
            f"{len(action_items)} action items identified"
        )

        return {
            "summary": summary,
            "status_groups": status_groups,
            "jobs_by_status": job_analysis["jobs_by_status"],
            "unique_errors": unique_errors,
            "failed_pipelines": failed_pipelines,
            "submitted_pipelines": submitted_pipelines,
            "action_items": action_items,
            "detailed_errors": error_messages_by_stream,
        }
