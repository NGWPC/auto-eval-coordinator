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
    ) -> Dict[str, Any]:
        """
        Retrieve errors for multiple failed job streams in bulk.
        Returns a dictionary with structured error information:
        {
            'all_errors': {job_stream: [error_messages]},
            'json_errors': {job_stream: [json_error_messages]},
            'unhandled_exceptions': {job_stream: [exception_messages]}
        }
        """
        if not failed_job_streams:
            return {
                "all_errors": {},
                "json_errors": {},
                "unhandled_exceptions": {},
            }

        logger.info(
            f"Retrieving errors for {len(failed_job_streams)} failed jobs in bulk..."
        )

        # Process in chunks to avoid query size limits
        chunk_size = 25
        all_errors = {}
        json_errors = {}
        unhandled_exceptions = {}

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

            # Process JSON errors separately
            for result in json_results:
                job_stream = self._get_field_value(result, "@logStream")
                message = self._get_field_value(result, "@message")

                if message and message != "Parse Error":
                    # Add to all_errors
                    if job_stream not in all_errors:
                        all_errors[job_stream] = []
                    if message not in all_errors[job_stream]:
                        all_errors[job_stream].append(message)

                    # Add to json_errors
                    if job_stream not in json_errors:
                        json_errors[job_stream] = []
                    if message not in json_errors[job_stream]:
                        json_errors[job_stream].append(message)

            # Process unhandled exceptions separately
            for result in unhandled_results:
                job_stream = self._get_field_value(result, "@logStream")
                message = self._get_field_value(result, "@message")

                if message and message != "Parse Error":
                    # Add to all_errors
                    if job_stream not in all_errors:
                        all_errors[job_stream] = []
                    if message not in all_errors[job_stream]:
                        all_errors[job_stream].append(message)

                    # Add to unhandled_exceptions
                    if job_stream not in unhandled_exceptions:
                        unhandled_exceptions[job_stream] = []
                    if message not in unhandled_exceptions[job_stream]:
                        unhandled_exceptions[job_stream].append(message)

        # Ensure all requested streams have an entry in all error collections
        for stream in failed_job_streams:
            if stream not in all_errors:
                all_errors[stream] = [
                    "No error messages found in job log stream"
                ]
            if stream not in json_errors:
                json_errors[stream] = []
            if stream not in unhandled_exceptions:
                unhandled_exceptions[stream] = []

        return {
            "all_errors": all_errors,
            "json_errors": json_errors,
            "unhandled_exceptions": unhandled_exceptions,
        }

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

        # Step 2: Get errors for failed jobs only (exclude LOST jobs as they are infrastructure issues)
        failed_jobs = status_groups[
            "application_failures"
        ]  # Only FAILED jobs, not LOST
        failed_job_streams = [job["job_log_stream"] for job in failed_jobs]

        error_results = {}
        error_messages_by_stream = {}
        unhandled_exceptions_by_stream = {}
        if failed_job_streams:
            error_results = self.get_errors_for_failed_jobs_bulk(
                failed_job_streams
            )
            error_messages_by_stream = error_results.get("all_errors", {})
            unhandled_exceptions_by_stream = error_results.get(
                "unhandled_exceptions", {}
            )

        # Step 2.1: Analyze terminal status jobs (STOPPED, CANCELLED, LOST) for detailed failure information
        terminal_jobs = (
            status_groups["infrastructure_issues"] +  # LOST jobs
            status_groups["intentional_stops"]  # STOPPED, CANCELLED jobs
        )
        terminal_status_details = {}
        if terminal_jobs:
            terminal_status_details = self.get_terminal_status_details(terminal_jobs)

        # Step 2.2: Extract nomad failure information from pipeline logs
        nomad_failures = self.extract_nomad_failures()

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

        # High-priority: Driver Failures (specific nomad issues)
        if nomad_failures["summary"]["total_driver_failures"] > 0:
            action_items.append(
                {
                    "priority": "HIGH",
                    "category": "Nomad Driver Failure",
                    "issue": f"{nomad_failures['summary']['total_driver_failures']} driver failures detected in pipeline logs",
                    "recommendation": "Investigate driver failures which indicate issues with job execution environment, resource allocation, or container runtime problems.",
                    "affected_jobs": nomad_failures["summary"]["total_driver_failures"],
                    "details": f"Affected job count: {len(set([f['job_id'] for f in nomad_failures['driver_failures']]))}",
                }
            )

        # High-priority: Infrastructure issues (LOST jobs) with enhanced analysis
        if len(status_groups["infrastructure_issues"]) > 0:
            lost_with_driver_failures = sum(1 for job in status_groups["infrastructure_issues"] 
                                          if terminal_status_details.get(job["job_log_stream"], {}).get("failure_type") == "driver_failure")
            lost_with_timeouts = sum(1 for job in status_groups["infrastructure_issues"] 
                                   if terminal_status_details.get(job["job_log_stream"], {}).get("failure_type") == "infrastructure_timeout")
            
            action_items.append(
                {
                    "priority": "HIGH",
                    "category": "Infrastructure",
                    "issue": f"{len(status_groups['infrastructure_issues'])} jobs lost due to infrastructure issues",
                    "recommendation": "Check Nomad cluster health, node availability, and timeout settings. Review driver failures and timeout patterns for root cause analysis.",
                    "affected_jobs": len(status_groups["infrastructure_issues"]),
                    "details": f"Driver failures: {lost_with_driver_failures}, Timeouts: {lost_with_timeouts}, Other: {len(status_groups['infrastructure_issues']) - lost_with_driver_failures - lost_with_timeouts}",
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

        # Medium-priority: Intentional stops (STOPPED, CANCELLED) with analysis
        if len(status_groups["intentional_stops"]) > 0:
            stopped_count = len([job for job in status_groups["intentional_stops"] if job["job_status"] == "STOPPED"])
            cancelled_count = len([job for job in status_groups["intentional_stops"] if job["job_status"] == "CANCELLED"])
            
            # Check if we have detailed reasons for these jobs
            with_reasons = sum(1 for job in status_groups["intentional_stops"] 
                             if terminal_status_details.get(job["job_log_stream"], {}).get("failure_reason", "Unknown") != "Unknown reason")
            
            action_items.append(
                {
                    "priority": "MEDIUM",
                    "category": "Intentional Job Termination",
                    "issue": f"{len(status_groups['intentional_stops'])} jobs were stopped or cancelled",
                    "recommendation": "Review reasons for intentional job termination. Check if these represent expected behavior or indicate issues requiring intervention.",
                    "affected_jobs": len(status_groups["intentional_stops"]),
                    "details": f"Stopped: {stopped_count}, Cancelled: {cancelled_count}, With detailed reasons: {with_reasons}",
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

        # Count unhandled exceptions
        unhandled_exceptions_count = sum(
            len(exceptions)
            for exceptions in unhandled_exceptions_by_stream.values()
        )

        logger.info(
            f"Comprehensive analysis complete: {summary['total_jobs']} jobs analyzed, "
            f"{len(status_groups['application_failures'])} FAILED jobs investigated for errors, "
            f"{len(status_groups['infrastructure_issues'])} LOST jobs categorized as infrastructure issues, "
            f"{len(unique_errors)} unique error patterns found, "
            f"{unhandled_exceptions_count} unhandled exceptions found, "
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
            "unhandled_exceptions_count": unhandled_exceptions_count,
            "unhandled_exceptions_by_stream": unhandled_exceptions_by_stream,
            "terminal_status_details": terminal_status_details,
            "nomad_failures": nomad_failures,
        }

    def get_terminal_status_details(self, jobs: List[Dict]) -> Dict[str, Any]:
        """
        Extract detailed information from both job and pipeline logs for terminal status jobs.
        Covers STOPPED, CANCELLED, and LOST jobs with context from logs.
        
        Args:
            jobs: List of job dictionaries with terminal statuses
            
        Returns:
            Dictionary with detailed information for each job including failure reasons
        """
        if not jobs:
            return {}
            
        logger.info(f"Extracting terminal status details for {len(jobs)} jobs...")
        
        terminal_details = {}
        
        # Process jobs in chunks to avoid query size limits
        chunk_size = 20
        
        for i in range(0, len(jobs), chunk_size):
            chunk = jobs[i:i + chunk_size]
            
            # Get job log streams for this chunk
            job_streams = [job["job_log_stream"] for job in chunk]
            pipeline_streams = [job["pipeline_log_stream"] for job in chunk]
            
            # Query job logs for error details
            if job_streams:
                job_streams_filter = " or ".join([f'@logStream = "{stream}"' for stream in job_streams])
                
                job_errors_query = f"""
                fields @timestamp, @logStream, @message
                | filter ({job_streams_filter})
                | filter @message like /(?i)error|fail|exception|stop|cancel|timeout/
                | sort @logStream, @timestamp desc
                | limit 100
                """
                
                job_error_results = self.run_query(self.config.job_log_group, job_errors_query)
                
                # Process job error results
                job_errors_by_stream = {}
                for result in job_error_results:
                    stream = self._get_field_value(result, "@logStream")
                    message = self._get_field_value(result, "@message")
                    timestamp = self._get_field_value(result, "@timestamp")
                    
                    if stream not in job_errors_by_stream:
                        job_errors_by_stream[stream] = []
                    job_errors_by_stream[stream].append({
                        "timestamp": timestamp,
                        "message": message
                    })
            
            # Query pipeline logs for status change context
            if pipeline_streams:
                pipeline_streams_filter = " or ".join([f'@logStream = "{stream}"' for stream in pipeline_streams])
                
                pipeline_context_query = f"""
                fields @timestamp, @logStream, @message
                | filter ({pipeline_streams_filter})
                | filter @message like /JobStatus\.(STOPPED|CANCELLED|LOST)|marking.*LOST|Driver Failure/
                | sort @logStream, @timestamp desc
                | limit 100
                """
                
                pipeline_context_results = self.run_query(self.config.pipeline_log_group, pipeline_context_query)
                
                # Process pipeline context results
                pipeline_context_by_stream = {}
                for result in pipeline_context_results:
                    stream = self._get_field_value(result, "@logStream")
                    message = self._get_field_value(result, "@message")
                    timestamp = self._get_field_value(result, "@timestamp")
                    
                    if stream not in pipeline_context_by_stream:
                        pipeline_context_by_stream[stream] = []
                    pipeline_context_by_stream[stream].append({
                        "timestamp": timestamp,
                        "message": message
                    })
            
            # Combine information for each job in this chunk
            for job in chunk:
                job_stream = job["job_log_stream"]
                pipeline_stream = job["pipeline_log_stream"]
                job_status = job["job_status"]
                
                # Extract failure reason based on status and available context
                failure_reason = "Unknown reason"
                failure_type = "unknown"
                nomad_details = {}
                
                # Get context from logs
                job_errors = job_errors_by_stream.get(job_stream, [])
                pipeline_context = pipeline_context_by_stream.get(pipeline_stream, [])
                
                # Analyze based on job status
                if job_status == "STOPPED":
                    failure_type = "intentional_stop"
                    if pipeline_context:
                        # Look for reasons in pipeline logs
                        for ctx in pipeline_context:
                            if "STOPPED" in ctx["message"]:
                                failure_reason = f"Job stopped: {ctx.get('message', '')[:200]}"
                                break
                        else:
                            failure_reason = "Job was intentionally stopped"
                    
                elif job_status == "CANCELLED":
                    failure_type = "intentional_cancel"
                    if pipeline_context:
                        for ctx in pipeline_context:
                            if "CANCELLED" in ctx["message"]:
                                failure_reason = f"Job cancelled: {ctx.get('message', '')[:200]}"
                                break
                        else:
                            failure_reason = "Job was cancelled"
                    
                elif job_status == "LOST":
                    failure_type = "infrastructure_timeout"
                    
                    # Check for specific LOST reasons in pipeline logs
                    driver_failure = False
                    timeout_reason = False
                    
                    for ctx in pipeline_context:
                        msg = ctx.get("message", "")
                        if "Driver Failure" in msg:
                            failure_type = "driver_failure"
                            failure_reason = f"Driver failure detected: {msg[:200]}"
                            driver_failure = True
                            nomad_details["driver_failure"] = msg
                            break
                        elif "marking" in msg.lower() and "lost" in msg.lower():
                            if "timeout" in msg.lower():
                                failure_reason = f"Job lost due to timeout: {msg[:200]}"
                                timeout_reason = True
                                nomad_details["timeout_reason"] = msg
                            else:
                                failure_reason = f"Job marked as LOST: {msg[:200]}"
                                nomad_details["lost_reason"] = msg
                    
                    if not driver_failure and not timeout_reason:
                        failure_reason = "Job lost due to infrastructure issues (timeout or node failure)"
                
                # Use nomad error pattern extraction for additional context
                all_messages = [err["message"] for err in job_errors] + [ctx["message"] for ctx in pipeline_context]
                if all_messages:
                    for message in all_messages[:3]:  # Check first few messages
                        nomad_pattern, nomad_type = self.error_extractor.extract_nomad_error_pattern(message)
                        if nomad_type.startswith("nomad_"):
                            nomad_details["pattern"] = nomad_pattern
                            nomad_details["type"] = nomad_type
                            if "unknown" in failure_reason.lower():
                                failure_reason = f"Nomad issue detected: {nomad_pattern[:150]}"
                            break
                
                terminal_details[job_stream] = {
                    "job_status": job_status,
                    "failure_type": failure_type,
                    "failure_reason": failure_reason,
                    "nomad_error_details": nomad_details,
                    "job_errors": job_errors[:5],  # Limit to first 5 errors
                    "pipeline_context": pipeline_context[:5],  # Limit to first 5 context messages
                    "pipeline_log_stream": pipeline_stream,
                    "timestamp": job.get("timestamp")
                }
        
        logger.info(f"Extracted terminal status details for {len(terminal_details)} jobs")
        return terminal_details

    def extract_nomad_failures(self) -> Dict[str, Any]:
        """
        Extract nomad failure information from pipeline logs.
        Queries for Driver Failures, dispatch failures, LOST markings, and job status changes.
        
        Returns:
            Dictionary with categorized nomad failure information
        """
        logger.info("Extracting nomad failure information from pipeline logs...")
        
        # Build filter pattern based on whether collection is specified
        if self.config.collection:
            stream_filter = f"/pipeline\\/dispatch-\\[batch_name={self.config.batch_name},.*collection={self.config.collection}.*\\]/"
        else:
            stream_filter = f"/pipeline\\/dispatch-\\[batch_name={self.config.batch_name},.*\\]/"
        
        nomad_failures = {
            "driver_failures": [],
            "dispatch_failures": [],
            "lost_markings": [],
            "timeout_failures": [],
            "status_transitions": [],
            "summary": {
                "total_driver_failures": 0,
                "total_dispatch_failures": 0,
                "total_lost_markings": 0,
                "total_timeout_failures": 0,
                "affected_jobs": set()
            }
        }
        
        # Query for Driver Failures
        driver_failure_query = f"""
        fields @timestamp, @logStream, @message
        | filter @logStream like {stream_filter}
        | filter @message like /Driver Failure/
        | sort @timestamp desc
        | limit 200
        """
        
        driver_results = self.run_query(self.config.pipeline_log_group, driver_failure_query)
        
        for result in driver_results:
            timestamp = self._get_field_value(result, "@timestamp")
            pipeline_stream = self._get_field_value(result, "@logStream")
            message = self._get_field_value(result, "@message")
            
            # Extract job information from message if available
            job_match = re.search(r'Job\s+([^\s]+)', message)
            job_id = job_match.group(1) if job_match else "unknown"
            
            # Extract allocation ID if available
            alloc_match = re.search(r'allocation\s+([a-f0-9-]+)', message, re.IGNORECASE)
            alloc_id = alloc_match.group(1) if alloc_match else "unknown"
            
            failure_info = {
                "timestamp": timestamp,
                "pipeline_log_stream": pipeline_stream,
                "job_id": job_id,
                "allocation_id": alloc_id,
                "message": message,
                "failure_type": "driver_failure"
            }
            
            nomad_failures["driver_failures"].append(failure_info)
            nomad_failures["summary"]["affected_jobs"].add(job_id)
        
        # Query for dispatch failures
        dispatch_failure_query = f"""
        fields @timestamp, @logStream, @message
        | filter @logStream like {stream_filter}
        | filter @message like /Failed to dispatch|dispatch.*fail/
        | sort @timestamp desc
        | limit 100
        """
        
        dispatch_results = self.run_query(self.config.pipeline_log_group, dispatch_failure_query)
        
        for result in dispatch_results:
            timestamp = self._get_field_value(result, "@timestamp")
            pipeline_stream = self._get_field_value(result, "@logStream")
            message = self._get_field_value(result, "@message")
            
            job_match = re.search(r'Job\s+([^\s]+)', message)
            job_id = job_match.group(1) if job_match else "unknown"
            
            failure_info = {
                "timestamp": timestamp,
                "pipeline_log_stream": pipeline_stream,
                "job_id": job_id,
                "message": message,
                "failure_type": "dispatch_failure"
            }
            
            nomad_failures["dispatch_failures"].append(failure_info)
            nomad_failures["summary"]["affected_jobs"].add(job_id)
        
        # Query for LOST markings
        lost_marking_query = f"""
        fields @timestamp, @logStream, @message
        | filter @logStream like {stream_filter}
        | filter @message like /marking.*LOST|JobStatus.*LOST/
        | sort @timestamp desc
        | limit 200
        """
        
        lost_results = self.run_query(self.config.pipeline_log_group, lost_marking_query)
        
        for result in lost_results:
            timestamp = self._get_field_value(result, "@timestamp")
            pipeline_stream = self._get_field_value(result, "@logStream")
            message = self._get_field_value(result, "@message")
            
            job_match = re.search(r'Job\s+([^\s]+)', message)
            job_id = job_match.group(1) if job_match else "unknown"
            
            # Determine if this is a timeout-related LOST
            is_timeout = "timeout" in message.lower()
            
            failure_info = {
                "timestamp": timestamp,
                "pipeline_log_stream": pipeline_stream,
                "job_id": job_id,
                "message": message,
                "failure_type": "lost_marking",
                "is_timeout": is_timeout
            }
            
            nomad_failures["lost_markings"].append(failure_info)
            nomad_failures["summary"]["affected_jobs"].add(job_id)
            
            if is_timeout:
                nomad_failures["timeout_failures"].append(failure_info)
        
        # Query for job status transitions (for context)
        status_transition_query = f"""
        fields @timestamp, @logStream, @message
        | filter @logStream like {stream_filter}
        | filter @message like /JobStatus\.(STOPPED|CANCELLED|LOST|FAILED)/
        | sort @timestamp desc
        | limit 300
        """
        
        transition_results = self.run_query(self.config.pipeline_log_group, status_transition_query)
        
        for result in transition_results:
            timestamp = self._get_field_value(result, "@timestamp")
            pipeline_stream = self._get_field_value(result, "@logStream")
            message = self._get_field_value(result, "@message")
            
            job_match = re.search(r'Job\s+([^\s]+)', message)
            job_id = job_match.group(1) if job_match else "unknown"
            
            # Extract status from message
            status_match = re.search(r'JobStatus\.(\w+)', message)
            status = status_match.group(1) if status_match else "unknown"
            
            transition_info = {
                "timestamp": timestamp,
                "pipeline_log_stream": pipeline_stream,
                "job_id": job_id,
                "status": status,
                "message": message,
                "failure_type": "status_transition"
            }
            
            nomad_failures["status_transitions"].append(transition_info)
            nomad_failures["summary"]["affected_jobs"].add(job_id)
        
        # Update summary counts
        nomad_failures["summary"]["total_driver_failures"] = len(nomad_failures["driver_failures"])
        nomad_failures["summary"]["total_dispatch_failures"] = len(nomad_failures["dispatch_failures"])
        nomad_failures["summary"]["total_lost_markings"] = len(nomad_failures["lost_markings"])
        nomad_failures["summary"]["total_timeout_failures"] = len(nomad_failures["timeout_failures"])
        nomad_failures["summary"]["total_affected_jobs"] = len(nomad_failures["summary"]["affected_jobs"])
        
        # Convert set to list for JSON serialization
        nomad_failures["summary"]["affected_jobs"] = list(nomad_failures["summary"]["affected_jobs"])
        
        logger.info(
            f"Extracted nomad failures: {nomad_failures['summary']['total_driver_failures']} driver failures, "
            f"{nomad_failures['summary']['total_dispatch_failures']} dispatch failures, "
            f"{nomad_failures['summary']['total_lost_markings']} lost markings, "
            f"{nomad_failures['summary']['total_timeout_failures']} timeout failures, "
            f"affecting {nomad_failures['summary']['total_affected_jobs']} jobs"
        )
        
        return nomad_failures
