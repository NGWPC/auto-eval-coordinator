"""CSV report generation for batch analysis results."""

import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Set

from .models import FailedJobInfo, FailedPipelineInfo, UniqueErrorInfo

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generates formatted CSV reports from analysis results."""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_failed_jobs_report(self, failed_jobs: List[FailedJobInfo]) -> str:
        """Generate CSV report of failed jobs with errors."""
        output_file = self.output_dir / "failed_jobs_errors.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["timestamp", "pipeline_log_stream", "job_log_stream", "job_status", "error_messages"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for job in failed_jobs:
                writer.writerow(
                    {
                        "timestamp": job.timestamp or "",
                        "pipeline_log_stream": job.pipeline_log_stream,
                        "job_log_stream": job.job_log_stream,
                        "job_status": job.job_status,
                        "error_messages": "\n\n".join(job.error_messages),
                    }
                )

        logger.info(f"Generated failed jobs report: {output_file}")
        return str(output_file)

    def generate_unhandled_exceptions_report(self, exceptions: List[FailedJobInfo]) -> str:
        """Generate CSV report of unhandled exceptions."""
        output_file = self.output_dir / "unhandled_exceptions.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["pipeline_log_stream", "job_log_stream", "job_status", "unhandled_error_messages"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for job in exceptions:
                writer.writerow(
                    {
                        "pipeline_log_stream": job.pipeline_log_stream,
                        "job_log_stream": job.job_log_stream,
                        "job_status": job.job_status,
                        "unhandled_error_messages": "\n\n".join(job.error_messages),
                    }
                )

        logger.info(f"Generated unhandled exceptions report: {output_file}")
        return str(output_file)

    def generate_missing_pipelines_report(self, submitted: Set[str], expected: Set[str]) -> str:
        """Generate report of missing pipeline executions."""
        output_file = self.output_dir / "missing_pipelines.txt"

        missing = expected - submitted

        with open(output_file, "w") as f:
            f.write(f"Missing Pipeline Executions Report\n")
            f.write(f"====================================\n\n")
            f.write(f"Expected pipelines: {len(expected)}\n")
            f.write(f"Submitted pipelines: {len(submitted)}\n")
            f.write(f"Missing pipelines: {len(missing)}\n\n")

            if missing:
                f.write("Missing pipeline streams:\n")
                for stream in sorted(missing):
                    f.write(f"  {stream}\n")
            else:
                f.write("All expected pipelines were submitted.\n")

        logger.info(f"Generated missing pipelines report: {output_file}")
        return str(output_file)

    def generate_missing_aois_report(self, missing_aois: List[str], batch_name: str, collection: str = None) -> str:
        """Generate CSV report of AOIs that have no corresponding pipeline logs."""
        output_file = self.output_dir / "missing_aois.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["aoi_id", "batch_name", "collection", "expected_log_stream_pattern"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for aoi_id in missing_aois:
                if collection:
                    expected_pattern = f"pipeline/dispatch-[batch_name={batch_name},aoi_name={aoi_id},collection={collection}]"
                else:
                    expected_pattern = f"pipeline/dispatch-[batch_name={batch_name},aoi_name={aoi_id},collection=*]"
                writer.writerow(
                    {
                        "aoi_id": aoi_id,
                        "batch_name": batch_name,
                        "collection": collection if collection else "any",
                        "expected_log_stream_pattern": expected_pattern,
                    }
                )

        logger.info(f"Generated missing AOIs report: {output_file}")
        return str(output_file)

    def generate_metrics_reports(
        self, missing_metrics: List[Dict], empty_metrics: List[Dict], missing_agg: List[Dict]
    ) -> List[str]:
        """Generate reports for metrics file issues."""
        reports = []

        # Missing metrics report
        if missing_metrics:
            output_file = self.output_dir / "missing_metrics.csv"
            with open(output_file, "w", newline="") as csvfile:
                fieldnames = ["directory", "issue"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(missing_metrics)
            reports.append(str(output_file))
            logger.info(f"Generated missing metrics report: {output_file}")

        # Empty metrics report
        if empty_metrics:
            output_file = self.output_dir / "empty_metrics.csv"
            with open(output_file, "w", newline="") as csvfile:
                fieldnames = ["file", "line_count", "issue"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(empty_metrics)
            reports.append(str(output_file))
            logger.info(f"Generated empty metrics report: {output_file}")

        # Missing agg_metrics report
        if missing_agg:
            output_file = self.output_dir / "missing_agg_metrics.csv"
            with open(output_file, "w", newline="") as csvfile:
                fieldnames = ["directory", "expected_file", "issue"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(missing_agg)
            reports.append(str(output_file))
            logger.info(f"Generated missing agg_metrics report: {output_file}")

        return reports

    def generate_unique_errors_report(self, unique_errors: List[UniqueErrorInfo]) -> str:
        """Generate CSV report of unique error patterns with occurrence counts."""
        output_file = self.output_dir / "unique_error_patterns.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "error_pattern", 
                "error_type", 
                "occurrence_count", 
                "first_occurrence_timestamp",
                "first_occurrence_job_stream",
                "first_occurrence_pipeline_stream",
                "affected_job_streams_count",
                "sample_raw_message"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for error in unique_errors:
                writer.writerow({
                    "error_pattern": error.error_pattern,
                    "error_type": error.error_type,
                    "occurrence_count": error.occurrence_count,
                    "first_occurrence_timestamp": error.first_occurrence_timestamp,
                    "first_occurrence_job_stream": error.first_occurrence_job_stream,
                    "first_occurrence_pipeline_stream": error.first_occurrence_pipeline_stream,
                    "affected_job_streams_count": len(error.affected_job_streams),
                    "sample_raw_message": error.sample_raw_message[:500] + "..." if len(error.sample_raw_message) > 500 else error.sample_raw_message
                })

        logger.info(f"Generated unique error patterns report: {output_file}")
        return str(output_file)

    def generate_unique_exceptions_report(self, unique_exceptions: List[UniqueErrorInfo]) -> str:
        """Generate CSV report of unique unhandled exception patterns."""
        output_file = self.output_dir / "unique_unhandled_exceptions.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "exception_pattern", 
                "exception_type", 
                "occurrence_count", 
                "first_occurrence_timestamp",
                "first_occurrence_job_stream",
                "first_occurrence_pipeline_stream",
                "affected_job_streams_count",
                "sample_raw_message"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for exception in unique_exceptions:
                writer.writerow({
                    "exception_pattern": exception.error_pattern,
                    "exception_type": exception.error_type,
                    "occurrence_count": exception.occurrence_count,
                    "first_occurrence_timestamp": exception.first_occurrence_timestamp,
                    "first_occurrence_job_stream": exception.first_occurrence_job_stream,
                    "first_occurrence_pipeline_stream": exception.first_occurrence_pipeline_stream,
                    "affected_job_streams_count": len(exception.affected_job_streams),
                    "sample_raw_message": exception.sample_raw_message[:500] + "..." if len(exception.sample_raw_message) > 500 else exception.sample_raw_message
                })

        logger.info(f"Generated unique unhandled exceptions report: {output_file}")
        return str(output_file)

    def generate_summary_report(self, analysis_results: Dict[str, Any]) -> str:
        """Generate JSON summary of all analysis results."""
        output_file = self.output_dir / "debug_summary.json"

        with open(output_file, "w") as f:
            json.dump(analysis_results, f, indent=2, default=str)

        logger.info(f"Generated summary report: {output_file}")
        return str(output_file)

    def generate_failed_pipelines_report(self, failed_pipelines: List[FailedPipelineInfo]) -> str:
        """Generate CSV report of pipelines that failed for non-job reasons."""
        output_file = self.output_dir / "failed_pipelines.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "pipeline_log_stream", 
                "aoi_name", 
                "batch_name", 
                "collection", 
                "timestamp", 
                "failure_reason",
                "failure_type",
                "investigation_notes"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for pipeline in failed_pipelines:
                # Categorize failure type and provide specific investigation notes
                if "job failures" in pipeline.failure_reason:
                    failure_type = "Pipeline Cleanup/Finalization Failure"
                    investigation_notes = "Pipeline had job failures but failed to complete properly. Check pipeline logic after job failures - may be missing error handling, cleanup steps, or proper failure reporting."
                else:
                    failure_type = "Infrastructure/Silent Failure"
                    investigation_notes = "Pipeline submitted but never reached SUCCESS state with no job failures detected. Check for infrastructure issues, resource constraints, configuration problems, or early pipeline termination."
                
                writer.writerow({
                    "pipeline_log_stream": pipeline.pipeline_log_stream,
                    "aoi_name": pipeline.aoi_name,
                    "batch_name": pipeline.batch_name,
                    "collection": pipeline.collection or "",
                    "timestamp": pipeline.timestamp or "",
                    "failure_reason": pipeline.failure_reason,
                    "failure_type": failure_type,
                    "investigation_notes": investigation_notes
                })

        logger.info(f"Generated failed pipelines report: {output_file}")
        return str(output_file)

    def generate_job_status_summary_report(self, analysis_results: Dict[str, Any]) -> str:
        """Generate comprehensive CSV report of job status summary."""
        output_file = self.output_dir / "job_status_summary.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "status", 
                "count", 
                "percentage",
                "description"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            
            status_descriptions = {
                "succeeded": "Jobs that completed successfully",
                "failed": "Jobs that failed due to application errors",
                "lost": "Jobs that were lost due to infrastructure issues",
                "pending": "Jobs waiting to be allocated and run",
                "running": "Jobs currently executing",
                "dispatched": "Jobs dispatched but not yet allocated",
                "stopped": "Jobs that were intentionally stopped",
                "cancelled": "Jobs that were cancelled",
                "unknown": "Jobs with undetermined status"
            }
            
            status_counts = {
                "succeeded": analysis_results.get("succeeded_jobs_count", 0),
                "failed": analysis_results.get("failed_jobs_count", 0),
                "lost": analysis_results.get("lost_jobs_count", 0),
                "pending": analysis_results.get("pending_jobs_count", 0),
                "running": analysis_results.get("running_jobs_count", 0),
                "dispatched": analysis_results.get("dispatched_jobs_count", 0),
                "stopped": analysis_results.get("stopped_jobs_count", 0),
                "cancelled": analysis_results.get("cancelled_jobs_count", 0),
                "unknown": analysis_results.get("unknown_jobs_count", 0),
            }
            
            percentages = analysis_results.get("status_percentages", {})
            
            for status, count in status_counts.items():
                writer.writerow({
                    "status": status.upper(),
                    "count": count,
                    "percentage": f"{percentages.get(status, 0)}%",
                    "description": status_descriptions.get(status, "")
                })

        logger.info(f"Generated job status summary report: {output_file}")
        return str(output_file)

    def generate_succeeded_jobs_report(self, succeeded_jobs: List[Dict]) -> str:
        """Generate CSV report of succeeded jobs."""
        output_file = self.output_dir / "succeeded_jobs.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["timestamp", "pipeline_log_stream", "job_log_stream", "job_status"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for job in succeeded_jobs:
                writer.writerow({
                    "timestamp": job.get("timestamp", ""),
                    "pipeline_log_stream": job.get("pipeline_log_stream", ""),
                    "job_log_stream": job.get("job_log_stream", ""),
                    "job_status": job.get("job_status", "SUCCEEDED")
                })

        logger.info(f"Generated succeeded jobs report: {output_file}")
        return str(output_file)

    def generate_pending_jobs_report(self, pending_jobs: List[Dict]) -> str:
        """Generate CSV report of pending jobs (potentially stuck)."""
        output_file = self.output_dir / "pending_jobs.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["timestamp", "pipeline_log_stream", "job_log_stream", "job_status", "investigation_notes"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for job in pending_jobs:
                writer.writerow({
                    "timestamp": job.get("timestamp", ""),
                    "pipeline_log_stream": job.get("pipeline_log_stream", ""),
                    "job_log_stream": job.get("job_log_stream", ""),
                    "job_status": job.get("job_status", "PENDING"),
                    "investigation_notes": "Job may be stuck waiting for resources. Check Nomad cluster capacity and allocation queue."
                })

        logger.info(f"Generated pending jobs report: {output_file}")
        return str(output_file)

    def generate_running_jobs_report(self, running_jobs: List[Dict]) -> str:
        """Generate CSV report of running jobs (potentially long-running)."""
        output_file = self.output_dir / "running_jobs.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["timestamp", "pipeline_log_stream", "job_log_stream", "job_status", "investigation_notes"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for job in running_jobs:
                writer.writerow({
                    "timestamp": job.get("timestamp", ""),
                    "pipeline_log_stream": job.get("pipeline_log_stream", ""),
                    "job_log_stream": job.get("job_log_stream", ""),
                    "job_status": job.get("job_status", "RUNNING"),
                    "investigation_notes": "Job is still running. If this is an old job, check for potential hangs or infinite loops."
                })

        logger.info(f"Generated running jobs report: {output_file}")
        return str(output_file)

    def generate_stopped_cancelled_jobs_report(self, stopped_jobs: List[Dict], cancelled_jobs: List[Dict], terminal_status_details: Dict = None) -> str:
        """Generate CSV report of stopped and cancelled jobs with enhanced failure reason analysis."""
        output_file = self.output_dir / "stopped_cancelled_jobs.csv"

        if terminal_status_details is None:
            terminal_status_details = {}

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["timestamp", "pipeline_log_stream", "job_log_stream", "job_status", "failure_type", "failure_reason", "nomad_details", "investigation_priority"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            
            for job in stopped_jobs:
                job_stream = job.get("job_log_stream", "")
                details = terminal_status_details.get(job_stream, {})
                
                failure_type = details.get("failure_type", "intentional_stop")
                failure_reason = details.get("failure_reason", "Job was intentionally stopped or terminated")
                nomad_details = details.get("nomad_error_details", {})
                
                # Determine investigation priority based on available details
                if nomad_details or "error" in failure_reason.lower() or "fail" in failure_reason.lower():
                    priority = "HIGH - Requires investigation"
                elif "unknown" in failure_reason.lower():
                    priority = "MEDIUM - Check for context"
                else:
                    priority = "LOW - Likely intentional"
                
                writer.writerow({
                    "timestamp": job.get("timestamp", ""),
                    "pipeline_log_stream": job.get("pipeline_log_stream", ""),
                    "job_log_stream": job_stream,
                    "job_status": job.get("job_status", "STOPPED"),
                    "failure_type": failure_type,
                    "failure_reason": failure_reason,
                    "nomad_details": json.dumps(nomad_details) if nomad_details else "",
                    "investigation_priority": priority
                })
            
            for job in cancelled_jobs:
                job_stream = job.get("job_log_stream", "")
                details = terminal_status_details.get(job_stream, {})
                
                failure_type = details.get("failure_type", "intentional_cancel")
                failure_reason = details.get("failure_reason", "Job was cancelled before completion")
                nomad_details = details.get("nomad_error_details", {})
                
                # Determine investigation priority based on available details
                if nomad_details or "error" in failure_reason.lower() or "fail" in failure_reason.lower():
                    priority = "HIGH - Requires investigation"
                elif "unknown" in failure_reason.lower():
                    priority = "MEDIUM - Check for context"
                else:
                    priority = "LOW - Likely intentional"
                
                writer.writerow({
                    "timestamp": job.get("timestamp", ""),
                    "pipeline_log_stream": job.get("pipeline_log_stream", ""),
                    "job_log_stream": job_stream,
                    "job_status": job.get("job_status", "CANCELLED"),
                    "failure_type": failure_type,
                    "failure_reason": failure_reason,
                    "nomad_details": json.dumps(nomad_details) if nomad_details else "",
                    "investigation_priority": priority
                })

        logger.info(f"Generated enhanced stopped/cancelled jobs report: {output_file}")
        return str(output_file)

    def generate_infrastructure_failure_report(self, lost_jobs: List[Dict], terminal_status_details: Dict = None, nomad_failures: Dict = None) -> str:
        """Generate CSV report of infrastructure failures (LOST jobs) with detailed analysis."""
        output_file = self.output_dir / "infrastructure_failures.csv"

        if terminal_status_details is None:
            terminal_status_details = {}
        if nomad_failures is None:
            nomad_failures = {"driver_failures": [], "timeout_failures": [], "lost_markings": []}

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "timestamp", "pipeline_log_stream", "job_log_stream", "job_status", 
                "failure_type", "failure_reason", "nomad_details", "driver_failure_detected",
                "timeout_detected", "investigation_priority", "recommended_action"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            
            for job in lost_jobs:
                job_stream = job.get("job_log_stream", "")
                details = terminal_status_details.get(job_stream, {})
                
                failure_type = details.get("failure_type", "infrastructure_timeout")
                failure_reason = details.get("failure_reason", "Job lost due to infrastructure issues")
                nomad_details = details.get("nomad_error_details", {})
                
                # Check if this job appears in specific nomad failure categories
                driver_failure_detected = any(f.get("job_id", "") in job_stream for f in nomad_failures.get("driver_failures", []))
                timeout_detected = any(f.get("job_id", "") in job_stream for f in nomad_failures.get("timeout_failures", []))
                
                # Determine investigation priority and recommended action
                if driver_failure_detected:
                    priority = "CRITICAL - Driver failure"
                    action = "Investigate driver configuration, resource allocation, and container runtime issues"
                elif failure_type == "driver_failure":
                    priority = "HIGH - Driver issue in logs"
                    action = "Check driver logs and job allocation details"
                elif timeout_detected:
                    priority = "HIGH - Timeout confirmed"
                    action = "Review job runtime limits and cluster resource availability"  
                elif nomad_details:
                    priority = "MEDIUM - Nomad context available"
                    action = "Review nomad error details for specific cause"
                else:
                    priority = "LOW - Standard timeout"
                    action = "Likely infrastructure timeout - verify cluster health during job execution time"
                
                writer.writerow({
                    "timestamp": job.get("timestamp", ""),
                    "pipeline_log_stream": job.get("pipeline_log_stream", ""),
                    "job_log_stream": job_stream,
                    "job_status": job.get("job_status", "LOST"),
                    "failure_type": failure_type,
                    "failure_reason": failure_reason,
                    "nomad_details": json.dumps(nomad_details) if nomad_details else "",
                    "driver_failure_detected": "YES" if driver_failure_detected else "NO",
                    "timeout_detected": "YES" if timeout_detected else "NO",
                    "investigation_priority": priority,
                    "recommended_action": action
                })

        logger.info(f"Generated infrastructure failures report: {output_file}")
        return str(output_file)

    def generate_nomad_failures_report(self, nomad_failures: Dict) -> str:
        """Generate CSV report of nomad-specific failures extracted from pipeline logs."""
        output_file = self.output_dir / "nomad_failures.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "timestamp", "pipeline_log_stream", "job_id", "allocation_id",
                "failure_type", "message", "severity", "category"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            
            # Write driver failures
            for failure in nomad_failures.get("driver_failures", []):
                writer.writerow({
                    "timestamp": failure.get("timestamp", ""),
                    "pipeline_log_stream": failure.get("pipeline_log_stream", ""),
                    "job_id": failure.get("job_id", ""),
                    "allocation_id": failure.get("allocation_id", ""),
                    "failure_type": "Driver Failure",
                    "message": failure.get("message", "")[:500],  # Limit message length
                    "severity": "CRITICAL",
                    "category": "Infrastructure - Driver"
                })
            
            # Write dispatch failures
            for failure in nomad_failures.get("dispatch_failures", []):
                writer.writerow({
                    "timestamp": failure.get("timestamp", ""),
                    "pipeline_log_stream": failure.get("pipeline_log_stream", ""),
                    "job_id": failure.get("job_id", ""),
                    "allocation_id": "",
                    "failure_type": "Dispatch Failure",
                    "message": failure.get("message", "")[:500],
                    "severity": "HIGH",
                    "category": "Infrastructure - Dispatch"
                })
            
            # Write timeout failures (subset of lost markings)
            for failure in nomad_failures.get("timeout_failures", []):
                writer.writerow({
                    "timestamp": failure.get("timestamp", ""),
                    "pipeline_log_stream": failure.get("pipeline_log_stream", ""),
                    "job_id": failure.get("job_id", ""),
                    "allocation_id": "",
                    "failure_type": "Timeout/LOST",
                    "message": failure.get("message", "")[:500],
                    "severity": "MEDIUM",
                    "category": "Infrastructure - Timeout"
                })
            
            # Write other lost markings (non-timeout)
            for failure in nomad_failures.get("lost_markings", []):
                if not failure.get("is_timeout", False):  # Skip timeouts as they're handled above
                    writer.writerow({
                        "timestamp": failure.get("timestamp", ""),
                        "pipeline_log_stream": failure.get("pipeline_log_stream", ""),
                        "job_id": failure.get("job_id", ""),
                        "allocation_id": "",
                        "failure_type": "LOST Marking",
                        "message": failure.get("message", "")[:500],
                        "severity": "MEDIUM",
                        "category": "Infrastructure - Lost"
                    })

        logger.info(f"Generated nomad failures report: {output_file}")
        return str(output_file)

    def generate_comprehensive_status_reports(self, analysis_results: Dict[str, Any]) -> List[str]:
        """Generate all job status reports."""
        reports = []
        
        # Generate job status summary
        summary_report = self.generate_job_status_summary_report(analysis_results)
        reports.append(summary_report)
        
        jobs_by_status = analysis_results.get("jobs_by_status", {})
        
        # Generate reports for each status type if they have jobs
        if jobs_by_status.get("SUCCEEDED"):
            reports.append(self.generate_succeeded_jobs_report(jobs_by_status["SUCCEEDED"]))
        
        if jobs_by_status.get("PENDING"):
            reports.append(self.generate_pending_jobs_report(jobs_by_status["PENDING"]))
        
        if jobs_by_status.get("RUNNING"):
            reports.append(self.generate_running_jobs_report(jobs_by_status["RUNNING"]))
        
        if jobs_by_status.get("STOPPED") or jobs_by_status.get("CANCELLED"):
            reports.append(self.generate_stopped_cancelled_jobs_report(
                jobs_by_status.get("STOPPED", []), 
                jobs_by_status.get("CANCELLED", []),
                analysis_results.get("terminal_status_details", {})
            ))
        
        # Generate infrastructure failure report for LOST jobs
        if jobs_by_status.get("LOST"):
            reports.append(self.generate_infrastructure_failure_report(
                jobs_by_status.get("LOST", []),
                analysis_results.get("terminal_status_details", {}),
                analysis_results.get("nomad_failures", {})
            ))
        
        # Generate nomad failures report if there are any nomad failures
        nomad_failures = analysis_results.get("nomad_failures", {})
        if (nomad_failures.get("summary", {}).get("total_driver_failures", 0) > 0 or
            nomad_failures.get("summary", {}).get("total_dispatch_failures", 0) > 0 or
            nomad_failures.get("summary", {}).get("total_lost_markings", 0) > 0):
            reports.append(self.generate_nomad_failures_report(nomad_failures))
        
        return reports