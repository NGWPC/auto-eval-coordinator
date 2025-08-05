"""Main orchestrator for batch run analysis."""

import logging
import time
from typing import Any, Dict

from .cloudwatch_analyzer import CloudWatchAnalyzer
from .html_generator import HTMLGenerator
from .models import DebugConfig, FailedJobInfo
from .report_generator import ReportGenerator
from .s3_analyzer import S3MetricsAnalyzer
from .summary_analyzer import SummaryAnalyzer

logger = logging.getLogger(__name__)


class BatchRunAnalyzer:
    """Main orchestrator for batch run analysis."""

    def __init__(self, config: DebugConfig):
        self.config = config
        self.cloudwatch = CloudWatchAnalyzer(config)
        self.s3_analyzer = S3MetricsAnalyzer(config) if config.s3_output_root else None
        self.report_generator = ReportGenerator(config.output_dir)
        self.html_generator = HTMLGenerator(config.output_dir) if config.generate_html else None
        self.summary_analyzer = SummaryAnalyzer(config.output_dir)

    def run_analysis(self) -> Dict[str, Any]:
        """Run complete batch run analysis using consolidated approach."""
        logger.info(f"Starting batch run analysis for batch: {self.config.batch_name}")

        # Extract basic info
        results = {
            "batch_name": self.config.batch_name,
            "collection": self.config.collection,
            "time_range_days": self.config.time_range_days,
            "analysis_timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "reports_generated": [],
        }
        
        # Initialize default values for CloudWatch-related fields
        failed_jobs = []
        unique_errors = []
        failed_pipelines = []
        missing_aois = []
        comprehensive_results = {}
        
        if self.config.scrape_bucket:
            logger.info("=== S3-Only Analysis Mode ===")
            logger.info("Skipping CloudWatch analysis as --scrape_bucket is enabled")
            
            # Initialize CloudWatch-related fields with empty/default values
            results.update({
                "failed_jobs_count": 0,
                "failed_pipelines_count": 0,
                "unhandled_exceptions_count": 0,
                "submitted_pipelines_count": 0,
                "missing_aois_count": 0,
                "missing_aois": [],
                "status_groups": {},
                "detailed_errors": {},
                "unique_errors": [],
                "failed_pipelines": [],
            })
        else:
            # Use new comprehensive analysis method
            logger.info("=== Comprehensive Batch Analysis ===")
            comprehensive_results = self.cloudwatch.analyze_batch_comprehensive()
            
            # Copy comprehensive analysis results
            results.update(comprehensive_results)
            
            # Generate executive summary
            logger.info("=== Generating Executive Summary ===")
            executive_summary = self.summary_analyzer.generate_executive_summary(comprehensive_results)
            results["executive_summary"] = executive_summary
            
            # Generate trend analysis
            logger.info("=== Analyzing Trends ===")
            trend_analysis = self.summary_analyzer.analyze_trends(comprehensive_results)
            results["trend_analysis"] = trend_analysis
            
            # Save snapshot for future trend analysis
            self.summary_analyzer.save_summary_snapshot(comprehensive_results)

            # AOI list comparison (if provided)
            if self.config.aoi_list_path:
                logger.info("=== AOI List Comparison ===")
                try:
                    # Read expected AOIs from file
                    with open(self.config.aoi_list_path, 'r') as f:
                        expected_aois = [line.strip() for line in f if line.strip()]
                    
                    logger.info(f"Loaded {len(expected_aois)} expected AOIs from {self.config.aoi_list_path}")
                    
                    # Find missing AOIs
                    missing_aois = self.cloudwatch.find_missing_pipelines(expected_aois)
                    results["missing_aois_count"] = len(missing_aois)
                    results["missing_aois"] = missing_aois
                    
                except Exception as e:
                    logger.error(f"Failed to process AOI list: {e}")
                    results["missing_aois_count"] = 0
                    results["missing_aois"] = []

            # Generate reports based on new structure
            logger.info("=== Generating Reports ===")
            
            # Create legacy compatible structures for reporting
            application_failures = comprehensive_results.get("status_groups", {}).get("application_failures", [])
            infrastructure_issues = comprehensive_results.get("status_groups", {}).get("infrastructure_issues", [])
            
            for job in application_failures + infrastructure_issues:
                job_stream = job["job_log_stream"]
                error_messages = comprehensive_results.get("detailed_errors", {}).get(job_stream, [])
                
                failed_jobs.append(
                    FailedJobInfo(
                        pipeline_log_stream=job["pipeline_log_stream"],
                        job_log_stream=job_stream,
                        error_messages=error_messages or ["No error messages found"],
                        timestamp=job.get("timestamp"),
                        job_status=job["job_status"]
                    )
                )

            # Get unique errors and failed pipelines from results
            unique_errors = comprehensive_results.get("unique_errors", [])
            failed_pipelines = comprehensive_results.get("failed_pipelines", [])
            
            # Update counts in results
            results["failed_jobs_count"] = len(failed_jobs)
            results["failed_pipelines_count"] = len(failed_pipelines)
            results["unique_error_patterns_count"] = len(unique_errors)
            results["submitted_pipelines_count"] = comprehensive_results.get("summary", {}).get("total_jobs", 0)

            # Generate reports
            if failed_jobs:
                report_file = self.report_generator.generate_failed_jobs_report(failed_jobs)
                results["reports_generated"].append(report_file)

            if unique_errors:
                report_file = self.report_generator.generate_unique_errors_report(unique_errors)
                results["reports_generated"].append(report_file)

            # Generate missing AOIs report
            if missing_aois:
                report_file = self.report_generator.generate_missing_aois_report(missing_aois, self.config.batch_name, self.config.collection)
                results["reports_generated"].append(report_file)

            # Generate failed pipelines report
            if failed_pipelines:
                report_file = self.report_generator.generate_failed_pipelines_report(failed_pipelines)
                results["reports_generated"].append(report_file)

        # S3 metrics analysis (only when scrape_bucket is True)
        missing_metrics = []
        empty_metrics = []
        missing_agg = []
        
        if self.config.scrape_bucket and self.s3_analyzer:
            logger.info("=== S3 Metrics Analysis ===")
            missing_metrics = self.s3_analyzer.find_missing_metrics()
            empty_metrics = self.s3_analyzer.find_empty_metrics()
            missing_agg = self.s3_analyzer.find_missing_agg_metrics()

            results["missing_metrics_count"] = len(missing_metrics)
            results["empty_metrics_count"] = len(empty_metrics)
            results["missing_agg_metrics_count"] = len(missing_agg)

            # Generate metrics reports
            metrics_reports = self.report_generator.generate_metrics_reports(
                missing_metrics, empty_metrics, missing_agg
            )
            results["reports_generated"].extend(metrics_reports)
        elif not self.config.scrape_bucket:
            # Initialize S3-related fields with zero values when not scraping bucket
            results["missing_metrics_count"] = 0
            results["empty_metrics_count"] = 0
            results["missing_agg_metrics_count"] = 0

        # Generate comprehensive job status reports (only if not scrape_bucket mode)
        if not self.config.scrape_bucket:
            logger.info("=== Generating Job Status Reports ===")
            status_reports = self.report_generator.generate_comprehensive_status_reports(results)
            results["reports_generated"].extend(status_reports)

        # Generate summary
        summary_file = self.report_generator.generate_summary_report(results)
        results["reports_generated"].append(summary_file)

        # Generate HTML dashboard if requested
        if self.html_generator:
            if self.config.scrape_bucket:
                logger.info("=== Generating S3 Metrics HTML Dashboard ===")
                html_file = self.html_generator.generate_s3_dashboard(
                    results,
                    missing_metrics,
                    empty_metrics,
                    missing_agg,
                )
            else:
                logger.info("=== Generating CloudWatch Analysis HTML Dashboard ===")
                html_file = self.html_generator.generate_cloudwatch_dashboard(
                    results,
                    failed_jobs,
                    [],  # unhandled_exceptions (empty for now)
                    missing_aois if missing_aois else None,
                    unique_errors,
                    [],  # unique_unhandled_exceptions (empty for now)
                    failed_pipelines,
                )
            results["reports_generated"].append(html_file)

        logger.info("=== Analysis Complete ===")
        logger.info(f"Reports generated in: {self.config.output_dir}")
        for report in results["reports_generated"]:
            logger.info(f"  - {report}")

        return results