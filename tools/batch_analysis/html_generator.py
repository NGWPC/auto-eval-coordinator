"""HTML dashboard generation for batch analysis results."""

import logging
from pathlib import Path
from typing import Any, Dict, List

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .models import FailedJobInfo, FailedPipelineInfo, UniqueErrorInfo

logger = logging.getLogger(__name__)


class HTMLGenerator:
    """Generates HTML dashboard reports from analysis results."""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up Jinja2 environment
        template_dir = Path(__file__).parent / "templates"
        self.env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=select_autoescape(['html', 'xml'])
        )


    def generate_cloudwatch_dashboard(
        self,
        analysis_results: Dict[str, Any],
        failed_jobs: List[FailedJobInfo],
        unhandled_exceptions: List[FailedJobInfo],
        missing_aois: List[str] = None,
        unique_errors: List[UniqueErrorInfo] = None,
        unique_unhandled_exceptions: List[UniqueErrorInfo] = None,
        failed_pipelines: List[FailedPipelineInfo] = None,
    ) -> str:
        """Generate CloudWatch-focused HTML dashboard from analysis results."""
        
        template = self.env.get_template('cloudwatch_dashboard.html')
        
        # Prepare template context for CloudWatch data only
        context = {
            'batch_name': analysis_results.get('batch_name', 'Unknown'),
            'collection': analysis_results.get('collection', None),
            'analysis_timestamp': analysis_results.get('analysis_timestamp', 'Unknown'),
            'time_range_days': analysis_results.get('time_range_days', 'Unknown'),
            'submitted_pipelines_count': analysis_results.get('submitted_pipelines_count', 0),
            'failed_jobs_count': analysis_results.get('failed_jobs_count', 0),
            'lost_jobs_count': analysis_results.get('lost_jobs_count', 0),
            'total_failed_jobs_count': analysis_results.get('total_failed_jobs_count', 0),
            'unhandled_exceptions_count': analysis_results.get('unhandled_exceptions_count', 0),
            'unique_error_patterns_count': analysis_results.get('unique_error_patterns_count', 0),
            'failed_jobs': failed_jobs,
            'unhandled_exceptions': unhandled_exceptions,
            'unique_errors': unique_errors or [],
            'unique_unhandled_exceptions': unique_unhandled_exceptions or [],
            'reports_generated': analysis_results.get('reports_generated', []),
            
            # Comprehensive job status data
            'total_jobs_count': analysis_results.get('total_jobs_count', 0),
            'succeeded_jobs_count': analysis_results.get('succeeded_jobs_count', 0),
            'pending_jobs_count': analysis_results.get('pending_jobs_count', 0),
            'running_jobs_count': analysis_results.get('running_jobs_count', 0),
            'dispatched_jobs_count': analysis_results.get('dispatched_jobs_count', 0),
            'stopped_jobs_count': analysis_results.get('stopped_jobs_count', 0),
            'cancelled_jobs_count': analysis_results.get('cancelled_jobs_count', 0),
            'unknown_jobs_count': analysis_results.get('unknown_jobs_count', 0),
            'status_percentages': analysis_results.get('status_percentages', {}),
            'jobs_by_status': analysis_results.get('jobs_by_status', {}),
            
            # Explicitly exclude S3 metrics data
            'missing_metrics_count': 0,
            'empty_metrics_count': 0,
            'missing_agg_metrics_count': 0,
            'missing_metrics': [],
            'empty_metrics': [],
            'missing_agg_metrics': [],
        }
        
        # Add missing AOIs data if available
        if missing_aois is not None:
            context.update({
                'missing_aois_count': analysis_results.get('missing_aois_count', 0),
                'missing_aois': missing_aois or [],
            })
        
        # Add failed pipelines data if available
        if failed_pipelines is not None:
            context.update({
                'failed_pipelines_count': analysis_results.get('failed_pipelines_count', 0),
                'failed_pipelines': failed_pipelines or [],
            })
        
        # Render template
        html_content = template.render(**context)
        
        # Write to file with CloudWatch-specific name
        output_file = self.output_dir / "cloudwatch_analysis_dashboard.html"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Generated CloudWatch HTML dashboard: {output_file}")
        return str(output_file)

    def generate_s3_dashboard(
        self,
        analysis_results: Dict[str, Any],
        missing_metrics: List[Dict],
        empty_metrics: List[Dict],
        missing_agg_metrics: List[Dict],
    ) -> str:
        """Generate S3 metrics-focused HTML dashboard from analysis results."""
        
        template = self.env.get_template('s3_dashboard.html')
        
        # Prepare template context for S3 metrics data only
        context = {
            'batch_name': analysis_results.get('batch_name', 'Unknown'),
            'collection': analysis_results.get('collection', None),
            'analysis_timestamp': analysis_results.get('analysis_timestamp', 'Unknown'),
            'time_range_days': analysis_results.get('time_range_days', 'Unknown'),
            'reports_generated': analysis_results.get('reports_generated', []),
            
            # S3 metrics data
            'missing_metrics_count': analysis_results.get('missing_metrics_count', 0),
            'empty_metrics_count': analysis_results.get('empty_metrics_count', 0),
            'missing_agg_metrics_count': analysis_results.get('missing_agg_metrics_count', 0),
            'missing_metrics': missing_metrics or [],
            'empty_metrics': empty_metrics or [],
            'missing_agg_metrics': missing_agg_metrics or [],
            
            # Explicitly exclude CloudWatch data
            'submitted_pipelines_count': 0,
            'failed_jobs_count': 0,
            'lost_jobs_count': 0,
            'total_failed_jobs_count': 0,
            'unhandled_exceptions_count': 0,
            'unique_error_patterns_count': 0,
            'failed_jobs': [],
            'unhandled_exceptions': [],
            'unique_errors': [],
            'unique_unhandled_exceptions': [],
            'total_jobs_count': 0,
            'succeeded_jobs_count': 0,
            'pending_jobs_count': 0,
            'running_jobs_count': 0,
            'dispatched_jobs_count': 0,
            'stopped_jobs_count': 0,
            'cancelled_jobs_count': 0,
            'unknown_jobs_count': 0,
            'status_percentages': {},
            'jobs_by_status': {},
            'missing_aois_count': 0,
            'missing_aois': [],
            'failed_pipelines_count': 0,
            'failed_pipelines': [],
        }
        
        # Render template
        html_content = template.render(**context)
        
        # Write to file with S3-specific name
        output_file = self.output_dir / "s3_metrics_dashboard.html"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Generated S3 metrics HTML dashboard: {output_file}")
        return str(output_file)