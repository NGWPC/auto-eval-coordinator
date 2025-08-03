"""Summary analyzer for creating executive summaries and trend analysis."""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class SummaryAnalyzer:
    """Generates executive summaries and recommendations from batch analysis."""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.history_dir = os.path.join(output_dir, "history")
        os.makedirs(self.history_dir, exist_ok=True)
    
    def generate_executive_summary(self, analysis_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a 1-page executive summary of batch health.
        
        Returns a structured summary with key metrics and insights.
        """
        summary = analysis_result.get("summary", {})
        status_groups = analysis_result.get("status_groups", {})
        unique_errors = analysis_result.get("unique_errors", [])
        action_items = analysis_result.get("action_items", [])
        
        # Calculate key metrics
        total_jobs = summary.get("total_jobs", 0)
        success_count = len(status_groups.get("success", []))
        failure_count = len(status_groups.get("application_failures", []))
        infrastructure_count = len(status_groups.get("infrastructure_issues", []))
        
        # Overall health score (0-100)
        health_score = self._calculate_health_score(summary, status_groups)
        
        # Top issues
        top_issues = []
        if infrastructure_count > 0:
            top_issues.append({
                "type": "Infrastructure",
                "severity": "HIGH",
                "impact": f"{infrastructure_count} jobs lost",
                "percentage": round((infrastructure_count / total_jobs * 100), 1) if total_jobs > 0 else 0
            })
        
        # Add top 3 error patterns
        for error in unique_errors[:3]:
            if error["occurrence_count"] >= 3:
                top_issues.append({
                    "type": "Application Error",
                    "severity": "HIGH" if error["occurrence_count"] >= 10 else "MEDIUM",
                    "impact": f"{error['occurrence_count']} jobs affected",
                    "percentage": round((error["occurrence_count"] / total_jobs * 100), 1) if total_jobs > 0 else 0,
                    "pattern": error["error_pattern"][:100]
                })
        
        # Generate recommendations
        recommendations = self._generate_recommendations(summary, status_groups, unique_errors)
        
        # Expected improvements if recommendations are implemented
        potential_improvements = self._calculate_potential_improvements(summary, status_groups, unique_errors)
        
        executive_summary = {
            "batch_info": {
                "batch_name": analysis_result.get("batch_name", "Unknown"),
                "collection": analysis_result.get("collection", "All"),
                "analysis_timestamp": datetime.utcnow().isoformat(),
                "total_jobs": total_jobs,
                "total_pipelines": len(analysis_result.get("submitted_pipelines", []))
            },
            "health_metrics": {
                "overall_health_score": health_score,
                "success_rate": summary.get("success_rate", 0),
                "failure_rate": summary.get("failure_rate", 0),
                "infrastructure_failure_rate": summary.get("infrastructure_failure_rate", 0),
                "jobs_in_progress": summary.get("in_progress_jobs", 0)
            },
            "status_breakdown": {
                "succeeded": success_count,
                "failed": failure_count,
                "lost": infrastructure_count,
                "in_progress": len(status_groups.get("in_progress", [])),
                "stopped_cancelled": len(status_groups.get("intentional_stops", [])),
                "unknown": len(status_groups.get("unknown", []))
            },
            "top_issues": top_issues[:5],  # Limit to top 5
            "recommendations": recommendations[:3],  # Top 3 recommendations
            "potential_improvements": potential_improvements,
            "action_items_count": len(action_items),
            "unique_error_patterns": len(unique_errors)
        }
        
        return executive_summary
    
    def analyze_trends(self, current_analysis: Dict[str, Any], lookback_days: int = 7) -> Dict[str, Any]:
        """
        Compare current batch with historical data to identify trends.
        
        Returns trend analysis with improvements/regressions.
        """
        batch_name = current_analysis.get("batch_name", "unknown")
        current_summary = current_analysis.get("summary", {})
        
        # Load historical summaries
        historical_data = self._load_historical_data(batch_name, lookback_days)
        
        if not historical_data:
            return {
                "trend_available": False,
                "message": "No historical data available for trend analysis"
            }
        
        # Calculate trends
        trends = {
            "trend_available": True,
            "lookback_days": lookback_days,
            "data_points": len(historical_data),
            "metrics": {}
        }
        
        # Success rate trend
        historical_success_rates = [h.get("success_rate", 0) for h in historical_data]
        current_success_rate = current_summary.get("success_rate", 0)
        
        trends["metrics"]["success_rate"] = {
            "current": current_success_rate,
            "average": round(sum(historical_success_rates) / len(historical_success_rates), 2),
            "min": min(historical_success_rates),
            "max": max(historical_success_rates),
            "trend": self._calculate_trend(historical_success_rates, current_success_rate),
            "improvement": round(current_success_rate - (sum(historical_success_rates) / len(historical_success_rates)), 2)
        }
        
        # Infrastructure failure trend
        historical_infra_rates = [h.get("infrastructure_failure_rate", 0) for h in historical_data]
        current_infra_rate = current_summary.get("infrastructure_failure_rate", 0)
        
        trends["metrics"]["infrastructure_failures"] = {
            "current": current_infra_rate,
            "average": round(sum(historical_infra_rates) / len(historical_infra_rates), 2),
            "trend": self._calculate_trend(historical_infra_rates, current_infra_rate, lower_is_better=True),
            "improvement": round((sum(historical_infra_rates) / len(historical_infra_rates)) - current_infra_rate, 2)
        }
        
        # Job volume trend
        historical_job_counts = [h.get("total_jobs", 0) for h in historical_data]
        current_job_count = current_summary.get("total_jobs", 0)
        
        trends["metrics"]["job_volume"] = {
            "current": current_job_count,
            "average": round(sum(historical_job_counts) / len(historical_job_counts), 0),
            "trend": self._calculate_trend(historical_job_counts, current_job_count)
        }
        
        # Overall assessment
        trends["overall_assessment"] = self._assess_overall_trend(trends["metrics"])
        
        return trends
    
    def generate_recommendations(self, analysis_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate automated recommendations based on analysis results.
        
        Returns prioritized list of recommendations.
        """
        return self._generate_recommendations(
            analysis_result.get("summary", {}),
            analysis_result.get("status_groups", {}),
            analysis_result.get("unique_errors", [])
        )
    
    def save_summary_snapshot(self, analysis_result: Dict[str, Any]):
        """Save current analysis snapshot for historical trending."""
        batch_name = analysis_result.get("batch_name", "unknown")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        snapshot = {
            "batch_name": batch_name,
            "timestamp": datetime.utcnow().isoformat(),
            "summary": analysis_result.get("summary", {}),
            "error_count": len(analysis_result.get("unique_errors", [])),
            "action_items_count": len(analysis_result.get("action_items", []))
        }
        
        filename = os.path.join(self.history_dir, f"{batch_name}_{timestamp}.json")
        with open(filename, 'w') as f:
            json.dump(snapshot, f, indent=2)
        
        logger.info(f"Saved summary snapshot to {filename}")
    
    def _calculate_health_score(self, summary: Dict[str, Any], status_groups: Dict[str, List]) -> int:
        """Calculate overall health score (0-100) based on multiple factors."""
        score = 100
        
        # Deduct for failures
        failure_rate = summary.get("failure_rate", 0)
        score -= min(failure_rate * 0.5, 30)  # Max 30 point deduction
        
        # Deduct for infrastructure issues
        infra_rate = summary.get("infrastructure_failure_rate", 0)
        score -= min(infra_rate * 1.0, 40)  # Max 40 point deduction (more severe)
        
        # Deduct for jobs stuck in progress
        in_progress_rate = (len(status_groups.get("in_progress", [])) / summary.get("total_jobs", 1)) * 100
        if in_progress_rate > 10:
            score -= min((in_progress_rate - 10) * 0.5, 20)  # Max 20 point deduction
        
        # Bonus for high success rate
        success_rate = summary.get("success_rate", 0)
        if success_rate >= 95:
            score += 10
        elif success_rate >= 90:
            score += 5
        
        return max(0, min(100, int(score)))
    
    def _generate_recommendations(self, summary: Dict[str, Any], status_groups: Dict[str, List], 
                                unique_errors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate prioritized recommendations based on analysis."""
        recommendations = []
        
        # Infrastructure recommendations
        infra_issues = len(status_groups.get("infrastructure_issues", []))
        if infra_issues > 0:
            infra_rate = summary.get("infrastructure_failure_rate", 0)
            recommendations.append({
                "priority": "CRITICAL",
                "category": "Infrastructure",
                "title": "Address Infrastructure Failures",
                "description": f"{infra_issues} jobs ({infra_rate}%) lost due to infrastructure issues",
                "actions": [
                    "Check Nomad cluster health and node availability",
                    "Review job timeout settings",
                    "Consider increasing cluster capacity if nodes are overloaded",
                    "Implement better job retry mechanisms"
                ],
                "expected_impact": f"Could recover up to {infra_rate}% success rate"
            })
        
        # Error pattern recommendations
        if unique_errors:
            top_error = unique_errors[0]
            if top_error["occurrence_count"] >= 5:
                impact_rate = round((top_error["occurrence_count"] / summary.get("total_jobs", 1)) * 100, 1)
                recommendations.append({
                    "priority": "HIGH",
                    "category": "Application Error",
                    "title": "Fix Most Common Error Pattern",
                    "description": f"Error affecting {top_error['occurrence_count']} jobs ({impact_rate}%): {top_error['error_pattern'][:100]}",
                    "actions": [
                        "Review error pattern in affected jobs",
                        "Fix root cause in application code",
                        "Add better error handling",
                        "Consider adding validation to prevent this error"
                    ],
                    "expected_impact": f"Could improve success rate by up to {impact_rate}%"
                })
        
        # Performance recommendations
        in_progress = len(status_groups.get("in_progress", []))
        if in_progress > summary.get("total_jobs", 0) * 0.1:
            recommendations.append({
                "priority": "MEDIUM",
                "category": "Performance",
                "title": "Investigate Stuck Jobs",
                "description": f"{in_progress} jobs still in progress",
                "actions": [
                    "Check if jobs are actually running or stuck",
                    "Review job resource requirements",
                    "Consider adjusting job parallelism",
                    "Implement better job monitoring"
                ],
                "expected_impact": "Faster job completion and better resource utilization"
            })
        
        # Sort by priority
        priority_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
        recommendations.sort(key=lambda x: priority_order.get(x["priority"], 999))
        
        return recommendations
    
    def _calculate_potential_improvements(self, summary: Dict[str, Any], status_groups: Dict[str, List],
                                        unique_errors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate potential improvements if recommendations are followed."""
        current_success_rate = summary.get("success_rate", 0)
        
        # Potential recovery from infrastructure fixes
        infra_recovery = summary.get("infrastructure_failure_rate", 0)
        
        # Potential recovery from fixing top errors
        error_recovery = 0
        for error in unique_errors[:3]:  # Top 3 errors
            if error["occurrence_count"] >= 3:
                error_recovery += (error["occurrence_count"] / summary.get("total_jobs", 1)) * 100
        
        error_recovery = min(error_recovery, summary.get("failure_rate", 0))  # Cap at current failure rate
        
        potential_success_rate = min(100, current_success_rate + infra_recovery + error_recovery)
        
        return {
            "current_success_rate": current_success_rate,
            "potential_success_rate": round(potential_success_rate, 1),
            "improvement_percentage": round(potential_success_rate - current_success_rate, 1),
            "recoverable_jobs": int((infra_recovery + error_recovery) * summary.get("total_jobs", 0) / 100)
        }
    
    def _calculate_trend(self, historical_values: List[float], current_value: float, 
                        lower_is_better: bool = False) -> str:
        """Calculate trend direction based on historical data."""
        if not historical_values:
            return "unknown"
        
        avg = sum(historical_values) / len(historical_values)
        
        # Calculate percentage change
        if avg == 0:
            change = 100 if current_value > 0 else 0
        else:
            change = ((current_value - avg) / avg) * 100
        
        # Determine trend
        if abs(change) < 5:
            return "stable"
        elif change > 0:
            return "degrading" if lower_is_better else "improving"
        else:
            return "improving" if lower_is_better else "degrading"
    
    def _assess_overall_trend(self, metrics: Dict[str, Any]) -> str:
        """Assess overall trend based on multiple metrics."""
        success_trend = metrics.get("success_rate", {}).get("trend", "unknown")
        infra_trend = metrics.get("infrastructure_failures", {}).get("trend", "unknown")
        
        if success_trend == "improving" and infra_trend in ["improving", "stable"]:
            return "Batch processing health is improving"
        elif success_trend == "degrading" or infra_trend == "degrading":
            return "Batch processing health is degrading - action needed"
        else:
            return "Batch processing health is stable"
    
    def _load_historical_data(self, batch_name: str, lookback_days: int) -> List[Dict[str, Any]]:
        """Load historical summary data for trend analysis."""
        historical_data = []
        cutoff_date = datetime.utcnow() - timedelta(days=lookback_days)
        
        # Look for historical snapshots
        for filename in os.listdir(self.history_dir):
            if filename.startswith(f"{batch_name}_") and filename.endswith(".json"):
                filepath = os.path.join(self.history_dir, filename)
                try:
                    with open(filepath, 'r') as f:
                        data = json.load(f)
                    
                    # Check if within lookback period
                    timestamp = datetime.fromisoformat(data.get("timestamp", ""))
                    if timestamp >= cutoff_date:
                        historical_data.append(data.get("summary", {}))
                except Exception as e:
                    logger.warning(f"Failed to load historical data from {filename}: {e}")
        
        # Sort by timestamp
        historical_data.sort(key=lambda x: x.get("timestamp", ""))
        
        return historical_data