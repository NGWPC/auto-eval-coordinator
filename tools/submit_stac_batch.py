import argparse
import logging
import os
import sys
import time
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

import nomad


def retry_with_backoff(max_retries: int = 2, backoff_base: float = 2.0):
    """
    Decorator to add retry logic with exponential backoff to functions.

    Args:
        max_retries: Maximum number of retry attempts (default: 2)
        backoff_base: Base for exponential backoff calculation (default: 2.0)
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            func_name = func.__name__

            for attempt in range(max_retries + 1):
                try:
                    result = func(*args, **kwargs)

                    if attempt > 0:
                        logging.info(f"Function {func_name} succeeded on attempt {attempt + 1}")

                    return result

                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = backoff_base**attempt
                        logging.warning(
                            f"Function {func_name} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. Retrying in {wait_time} seconds..."
                        )
                        time.sleep(wait_time)
                    else:
                        logging.error(f"Function {func_name} failed after {max_retries + 1} attempts: {e}")

            raise last_exception

        return wrapper

    return decorator


@retry_with_backoff(max_retries=2)
def submit_pipeline_job(
    nomad_client: nomad.Nomad,
    item_id: str,
    batch_name: str,
    output_root: str,
    hand_index_path: str,
    benchmark_sources: str,
    nomad_token: Optional[str] = None,
    use_local_creds: bool = False,
) -> str:
    """
    Submit a pipeline job for a single STAC item.

    Returns:
        Dispatched job ID
    """
    # Ensure output_root doesn't have redundant trailing slashes
    output_root_clean = output_root.rstrip("/")

    meta = {
        "aoi_stac_item_id": item_id,
        "outputs_path": f"{output_root_clean}/{item_id}/",
        "hand_index_path": hand_index_path,
        "benchmark_sources": benchmark_sources,
        "tags": f"batch_name={batch_name} aoi_name={item_id}",
        "nomad_token": nomad_token or os.environ.get("NOMAD_TOKEN", ""),
        "registry_token": os.environ.get("REGISTRY_TOKEN", ""),
    }

    # Include AWS credentials from environment if using local creds
    if use_local_creds:
        meta.update(
            {
                "aws_access_key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
                "aws_secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
                "aws_session_token": os.environ.get("AWS_SESSION_TOKEN", ""),
            }
        )

    # Create the id_prefix_template in the format [batch_name=value,aoi_name=value]
    # Note: We no longer have collection_id since we're not extracting geometries
    id_prefix_template = f"[batch_name={batch_name},aoi_name={item_id}]"

    result = nomad_client.job.dispatch_job(
        id_="pipeline",
        payload=None,
        meta=meta,
        id_prefix_template=id_prefix_template,
    )

    return result["DispatchedJobID"]


@retry_with_backoff(max_retries=2)
def get_running_pipeline_jobs(nomad_client: nomad.Nomad) -> int:
    """
    Get the count of running and queued pipeline jobs, including dispatched jobs waiting for allocation.

    Returns:
        Number of pipeline jobs in active states (not finished)
    """
    # Get all jobs to include dispatched jobs that haven't been allocated yet
    jobs = nomad_client.jobs.get_jobs()
    pipeline_jobs = [job for job in jobs if job.get("ID", "").startswith("pipeline")]

    running_count = 0
    for job in pipeline_jobs:
        job_status = job.get("Status", "")
        # Debug logging to see actual job statuses
        logging.debug(f"Pipeline job {job.get('ID', 'unknown')}: Status={job_status}")

        # Count jobs that are not finished (dead = finished)
        # "running" includes both allocated jobs and dispatched jobs waiting for allocation
        if job_status != "dead":
            running_count += 1

    logging.debug(f"Found {running_count} active pipeline jobs out of {len(pipeline_jobs)} total pipeline jobs")
    return running_count


def main():
    parser = argparse.ArgumentParser(description="Submit batch of pipeline jobs for multiple STAC items")

    # Required arguments
    parser.add_argument(
        "--batch_name",
        required=True,
        help="Name for this batch of jobs (passed as tag)",
    )
    parser.add_argument(
        "--output_root",
        required=True,
        help="Root directory for outputs (STAC item ID will be appended to create an individual pipelines output path)",
    )
    parser.add_argument(
        "--hand_index_path",
        required=True,
        help="Path to HAND index (passed to pipeline)",
    )
    parser.add_argument(
        "--benchmark_sources",
        required=True,
        help="Comma-separated benchmark sources (passed to pipeline)",
    )
    parser.add_argument(
        "--item_list",
        required=True,
        help="Path to text file with STAC item IDs (one per line)",
    )

    # Optional arguments
    parser.add_argument(
        "--wait_seconds",
        type=int,
        default=0,
        help="Seconds to wait between job submissions",
    )
    parser.add_argument(
        "--stop_threshold",
        type=int,
        required=True,
        help="Stop submitting jobs when this many pipelines are running/queued",
    )
    parser.add_argument(
        "--resume_threshold",
        type=int,
        required=True,
        help="Resume submitting jobs when running/queued pipelines drop to this level (must be less than stop_threshold)",
    )

    # Nomad connection arguments
    parser.add_argument(
        "--nomad_addr",
        default=os.environ.get("NOMAD_ADDR", "http://localhost:4646"),
        help="Nomad server address",
    )
    parser.add_argument(
        "--nomad_namespace",
        default=os.environ.get("NOMAD_NAMESPACE", "default"),
        help="Nomad namespace",
    )
    parser.add_argument(
        "--nomad_token",
        default=os.environ.get("NOMAD_TOKEN"),
        help="Nomad ACL token",
    )

    # AWS authentication arguments
    parser.add_argument(
        "--use-local-creds",
        action="store_true",
        help="Use AWS credentials from shell environment instead of IAM roles",
    )

    args = parser.parse_args()

    # Validate threshold relationship
    if args.resume_threshold >= args.stop_threshold:
        parser.error("--resume_threshold must be less than --stop_threshold")

    # Setup logging
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Read STAC item IDs
    with open(args.item_list, "r") as f:
        item_ids = [line.strip() for line in f if line.strip()]

    if not item_ids:
        logging.error("No STAC item IDs found in item list")
        return 1

    logging.info(f"Loaded {len(item_ids)} STAC item IDs from {args.item_list}")

    # Initialize Nomad client
    parsed = urlparse(args.nomad_addr)
    nomad_client = nomad.Nomad(
        host=parsed.hostname,
        port=parsed.port or 4646,
        verify=False,
        token=args.nomad_token,
        namespace=args.nomad_namespace,
    )

    # Process STAC items - submit all jobs immediately
    submitted_jobs = []
    failed_submissions = []

    # Track submission state for hysteresis
    submission_paused = False

    logging.info(f"Starting job submission for {len(item_ids)} STAC items")
    logging.info(f"Thresholds - Stop: {args.stop_threshold}, Resume: {args.resume_threshold}")

    for item_id in item_ids:
        # Implement hysteresis for job submission control
        while True:
            current_jobs = get_running_pipeline_jobs(nomad_client)
            # Subtract 1 to account for the parameterized job template that's always running
            actual_running = current_jobs - 1

            if not submission_paused:
                # Currently submitting - check if we should pause
                if actual_running >= args.stop_threshold:
                    submission_paused = True
                    logging.info(
                        f"Stop threshold ({args.stop_threshold}) reached. Current jobs: {actual_running}. "
                        f"Pausing submissions until jobs drop to {args.resume_threshold}..."
                    )
                else:
                    # Can continue submitting
                    break
            else:
                # Currently paused - check if we should resume
                if actual_running <= args.resume_threshold:
                    submission_paused = False
                    logging.info(
                        f"Resume threshold ({args.resume_threshold}) reached. Current jobs: {actual_running}. "
                        f"Resuming submissions..."
                    )
                    break
                else:
                    # Still need to wait
                    wait_time = max(args.wait_seconds, 10)  # Minimum 10 seconds to avoid hammering the API
                    logging.debug(
                        f"Waiting for jobs to drop to resume threshold. Current: {actual_running}, "
                        f"Resume at: {args.resume_threshold}. Waiting {wait_time} seconds..."
                    )
                    time.sleep(wait_time)

        logging.info(f"Submitting job for STAC item {item_id}")

        try:
            job_id = submit_pipeline_job(
                nomad_client=nomad_client,
                item_id=item_id,
                batch_name=args.batch_name,
                output_root=args.output_root,
                hand_index_path=args.hand_index_path,
                benchmark_sources=args.benchmark_sources,
                nomad_token=args.nomad_token,
                use_local_creds=args.use_local_creds,
            )

            submitted_jobs.append((item_id, job_id))
            logging.info(f"Successfully submitted job {job_id} for STAC item {item_id}")

            # Wait between submissions if specified
            if args.wait_seconds > 0:
                logging.info(f"Waiting {args.wait_seconds} seconds before next submission...")
                time.sleep(args.wait_seconds)

        except Exception as e:
            logging.error(f"Failed to submit job for STAC item {item_id}: {e}")
            failed_submissions.append((item_id, str(e)))

    # Summary
    logging.info("\n" + "=" * 60)
    logging.info("BATCH SUBMISSION COMPLETE")
    logging.info("=" * 60)
    logging.info(f"Total STAC items processed: {len(item_ids)}")
    logging.info(f"Successfully submitted: {len(submitted_jobs)}")
    logging.info(f"Failed submissions: {len(failed_submissions)}")

    if submitted_jobs:
        logging.info("\nSubmitted jobs:")
        for item_id, job_id in submitted_jobs:
            logging.info(f"  STAC item {item_id}: {job_id}")

    if failed_submissions:
        logging.info("\nFailed submissions:")
        for item_id, error in failed_submissions:
            logging.info(f"  STAC item {item_id}: {error}")

    # Monitor running jobs until all are complete
    if submitted_jobs:
        logging.info("\nMonitoring job completion...")
        while True:
            current_jobs = get_running_pipeline_jobs(nomad_client)
            logging.info(f"Currently running pipeline jobs: {current_jobs - 1}")  # don't count the parent job

            if current_jobs <= 1:  # Only the parameterized job template should remain
                logging.info("All submitted jobs have completed!")
                break

            # Wait before checking again
            time.sleep(60)  # Check every minute

    return 0 if not failed_submissions else 1


if __name__ == "__main__":
    sys.exit(main())
