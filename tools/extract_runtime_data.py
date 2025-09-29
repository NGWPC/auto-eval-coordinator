#!/usr/bin/env python3

import json
import random
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import boto3


def create_database(db_path: str):
    """Create SQLite database with schema for job runtime data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_runtimes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name TEXT NOT NULL,
            resolution TEXT NOT NULL,
            batch_name TEXT NOT NULL,
            job_type TEXT NOT NULL,
            job_id TEXT NOT NULL,
            log_stream TEXT NOT NULL,
            start_time INTEGER NOT NULL,
            end_time INTEGER NOT NULL,
            runtime_seconds REAL NOT NULL,
            is_early_exit BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create indexes for efficient querying
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_resolution ON job_runtimes(pipeline_name, resolution)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_batch_name ON job_runtimes(batch_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_type ON job_runtimes(job_type)")

    conn.commit()
    conn.close()
    print(f"Database created: {db_path}")


def get_processed_pipelines(db_path: str) -> Dict[str, Set[str]]:
    """Get already processed pipelines from database, organized by resolution.

    Returns:
        Dict mapping resolution to set of pipeline names that have been processed.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all unique pipeline names grouped by resolution
    cursor.execute("""
        SELECT DISTINCT resolution, pipeline_name
        FROM job_runtimes
        WHERE job_type = 'pipeline'
    """)

    processed = {}
    for resolution, pipeline_name in cursor.fetchall():
        if resolution not in processed:
            processed[resolution] = set()
        processed[resolution].add(pipeline_name)

    conn.close()
    return processed


def get_log_streams_for_job_type(logs_client, batch_name: str, pipeline_name: str, job_type: str) -> List[str]:
    """Get log streams for a specific job type, batch, and pipeline."""

    # Map job types to log stream prefixes
    job_type_map = {
        "pipeline": "pipeline",
        "agreement": "agreement_maker",
        "mosaic": "fim_mosaicker",
        "inundate": "hand_inundator",
    }

    if job_type not in job_type_map:
        raise ValueError(f"Unknown job type: {job_type}")

    log_group = "/aws/ec2/nomad-client-linux-test"
    stream_prefix = f"{job_type_map[job_type]}/dispatch-[batch_name={batch_name},aoi_name={pipeline_name}"

    try:
        paginator = logs_client.get_paginator("describe_log_streams")
        page_iterator = paginator.paginate(
            logGroupName=log_group, logStreamNamePrefix=stream_prefix, orderBy="LogStreamName"
        )

        streams = []
        page_count = 0
        for page in page_iterator:
            page_count += 1
            if page_count % 5 == 0:
                print(f"      Processed {page_count} pages, found {len(streams)} streams so far...")
            for stream in page["logStreams"]:
                streams.append(stream["logStreamName"])

        return streams
    except Exception as e:
        print(f"Error getting log streams for {job_type}/{batch_name}/{pipeline_name}: {e}")
        return []


def get_first_timestamp(logs_client, log_group: str, log_stream: str) -> Optional[int]:
    """Get first event timestamp from a log stream."""
    try:
        first_response = logs_client.get_log_events(
            logGroupName=log_group, logStreamName=log_stream, startFromHead=True, limit=1
        )
        first_events = first_response.get("events", [])
        return first_events[0]["timestamp"] if first_events else None
    except Exception as e:
        print(f"Error getting first timestamp for {log_stream}: {e}")
        return None


def get_batch_timing_results(
    logs_client, log_group: str, streams: List[str], job_type: str
) -> List[Tuple[str, Optional[Tuple[int, int]], bool]]:
    """Submit timing queries for multiple streams at once, then poll results.
    Returns list of (stream, timing_tuple, is_early_exit) tuples."""

    if not streams:
        return []

    # Define success message patterns based on job type
    if job_type == "pipeline":
        success_pattern = "Pipeline SUCCESS|Pipeline exiting early"
    else:
        # For agreement, mosaic, inundate jobs
        success_pattern = '"level": "SUCCESS"'

    print(f"    Submitting {len(streams)} queries...")

    # Parallelize getting first timestamps for all streams
    stream_first_timestamps = {}
    max_workers = min(50, len(streams))  # Use up to 50 threads for first timestamps

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all first timestamp queries
        future_to_stream = {
            executor.submit(get_first_timestamp, logs_client, log_group, stream): stream for stream in streams
        }

        # Collect results
        for future in as_completed(future_to_stream):
            stream = future_to_stream[future]
            try:
                first_ts = future.result()
                if first_ts:
                    stream_first_timestamps[stream] = first_ts
            except Exception as e:
                print(f"    Error getting first timestamp for {stream}: {e}")

    # Parallelize submitting all CloudWatch Insights queries
    query_jobs = []
    current_time = int(time.time() * 1000)

    def submit_query(stream, first_timestamp):
        """Helper function to submit a single query."""
        query = f'''
            fields @timestamp, @message
            | filter @logStream = "{stream}"
            | filter @message like /{success_pattern}/
            | sort @timestamp desc
            | limit 1
        '''

        try:
            response = logs_client.start_query(
                logGroupName=log_group, startTime=first_timestamp, endTime=current_time, queryString=query
            )
            return {
                "query_id": response["queryId"],
                "stream": stream,
                "first_timestamp": first_timestamp,
                "status": "Running",
            }
        except Exception as e:
            print(f"    Error submitting query for {stream}: {e}")
            return None

    # Submit queries in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for stream, first_timestamp in stream_first_timestamps.items():
            futures.append(executor.submit(submit_query, stream, first_timestamp))

        # Collect results
        for future in as_completed(futures):
            try:
                job = future.result()
                if job:
                    query_jobs.append(job)
            except Exception as e:
                print(f"    Error processing query submission: {e}")

    print(f"    Submitted {len(query_jobs)} queries, waiting for completion...")

    # Poll all queries until complete
    max_wait = 60  # Total max wait time
    elapsed = 0
    sleep_interval = 1

    while elapsed < max_wait and any(job["status"] in ["Running", "Scheduled"] for job in query_jobs):
        time.sleep(sleep_interval)
        elapsed += sleep_interval

        # Check status every 2 seconds
        if elapsed % 2 == 0:
            running_count = sum(1 for job in query_jobs if job["status"] in ["Running", "Scheduled"])
            print(f"    {running_count} queries still running...")

        for job in query_jobs:
            if job["status"] in ["Running", "Scheduled"]:
                try:
                    result = logs_client.get_query_results(queryId=job["query_id"])
                    job["status"] = result["status"]
                    if result["status"] == "Complete":
                        job["results"] = result.get("results", [])
                except Exception as e:
                    print(f"    Error checking query {job['query_id']}: {e}")
                    job["status"] = "Failed"

    # Process results
    timing_results = []
    for job in query_jobs:
        stream = job["stream"]

        if job["status"] == "Complete" and "results" in job and job["results"]:
            is_early_exit = False
            # Check if this is an early exit for pipeline jobs
            if job_type == "pipeline":
                for field in job["results"][0]:
                    if field["field"] == "@message" and "Pipeline exiting early" in field["value"]:
                        is_early_exit = True
                        break

            # Extract timestamp from first result
            for field in job["results"][0]:
                if field["field"] == "@timestamp":
                    try:
                        from datetime import datetime, timezone

                        timestamp_str = field["value"]
                        dt = datetime.fromisoformat(timestamp_str).replace(tzinfo=timezone.utc)
                        last_timestamp = int(dt.timestamp() * 1000)
                        timing_results.append((stream, (job["first_timestamp"], last_timestamp), is_early_exit))
                        break
                    except Exception as e:
                        print(f"    Error parsing timestamp for {stream}: {e}")
                        timing_results.append((stream, None, False))
                        break
            else:
                timing_results.append((stream, None, False))
        else:
            timing_results.append((stream, None, False))

    successful_count = sum(1 for _, timing, _ in timing_results if timing is not None)
    print(f"    Completed: {successful_count}/{len(streams)} streams had successful timing data")

    return timing_results


def process_jobs_with_target(
    logs_client,
    log_group: str,
    streams: List[str],
    job_type: str,
    target_count: int,
    cursor,
    pipeline_name: str,
    resolution: str,
    batch_name: str,
) -> int:
    """Process job streams with a target number of successful measurements.

    Uses random sampling to get a representative sample of job timings.
    Returns the number of successful jobs processed.
    """
    if not streams:
        return 0

    target_successful = min(target_count, len(streams))
    successful_jobs = 0
    processed_streams = set()
    remaining_streams = streams.copy()

    while successful_jobs < target_successful and remaining_streams:
        # Sample up to 30 streams that we haven't processed yet
        batch_size = min(30, len(remaining_streams))
        current_batch = random.sample(remaining_streams, batch_size)

        print(f"    Processing batch of {batch_size} {job_type} streams (target: {target_successful} successful)")

        # Get timing results for current batch
        timing_results = get_batch_timing_results(logs_client, log_group, current_batch, job_type)

        # Process results
        batch_successful = 0
        for stream, timing, _ in timing_results:  # _ is is_early_exit (not used for non-pipeline jobs)
            processed_streams.add(stream)
            remaining_streams.remove(stream)

            if timing and successful_jobs < target_successful:
                start_time, end_time = timing
                runtime_seconds = (end_time - start_time) / 1000.0  # Convert ms to seconds

                # Extract job ID from stream name
                job_id_match = re.search(r"-(\d+)-[a-f0-9]+$", stream)
                job_id = job_id_match.group(1) if job_id_match else "unknown"

                # Insert into database
                cursor.execute(
                    """
                    INSERT INTO job_runtimes
                    (pipeline_name, resolution, batch_name, job_type, job_id, log_stream,
                     start_time, end_time, runtime_seconds, is_early_exit)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        pipeline_name,
                        resolution,
                        batch_name,
                        job_type,
                        job_id,
                        stream,
                        start_time,
                        end_time,
                        runtime_seconds,
                        0,  # Not early exit for non-pipeline jobs
                    ),
                )
                successful_jobs += 1
                batch_successful += 1

        print(f"    Batch yielded {batch_successful} successful jobs (total: {successful_jobs}/{target_successful})")

        # If we've hit our target or run out of streams, stop
        if successful_jobs >= target_successful:
            print(f"    Reached target of {target_successful} successful {job_type} jobs")
            break

        if not remaining_streams:
            print(f"    No more streams to process. Got {successful_jobs} successful out of {len(streams)} total")
            break

    # Calculate and print average runtime if we processed any successful jobs
    if successful_jobs > 0:
        cursor.execute(
            """
            SELECT AVG(runtime_seconds)
            FROM job_runtimes
            WHERE pipeline_name = ? AND job_type = ? AND batch_name = ?
            """,
            (pipeline_name, job_type, batch_name),
        )
        avg_runtime = cursor.fetchone()[0]
        if avg_runtime:
            print(f"    Average runtime for {job_type} jobs: {avg_runtime:.2f} seconds")

    return successful_jobs


def process_pipeline_for_batch(logs_client, pipeline_name: str, batch_name: str, resolution: str, db_path: str) -> bool:
    """Process a single pipeline for a specific batch and store runtime data.

    Returns:
        bool: True if pipeline was successfully processed (has timing data), False otherwise.
    """

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    log_group = "/aws/ec2/nomad-client-linux-test"

    # Track whether we successfully processed this pipeline (found timing data)
    pipeline_successful = False

    # First check if this is an early exit pipeline
    is_early_exit_pipeline = False

    # Get pipeline job streams first to check for early exit
    pipeline_streams = get_log_streams_for_job_type(logs_client, batch_name, pipeline_name, "pipeline")

    if pipeline_streams:
        print(f"  Checking if pipeline is early exit...")
        timing_results = get_batch_timing_results(logs_client, log_group, pipeline_streams, "pipeline")

        for stream, timing, is_early_exit in timing_results:
            if timing:
                pipeline_successful = True  # Found timing data, pipeline is successful
                if is_early_exit:
                    is_early_exit_pipeline = True
                    print(f"  Pipeline is EARLY EXIT - only processing pipeline job")

                # Store the pipeline job timing
                start_time, end_time = timing
                runtime_seconds = (end_time - start_time) / 1000.0

                job_id_match = re.search(r"-(\d+)-[a-f0-9]+$", stream)
                job_id = job_id_match.group(1) if job_id_match else "unknown"

                cursor.execute(
                    """
                    INSERT INTO job_runtimes
                    (pipeline_name, resolution, batch_name, job_type, job_id, log_stream,
                     start_time, end_time, runtime_seconds, is_early_exit)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        pipeline_name,
                        resolution,
                        batch_name,
                        "pipeline",
                        job_id,
                        stream,
                        start_time,
                        end_time,
                        runtime_seconds,
                        1 if is_early_exit else 0,
                    ),
                )

                print(f"    runtime for pipeline job: {runtime_seconds:.2f} seconds")
                break  # Only one pipeline job per pipeline

        if pipeline_successful:
            print(f"    Found 1 successful pipeline jobs")
        else:
            print(f"    No successful pipeline jobs found")
        conn.commit()

        # If early exit, skip all other job types
        if is_early_exit_pipeline:
            conn.close()
            return pipeline_successful

        # If pipeline wasn't successful (no timing data), don't process other job types
        if not pipeline_successful:
            print(f"  Pipeline {pipeline_name} has no timing data - skipping other job types")
            conn.close()
            return False

    else:
        # No pipeline streams found at all
        print(f"  No pipeline streams found for {pipeline_name} in batch {batch_name}")
        conn.close()
        return False

    # Process other job types only if pipeline was successful and not early exit
    # Define target counts for each job type
    job_configs = {"agreement": 6, "mosaic": 12, "inundate": 25}

    for job_type, target_count in job_configs.items():
        print(f"  Processing {job_type} jobs for {pipeline_name} in batch {batch_name}")

        # Get log streams for this job type
        streams = get_log_streams_for_job_type(logs_client, batch_name, pipeline_name, job_type)

        if not streams:
            print(f"    No {job_type} streams found")
            continue

        print(f"    Found {len(streams)} {job_type} streams, targeting {target_count} successful measurements")

        # Process jobs with the specified target
        successful_jobs = process_jobs_with_target(
            logs_client,
            log_group,
            streams,
            job_type,
            target_count,
            cursor,
            pipeline_name,
            resolution,
            batch_name,
        )

        print(f"    Found {successful_jobs} successful {job_type} jobs")
        conn.commit()

    conn.close()
    return pipeline_successful


def main():
    """Main function to process all pipelines and extract runtime data."""

    # Load the batch mappings
    with open("ripple_fim_batch_mappings.json", "r") as f:
        batch_mappings = json.load(f)

    # Create database
    db_path = "pipeline_runtimes.db"
    create_database(db_path)

    # Get already processed pipelines for resume capability
    already_processed = get_processed_pipelines(db_path)

    # Initialize AWS client
    logs_client = boto3.client("logs", region_name="us-east-1")

    # Calculate total pipelines and how many are already done
    total_pipelines = 0
    already_done_count = 0
    for resolution, runs in batch_mappings.items():
        for run_file, run_data in runs.items():
            total_pipelines += len(run_data["successful_pipelines"])
            if resolution in already_processed:
                already_done_count += len(
                    [p for p in run_data["successful_pipelines"] if p in already_processed[resolution]]
                )

    print(f"Total pipelines to process: {total_pipelines}")
    if already_done_count > 0:
        print(
            f"Resuming: {already_done_count} pipelines already processed, {total_pipelines - already_done_count} remaining"
        )

    processed_pipelines = already_done_count  # Start from already processed count
    pipeline_counter = 0

    # Process each resolution
    for resolution, runs in batch_mappings.items():
        print(f"\nProcessing {resolution}...")

        for run_file, run_data in runs.items():
            batch_names = run_data["batch_names"]
            successful_pipelines = run_data["successful_pipelines"]

            print(f"  Run file: {run_file}")
            print(f"  Batch names: {batch_names}")
            print(f"  Successful pipelines: {len(successful_pipelines)}")

            # Filter out already processed pipelines
            pipelines_to_skip = []
            if resolution in already_processed:
                pipelines_to_skip = [p for p in successful_pipelines if p in already_processed[resolution]]

            # Start with pipelines that need to be processed (excluding already done)
            remaining_pipelines = [p for p in successful_pipelines if p not in pipelines_to_skip]

            if pipelines_to_skip:
                print(f"  Skipping {len(pipelines_to_skip)} already processed pipelines")

            print(f"  Starting with {len(remaining_pipelines)} pipelines to process")

            # Process each batch until all pipelines are successful or batches are exhausted
            for batch_idx, batch_name in enumerate(batch_names):
                if not remaining_pipelines:
                    print(f"  All pipelines successfully processed, skipping remaining batches")
                    break

                print(f"\n  Processing batch {batch_idx + 1}/{len(batch_names)}: {batch_name}")
                print(f"  Remaining pipelines to process: {len(remaining_pipelines)}")

                # Track pipelines that were successful in this batch
                successful_in_batch = []

                # Process each remaining pipeline in this batch
                for pipeline_name in remaining_pipelines:
                    pipeline_counter += 1

                    # Show progress every 10 pipelines
                    if pipeline_counter % 10 == 0:
                        print(
                            f"\n*** PROGRESS: {pipeline_counter}/{total_pipelines - already_done_count} pipelines checked ({processed_pipelines} successful) ***"
                        )

                    print(f"\n    Processing pipeline {pipeline_counter}: {pipeline_name}")

                    # Try to process this pipeline in the current batch
                    pipeline_success = process_pipeline_for_batch(
                        logs_client, pipeline_name, batch_name, resolution, db_path
                    )

                    if pipeline_success:
                        print(f"    ✓ Pipeline successfully processed in batch {batch_name}")
                        successful_in_batch.append(pipeline_name)
                        processed_pipelines += 1
                    else:
                        print(f"    ✗ Pipeline failed in batch {batch_name}, will try next batch")

                # Remove successful pipelines from remaining list
                for pipeline_name in successful_in_batch:
                    remaining_pipelines.remove(pipeline_name)

                print(
                    f"  Batch {batch_name} completed: {len(successful_in_batch)} pipelines successful, {len(remaining_pipelines)} remaining"
                )

            # Report any pipelines that couldn't be processed in any batch
            if remaining_pipelines:
                print(f"  WARNING: {len(remaining_pipelines)} pipelines could not be processed in any batch:")
                for pipeline_name in remaining_pipelines:
                    print(f"    - {pipeline_name}")
            else:
                print(f"  All pipelines for {run_file} successfully processed")

    print(f"\n=== SUMMARY ===")
    print(f"Total pipelines in JSON: {total_pipelines}")
    if already_done_count > 0:
        print(f"Previously processed (skipped): {already_done_count}")
        print(f"Newly processed this run: {processed_pipelines - already_done_count}")
    print(f"Total processed pipelines in DB: {processed_pipelines}")
    print(f"Database saved to: {db_path}")


if __name__ == "__main__":
    main()
