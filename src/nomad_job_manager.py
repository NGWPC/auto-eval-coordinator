import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import aiohttp
import nomad
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    DISPATCHED = "dispatched"
    ALLOCATED = "allocated"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LOST = "lost"
    UNKNOWN = "unknown"


# Mapping from Nomad statuses to our internal statuses
NOMAD_STATUS_MAP = {
    "pending": JobStatus.ALLOCATED,
    "running": JobStatus.RUNNING,
    "complete": JobStatus.SUCCEEDED,
    "failed": JobStatus.FAILED,
    "lost": JobStatus.LOST,
    "dead": JobStatus.FAILED,
    # Additional mappings for edge cases
    "stopped": JobStatus.FAILED,
    "cancelled": JobStatus.FAILED,
}


class NomadError(Exception):
    pass


class JobNotFoundError(NomadError):
    pass


class JobDispatchError(NomadError):
    pass


@dataclass
class JobTracker:
    """Tracks the state of a single Nomad job."""

    job_id: str
    dispatch_time: float = field(default_factory=time.time)
    status: JobStatus = JobStatus.DISPATCHED
    allocation_id: Optional[str] = None
    exit_code: Optional[int] = None
    completion_event: asyncio.Event = field(default_factory=asyncio.Event)
    error: Optional[Exception] = None
    task_name: Optional[str] = None
    stage: Optional[str] = None
    timestamp: Optional[datetime] = None


class NomadJobManager:
    def __init__(
        self,
        nomad_addr: str,
        namespace: str = "default",
        token: Optional[str] = None,
        session: Optional[aiohttp.ClientSession] = None,
        log_db: Optional[Any] = None,
        max_concurrent_dispatch: int = 2,
        polling_interval: int = 30,
        job_timeout: int = 7200,
    ):
        self.nomad_addr = nomad_addr
        self.namespace = namespace
        self.token = token
        self.session = session
        self.log_db = log_db
        self.max_concurrent_dispatch = max_concurrent_dispatch
        self.polling_interval = polling_interval
        self.job_timeout = job_timeout

        parsed = urlparse(str(nomad_addr))
        self.client = nomad.Nomad(
            host=parsed.hostname,
            port=parsed.port,
            verify=False,
            token=token or None,
            namespace=namespace or None,
        )

        # Create semaphore to limit concurrent Nomad API calls
        self._api_semaphore = asyncio.Semaphore(max_concurrent_dispatch)
        logger.info(f"Nomad API concurrency limited to {max_concurrent_dispatch} calls")

        # Track active jobs
        self._active_jobs: Dict[str, JobTracker] = {}
        self._monitoring_task: Optional[asyncio.Task] = None
        self._polling_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        self._event_index = 0
        self._last_event_time = time.time()

    async def start(self):
        if not self._monitoring_task:
            self._monitoring_task = asyncio.create_task(self._monitor_events())
        if not self._polling_task:
            self._polling_task = asyncio.create_task(self._polling_fallback())
        logger.info("Started Nomad job manager with event monitoring and polling fallback")

    async def stop(self):
        self._shutdown_event.set()
        if self._monitoring_task:
            await self._monitoring_task
            self._monitoring_task = None
        if self._polling_task:
            await self._polling_task
            self._polling_task = None
        logger.info("Stopped Nomad job manager")

    # Retry decorator for Nomad API calls
    _nomad_retry = retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(
            (
                nomad.api.exceptions.BaseNomadException,
                nomad.api.exceptions.URLNotFoundNomadException,
            )
        ),
    )

    @_nomad_retry
    async def _nomad_call(self, func, *args, **kwargs):
        """Execute a Nomad API call with retry logic and concurrency limiting."""
        async with self._api_semaphore:
            logger.debug(
                f"Nomad API call: {func.__name__} (semaphore: {self._api_semaphore._value}/{self.max_concurrent_dispatch})"
            )
            return await asyncio.to_thread(func, *args, **kwargs)

    async def dispatch_and_track(
        self,
        job_name: str,
        prefix: str,
        meta: Optional[Dict[str, str]] = None,
    ) -> Tuple[str, int]:
        """
        Dispatch a job and track it to completion.

        Returns:
            Tuple of (job_id, exit_code)
        """
        # Replace colons with hyphens in prefix to avoid issues with job dispatch strings
        clean_prefix = prefix.replace(":", "-")
        job_id = await self._dispatch_job(job_name, clean_prefix, meta)

        tracker = JobTracker(
            job_id=job_id,
            task_name=job_name,
            stage=meta.get("stage") if meta else None,
        )
        self._active_jobs[job_id] = tracker

        # Update database if available
        await self._update_job_status(tracker)

        # Check if job already completed before we started tracking
        await self._reconcile_job_status(tracker)

        try:
            await self._wait_for_allocation(tracker)

            await self._wait_for_completion(tracker)

            if tracker.error:
                # Create enriched error with exit code and job details
                error_msg = f"Job {job_id} failed with exit code {tracker.exit_code}: {tracker.error}"
                enriched_error = JobDispatchError(error_msg)
                # Attach additional info for programmatic access
                enriched_error.job_id = job_id
                enriched_error.exit_code = tracker.exit_code
                enriched_error.original_error = tracker.error
                enriched_error.allocation_id = tracker.allocation_id
                raise enriched_error

            return job_id, tracker.exit_code or 0

        finally:
            self._active_jobs.pop(job_id, None)

    async def _dispatch_job(
        self,
        job_name: str,
        prefix: str,
        meta: Optional[Dict[str, str]] = None,
    ) -> str:
        """Dispatch a parameterized job."""
        payload = {"Meta": meta} if meta else {}

        try:
            logger.debug(f"Dispatching job {job_name} with meta: {meta}")
            result = await self._nomad_call(
                self.client.job.dispatch_job,
                id_=job_name,
                payload=None,
                meta=meta,
                id_prefix_template=prefix,
            )
            job_id = result["DispatchedJobID"]
            logger.info(f"Dispatched job {job_id} from {job_name}")
            return job_id

        except Exception as e:
            logger.error(f"Failed to dispatch job {job_name}: {e}")
            logger.error(f"Payload was: {payload}")
            raise JobDispatchError(f"Failed to dispatch job {job_name}: {e}")

    async def _wait_for_allocation(self, tracker: JobTracker):
        while True:
            if tracker.allocation_id:
                return
            if tracker.status in (JobStatus.FAILED, JobStatus.LOST):
                raise JobDispatchError(f"Job {tracker.job_id} failed during allocation")
            await asyncio.sleep(1)

    async def _wait_for_completion(self, tracker: JobTracker):
        await tracker.completion_event.wait()

    async def _monitor_events(self):
        """Monitor Nomad event stream for job updates."""
        retry_count = 0
        max_retries = 10
        consecutive_failures = 0

        while not self._shutdown_event.is_set():
            try:
                logger.debug(f"Starting event stream (attempt {retry_count + 1})")
                await self._process_event_stream()
                # If we get here, stream ended gracefully
                retry_count = 0
                consecutive_failures = 0
                logger.info("Event stream ended gracefully")

            except asyncio.CancelledError:
                logger.info("Event monitoring cancelled")
                break

            except Exception as e:
                retry_count += 1
                consecutive_failures += 1

                # Log different levels based on failure type
                if "timeout" in str(e).lower() or "connection" in str(e).lower():
                    logger.warning(f"Event stream connection issue (attempt {retry_count}): {e}")
                else:
                    logger.error(f"Event stream error (attempt {retry_count}): {e}")

                # Give up after too many retries
                if retry_count >= max_retries:
                    logger.error(f"Event stream failed {max_retries} times, giving up on event monitoring")
                    logger.warning("Job manager will rely entirely on polling fallback")
                    break

                # Exponential backoff with jitter
                base_wait = min(2 ** min(retry_count, 6), 60)
                jitter = base_wait * 0.1 * (0.5 - time.time() % 1)
                wait_time = base_wait + jitter

                logger.info(f"Retrying event stream in {wait_time:.1f}s")
                await asyncio.sleep(wait_time)

                # Reset event index if we've had many consecutive failures
                if consecutive_failures >= 3:
                    logger.warning("Resetting event index due to consecutive failures")
                    self._event_index = 0

    async def _process_event_stream(self):
        if not self.session:
            # Create session with reasonable timeouts
            timeout = aiohttp.ClientTimeout(
                total=None,  # No total timeout
                connect=30,  # 30s to connect
                sock_read=120,  # 2 minutes for socket read timeout
            )
            self.session = aiohttp.ClientSession(timeout=timeout)

        url = f"{self.nomad_addr}/v1/event/stream"
        params = {
            "index": self._event_index,
            "namespace": self.namespace,
        }

        headers = {}
        if self.token:
            headers["X-Nomad-Token"] = self.token

        logger.debug(f"Connecting to event stream: {url} (index: {self._event_index})")

        async with self.session.get(url, params=params, headers=headers) as response:
            response.raise_for_status()

            buffer = b""
            async for chunk in response.content.iter_chunked(8 * 1024):
                if self._shutdown_event.is_set():
                    break

                buffer += chunk
                while b"\n" in buffer:
                    line_bytes, buffer = buffer.split(b"\n", 1)
                    line = line_bytes.decode("utf-8").strip()

                    if not line or line == "{}":
                        continue

                    try:
                        data = json.loads(line)
                        if "Index" in data:
                            self._event_index = data["Index"]

                        events = data.get("Events", [])
                        if events:
                            self._last_event_time = time.time()
                        for event in events:
                            await self._handle_event(event)

                    except json.JSONDecodeError:
                        logger.debug(f"Failed to parse event line: {line}")
                    except Exception as e:
                        logger.error(f"Error handling event: {e}")

    async def _polling_fallback(self):
        """Fallback polling mechanism to catch missed job completions."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.polling_interval)

                if not self._active_jobs:
                    continue

                # Log summary of active jobs
                job_summary = {}
                for job_id, tracker in self._active_jobs.items():
                    status = tracker.status.value
                    job_summary[status] = job_summary.get(status, 0) + 1

                summary_str = ", ".join([f"{status}: {count}" for status, count in job_summary.items()])
                logger.debug(f"Polling {len(self._active_jobs)} active jobs [{summary_str}]")

                # Check if event stream is healthy
                event_stream_stale = (time.time() - self._last_event_time) > (self.polling_interval * 2)
                if event_stream_stale:
                    logger.warning("Event stream appears stale, relying more on polling")

                # Check each active job
                jobs_to_check = list(self._active_jobs.keys())
                for job_id in jobs_to_check:
                    if job_id not in self._active_jobs:
                        continue

                    tracker = self._active_jobs[job_id]

                    # Check for job timeout - use graceful degradation
                    job_age = time.time() - tracker.dispatch_time
                    if job_age > self.job_timeout:
                        logger.warning(
                            f"Job {job_id} has exceeded timeout ({self.job_timeout}s), checking via polling..."
                        )

                        # Try to get job status one more time via polling
                        try:
                            await self._poll_job_status(tracker, force_check=True)

                            # If still not in terminal state after forced polling, mark as failed
                            if tracker.status not in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.LOST):
                                logger.error(
                                    f"Job {job_id} timed out after {self.job_timeout}s - no status found via polling"
                                )
                                tracker.status = JobStatus.FAILED
                                tracker.error = TimeoutError(f"Job timed out after {self.job_timeout}s")
                                tracker.completion_event.set()
                                await self._update_job_status(tracker)
                            else:
                                logger.info(
                                    f"Job {job_id} status recovered via timeout polling: {tracker.status.value}"
                                )
                        except Exception as e:
                            logger.error(f"Job {job_id} timed out and polling failed: {e}")
                            tracker.status = JobStatus.FAILED
                            tracker.error = TimeoutError(
                                f"Job timed out after {self.job_timeout}s and polling failed: {e}"
                            )
                            tracker.completion_event.set()
                            await self._update_job_status(tracker)
                        continue

                    # Skip if already completed
                    if tracker.status in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.LOST):
                        continue

                    # Poll job status from Nomad
                    try:
                        await self._poll_job_status(tracker, force_check=event_stream_stale)
                    except Exception as e:
                        logger.error(f"Error polling job {job_id}: {e}")

                # Periodically check for orphaned jobs (every 5 polling cycles)
                if len(jobs_to_check) > 0 and time.time() % (self.polling_interval * 5) < self.polling_interval:
                    await self._check_for_orphaned_jobs()

            except Exception as e:
                logger.error(f"Error in polling fallback: {e}")
                await asyncio.sleep(5)

    async def _poll_job_status(self, tracker: JobTracker, force_check: bool = False):
        """Poll a single job's status from Nomad API."""
        try:
            # Get job status
            job = await self._nomad_call(self.client.job.get_job, tracker.job_id)
            job_status = job.get("Status", "unknown").lower()

            # Get allocations for more detailed status
            allocations = await self._nomad_call(self.client.job.get_allocations, tracker.job_id)

            if not allocations:
                logger.debug(f"No allocations found for job {tracker.job_id}")
                return

            # Find the most recent allocation
            latest_alloc = max(allocations, key=lambda a: a.get("CreateTime", 0))
            alloc_id = latest_alloc.get("ID")
            client_status = latest_alloc.get("ClientStatus", "").lower()

            # Update allocation ID if not set
            if not tracker.allocation_id and alloc_id:
                tracker.allocation_id = alloc_id
                logger.info(f"Job {tracker.job_id} allocation discovered via polling: {alloc_id}")

            # Check if status changed
            old_status = tracker.status
            new_status = NOMAD_STATUS_MAP.get(client_status, JobStatus.UNKNOWN)

            if new_status != JobStatus.UNKNOWN and (new_status != old_status or force_check):
                logger.info(f"Job {tracker.job_id} status change via polling: {old_status.value} -> {new_status.value}")
                tracker.status = new_status
                tracker.timestamp = datetime.now(timezone.utc)

                # Extract exit code for completed jobs
                if client_status == "complete":
                    task_states = latest_alloc.get("TaskStates", {})
                    for task_state in task_states.values():
                        if task_state.get("FinishedAt"):
                            tracker.exit_code = task_state.get("ExitCode", 0)
                            break
                elif client_status == "failed":
                    task_states = latest_alloc.get("TaskStates", {})
                    failure_details = []
                    for task_name, task_state in task_states.items():
                        if task_state.get("FinishedAt"):
                            exit_code = task_state.get("ExitCode", 1)
                            tracker.exit_code = exit_code
                            failed_reason = task_state.get("Failed", "unknown")
                            
                            # Collect failure events for detailed logging
                            events = task_state.get("Events", [])
                            failure_events = [e for e in events if e.get("Type") in ["Task Failed", "Driver Failure"]]
                            
                            failure_detail = f"Task {task_name}: exit_code={exit_code}, reason={failed_reason}"
                            if failure_events:
                                event_msgs = [e.get("DisplayMessage", "") for e in failure_events[-2:]]  # Last 2 events
                                failure_detail += f", events={'; '.join(filter(None, event_msgs))}"
                            
                            failure_details.append(failure_detail)
                            logger.info(f"Job {tracker.job_id} failure details via polling: {failure_detail}")
                            break
                    
                    # Store comprehensive failure info in tracker
                    if failure_details and not tracker.error:
                        tracker.error = Exception(f"Job failed: {'; '.join(failure_details)}")

                # Update database
                await self._update_job_status(tracker)

                # Signal completion if terminal state
                if new_status in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.LOST):
                    tracker.completion_event.set()
                    logger.info(f"Job {tracker.job_id} completed via polling with status {new_status}")

        except nomad.api.exceptions.URLNotFoundNomadException:
            logger.warning(f"Job {tracker.job_id} not found in Nomad - may have been garbage collected")
            # Don't mark as failed immediately - could be a temporary issue
            if force_check:
                logger.error(f"Job {tracker.job_id} confirmed missing from Nomad")
                # Only mark as lost if this was a forced check (e.g., from timeout)
                tracker.status = JobStatus.LOST
                tracker.completion_event.set()
                await self._update_job_status(tracker)
        except Exception as e:
            logger.error(f"Failed to poll status for job {tracker.job_id}: {e}")
            # Don't mark as failed here - let timeout handling deal with it

    async def _reconcile_job_status(self, tracker: JobTracker):
        """Check if a job has already completed before we started tracking it."""
        try:
            logger.debug(f"Reconciling status for newly tracked job {tracker.job_id}")

            # Poll the job immediately to see current state
            await self._poll_job_status(tracker, force_check=True)

            # If the job is already in a terminal state, no need to wait
            if tracker.status in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.LOST):
                logger.info(f"Job {tracker.job_id} was already completed at dispatch time with status {tracker.status}")
                tracker.completion_event.set()

        except Exception as e:
            logger.warning(f"Failed to reconcile status for job {tracker.job_id}: {e}")
            # Continue anyway - polling fallback will catch it

    async def _check_for_orphaned_jobs(self):
        """Check for jobs that may have been missed by event tracking."""
        try:
            logger.debug("Checking for orphaned jobs")

            # Get list of recent jobs from Nomad
            jobs = await self._nomad_call(self.client.jobs.get_jobs)

            # Look for dispatched jobs that we're not tracking
            for job in jobs:
                job_id = job.get("ID", "")
                job_type = job.get("Type", "")
                job_status = job.get("Status", "").lower()

                # Only check parameterized jobs (dispatched jobs)
                if job_type != "batch" or not job_id:
                    continue

                # Skip if we're already tracking this job
                if job_id in self._active_jobs:
                    continue

                # Skip very old jobs (older than 1 hour)
                submit_time = job.get("SubmitTime", 0)
                if submit_time and (time.time() * 1e9 - submit_time) > 3600 * 1e9:
                    continue

                # Check if this job looks like it might be ours
                # (This is a heuristic - you might want to add metadata checks)
                if job_status in ["running", "complete", "failed"] and "hand-inundator" in job_id:
                    logger.warning(f"Found potentially orphaned job: {job_id} (status: {job_status})")
                    # Note: We don't auto-track these as it could cause confusion
                    # This is just for monitoring/alerting purposes

        except Exception as e:
            logger.error(f"Error checking for orphaned jobs: {e}")

    async def _handle_event(self, event: Dict[str, Any]):
        """Handle a single Nomad event."""
        topic = event.get("Topic")
        event_type = event.get("Type", "unknown")

        logger.debug(f"Received event: topic={topic}, type={event_type}")

        if topic != "Allocation":
            return

        payload = event.get("Payload", {}).get("Allocation", {})
        job_id = payload.get("JobID")
        allocation_id = payload.get("ID")
        client_status = payload.get("ClientStatus", "").lower()

        logger.debug(f"Allocation event: job_id={job_id}, alloc_id={allocation_id}, status={client_status}")

        if not job_id:
            logger.debug("Skipping event with no job ID")
            return

        if job_id not in self._active_jobs:
            logger.debug(f"Skipping event for untracked job {job_id}")
            return

        tracker = self._active_jobs[job_id]
        old_status = tracker.status

        logger.debug(f"Processing event for tracked job {job_id}: {old_status.value} -> {client_status}")

        # Update allocation ID
        if not tracker.allocation_id and allocation_id:
            tracker.allocation_id = allocation_id
            logger.info(f"Job {job_id} allocated: {allocation_id}")

        # Map status
        new_status = NOMAD_STATUS_MAP.get(client_status, JobStatus.UNKNOWN)

        if new_status != JobStatus.UNKNOWN and new_status != old_status:
            logger.info(f"Job {job_id} status change via event: {old_status.value} -> {new_status.value}")
            tracker.status = new_status
            tracker.timestamp = datetime.now(timezone.utc)

            # Extract exit code for completed jobs
            if client_status == "complete":
                task_states = payload.get("TaskStates", {})
                for task_name, task_state in task_states.items():
                    if task_state.get("FinishedAt"):
                        exit_code = task_state.get("ExitCode", 0)
                        tracker.exit_code = exit_code
                        logger.debug(f"Job {job_id} task {task_name} finished with exit code {exit_code}")
                        break
            elif client_status == "failed":
                task_states = payload.get("TaskStates", {})
                failure_details = []
                for task_name, task_state in task_states.items():
                    if task_state.get("FinishedAt"):
                        exit_code = task_state.get("ExitCode", 1)
                        tracker.exit_code = exit_code
                        failed_reason = task_state.get("Failed", "unknown")
                        
                        # Collect failure events for detailed logging
                        events = task_state.get("Events", [])
                        failure_events = [e for e in events if e.get("Type") in ["Task Failed", "Driver Failure"]]
                        
                        failure_detail = f"Task {task_name}: exit_code={exit_code}, reason={failed_reason}"
                        if failure_events:
                            event_msgs = [e.get("DisplayMessage", "") for e in failure_events[-2:]]  # Last 2 events
                            failure_detail += f", events={'; '.join(filter(None, event_msgs))}"
                        
                        failure_details.append(failure_detail)
                        logger.info(f"Job {job_id} failure details: {failure_detail}")
                        break
                
                # Store comprehensive failure info in tracker
                if failure_details:
                    tracker.error = Exception(f"Job failed: {'; '.join(failure_details)}")
                else:
                    tracker.error = Exception(f"Job failed with exit code {tracker.exit_code}")

            # Update database
            await self._update_job_status(tracker)

            if new_status in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.LOST):
                tracker.completion_event.set()
                logger.info(
                    f"Job {job_id} completed via event with status {new_status.value} (exit_code: {tracker.exit_code})"
                )
        elif new_status == JobStatus.UNKNOWN:
            logger.warning(f"Job {job_id} received unknown status: {client_status}")
        else:
            logger.debug(f"Job {job_id} status unchanged: {old_status.value}")

    async def _update_job_status(self, tracker: JobTracker):
        if not self.log_db:
            return

        try:
            status_str = tracker.status.value

            # Map internal status to database status if needed
            if tracker.status == JobStatus.ALLOCATED:
                status_str = "allocated"
            elif tracker.status == JobStatus.SUCCEEDED:
                status_str = "succeeded"

            await self.log_db.update_job_status(
                job_id=tracker.job_id,
                status=status_str,
                stage=tracker.stage or "unknown",
            )

        except Exception as e:
            logger.error(f"Failed to update job status in database: {e}")

    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        if job_id in self._active_jobs:
            tracker = self._active_jobs[job_id]
            return {
                "job_id": job_id,
                "status": tracker.status.value,
                "allocation_id": tracker.allocation_id,
                "exit_code": tracker.exit_code,
                "dispatch_time": tracker.dispatch_time,
            }

        # Query Nomad for job status
        try:
            job = await self._nomad_call(self.client.job.get_job, job_id)
            status = job.get("Status", "unknown").lower()
            return {
                "job_id": job_id,
                "status": NOMAD_STATUS_MAP.get(status, JobStatus.UNKNOWN).value,
            }
        except Exception as e:
            logger.error(f"Failed to get job status: {e}")
            raise JobNotFoundError(f"Job {job_id} not found")
