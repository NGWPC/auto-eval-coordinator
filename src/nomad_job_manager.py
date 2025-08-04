import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set, Tuple
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
    """Internal job status enumeration aligned with Nomad ClientStatus values."""

    DISPATCHED = "dispatched"
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LOST = "lost"
    STOPPED = "stopped"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


# Mapping from Nomad statuses to our internal statuses
NOMAD_STATUS_MAP = {
    "pending": JobStatus.PENDING,
    "running": JobStatus.RUNNING,
    "complete": JobStatus.SUCCEEDED,
    "failed": JobStatus.FAILED,
    "lost": JobStatus.LOST,
    "dead": JobStatus.STOPPED,
    "stopped": JobStatus.STOPPED,
    "cancelled": JobStatus.CANCELLED,
}


class NomadError(Exception):
    """Base exception for the NomadJobManager."""

    pass


class JobNotFoundError(NomadError):
    """Raised when a specific job cannot be found in Nomad."""

    pass


class JobDispatchError(NomadError):
    """
    Raised when a job fails to dispatch or fails during execution.
    Contains enriched details about the failure.
    """

    def __init__(self, message: str, **kwargs):
        super().__init__(message)
        self.job_id: Optional[str] = kwargs.get("job_id")
        self.allocation_id: Optional[str] = kwargs.get("allocation_id")
        self.exit_code: Optional[int] = kwargs.get("exit_code")
        self.original_error: Optional[Exception] = kwargs.get("original_error")


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


class _NomadAPIClient:
    """A wrapper for Nomad API calls that provides retries and concurrency limiting."""

    def __init__(self, client: nomad.Nomad, max_concurrency: int):
        self.client = client
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._nomad_retry = retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=30),
            retry=retry_if_exception_type(
                (
                    nomad.api.exceptions.BaseNomadException,
                    nomad.api.exceptions.URLNotFoundNomadException,
                )
            ),
        )

    async def _call(self, func: Callable, *args, **kwargs) -> Any:
        async with self.semaphore:
            return await asyncio.to_thread(func, *args, **kwargs)

    async def dispatch_job(
        self, job_name: str, prefix: str, meta: Optional[Dict[str, str]]
    ) -> str:
        wrapped_call = self._nomad_retry(self._call)
        result = await wrapped_call(
            self.client.job.dispatch_job,
            id_=job_name,
            payload=None,
            meta=meta,
            id_prefix_template=prefix.replace(":", "-"),
        )
        return result["DispatchedJobID"]

    async def get_job(self, job_id: str) -> Dict[str, Any]:
        wrapped_call = self._nomad_retry(self._call)
        return await wrapped_call(self.client.job.get_job, job_id)

    async def get_allocations(self, job_id: str) -> List[Dict[str, Any]]:
        wrapped_call = self._nomad_retry(self._call)
        return await wrapped_call(self.client.job.get_allocations, job_id)


class _EventMonitor:
    """Monitors the Nomad event stream and dispatches events to the manager."""

    def __init__(
        self,
        nomad_addr: str,
        namespace: str,
        token: Optional[str],
        get_active_jobs_cb: Callable[[], Set[str]],
        handle_event_cb: Callable[[Dict[str, Any]], Coroutine],
        restart_event: asyncio.Event,
        shutdown_event: asyncio.Event,
        filter_by_jobs: bool,
    ):
        self.nomad_addr = nomad_addr.rstrip("/")
        self.namespace = namespace
        self.token = token
        self._get_active_jobs = get_active_jobs_cb
        self._handle_event = handle_event_cb
        self._restart_event = restart_event
        self._shutdown_event = shutdown_event
        self.filter_by_jobs = filter_by_jobs

        self.session: Optional[aiohttp.ClientSession] = None
        self.event_index = 0
        self.last_event_time = time.time()
        self.task: Optional[asyncio.Task] = None

    def start(self):
        if not self.task:
            self.task = asyncio.create_task(self._monitor_loop())

    async def stop(self):
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
        if self.session and not self.session.closed:
            await self.session.close()

    async def _monitor_loop(self):
        retry_count = 0
        max_retries = 10
        while not self._shutdown_event.is_set():
            try:
                self._restart_event.clear()
                await self._process_event_stream()
                retry_count = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                retry_count += 1
                logger.warning(
                    f"Event stream error (attempt {retry_count}): {e}"
                )
                if retry_count >= max_retries:
                    logger.error(
                        "Event stream failed too many times, giving up."
                    )
                    break
                wait_time = min(2**retry_count, 60)
                await asyncio.sleep(wait_time)
                if self.session and not self.session.closed:
                    await self.session.close()
                self.session = None

    async def _process_event_stream(self):
        if not self.session or self.session.closed:
            timeout = aiohttp.ClientTimeout(
                total=None, connect=30, sock_read=120
            )
            self.session = aiohttp.ClientSession(timeout=timeout)

        params = {"index": self.event_index, "namespace": self.namespace}
        active_jobs = self._get_active_jobs()
        if self.filter_by_jobs and active_jobs:
            params["jobs"] = ",".join(active_jobs)

        url = f"{self.nomad_addr}/v1/event/stream"
        headers = {"X-Nomad-Token": self.token} if self.token else {}

        async with self.session.get(
            url, params=params, headers=headers
        ) as response:
            response.raise_for_status()
            async for chunk in response.content.iter_chunked(8192):
                if (
                    self._shutdown_event.is_set()
                    or self._restart_event.is_set()
                ):
                    logger.info("Restarting event stream due to signal.")
                    break

                self.last_event_time = time.time()
                for raw_line in chunk.split(b"\n"):
                    if not raw_line.strip():
                        continue
                    try:
                        data = json.loads(raw_line.decode("utf-8"))
                        if "Index" in data:
                            self.event_index = data["Index"]
                        for event in data.get("Events", []):
                            await self._handle_event(event)
                    except json.JSONDecodeError:
                        logger.debug(
                            f"Failed to decode JSON from event stream: {raw_line!r}"
                        )


class _PollingFallback:
    """Periodically polls Nomad to catch missed updates and handle stale jobs."""

    def __init__(
        self,
        api_client: _NomadAPIClient,
        get_active_trackers: Callable[[], List[JobTracker]],
        update_tracker_cb: Callable[[JobTracker, Dict[str, Any]], Coroutine],
        shutdown_event: asyncio.Event,
        poll_interval: int,
        escalation_timeout: int,
    ):
        self.api = api_client
        self._get_active_trackers = get_active_trackers
        self._update_tracker = update_tracker_cb
        self._shutdown_event = shutdown_event
        self.poll_interval = poll_interval
        self.escalation_timeout = escalation_timeout
        self.task: Optional[asyncio.Task] = None

    def start(self):
        if not self.task:
            self.task = asyncio.create_task(self._poll_loop())

    async def stop(self):
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None

    async def _poll_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.poll_interval)
            now = time.time()
            for tracker in self._get_active_trackers():
                if tracker.completion_event.is_set():
                    continue
                is_stale = (
                    now - tracker.dispatch_time
                ) > self.escalation_timeout
                if is_stale:
                    logger.warning(
                        f"Job {tracker.job_id} is stale, forcing status poll."
                    )
                await self.check_job_status(tracker, force_update=is_stale)

    async def check_job_status(
        self, tracker: JobTracker, force_update: bool = False
    ):
        """Polls a single job and updates its tracker if necessary."""
        try:
            allocations = await self.api.get_allocations(tracker.job_id)
            if not allocations:
                if force_update:
                    logger.warning(
                        f"No allocations found for stale job {tracker.job_id}, marking as LOST."
                    )
                    tracker.status = JobStatus.LOST
                    tracker.error = Exception(
                        "Job lost: No allocations found after escalation timeout."
                    )
                    tracker.completion_event.set()
                    await self._update_tracker(
                        tracker, {}
                    )  # Update DB with final state
                return

            latest_alloc = max(
                allocations, key=lambda a: a.get("CreateTime", 0)
            )
            await self._update_tracker(tracker, latest_alloc)

        except (
            nomad.api.exceptions.URLNotFoundNomadException,
            nomad.api.exceptions.URLNotAuthorizedNomadException,
            nomad.api.exceptions.TimeoutNomadException,
        ) as e:
            if force_update:
                logger.error(
                    f"Job {tracker.job_id} encountered a terminal API error after escalation timeout "
                    f"and will be marked as LOST. Error: {e}"
                )
                tracker.status = JobStatus.LOST
                tracker.error = e
                tracker.completion_event.set()
                await self._update_tracker(
                    tracker, {}
                )  # Update DB with final state
            else:
                logger.warning(
                    f"[Polling] Transient API error for {tracker.job_id}: {e}"
                )
        except Exception as e:
            logger.error(
                f"[Polling] Unexpected error while checking status for {tracker.job_id}: {e}"
            )


class NomadJobManager:
    """
    Manages the lifecycle of parameterized Nomad jobs using a combination of
    event streaming and periodic polling.
    """

    def __init__(
        self,
        nomad_addr: str,
        namespace: str = "default",
        token: Optional[str] = None,
        log_db: Optional[Any] = None,
        max_concurrent_dispatch: int = 10,
        polling_interval: int = 30,
        polling_escalation_timeout: int = 1800,
        filter_events_by_jobs: bool = True,
        job_list_change_threshold: int = 5,
    ):
        self.nomad_addr = nomad_addr
        self.namespace = namespace
        self.token = token
        self.log_db = log_db
        self.job_list_change_threshold = job_list_change_threshold

        parsed = urlparse(nomad_addr)
        nomad_client = nomad.Nomad(
            host=parsed.hostname,
            port=parsed.port,
            verify=False,
            token=token,
            namespace=namespace,
        )
        self.api = _NomadAPIClient(nomad_client, max_concurrent_dispatch)

        self._active_jobs: Dict[str, JobTracker] = {}
        self._stream_job_list: Set[str] = set()
        self._shutdown_event = asyncio.Event()
        self._stream_restart_event = asyncio.Event()

        self.event_monitor = _EventMonitor(
            nomad_addr=nomad_addr,
            namespace=namespace,
            token=token,
            get_active_jobs_cb=lambda: set(self._active_jobs.keys()),
            handle_event_cb=self._handle_allocation_event,
            restart_event=self._stream_restart_event,
            shutdown_event=self._shutdown_event,
            filter_by_jobs=filter_events_by_jobs,
        )
        self.poller = _PollingFallback(
            api_client=self.api,
            get_active_trackers=lambda: list(self._active_jobs.values()),
            update_tracker_cb=self._update_tracker_from_allocation,
            shutdown_event=self._shutdown_event,
            poll_interval=polling_interval,
            escalation_timeout=polling_escalation_timeout,
        )

    async def start(self):
        logger.info("Starting NomadJobManager...")
        self.event_monitor.start()
        self.poller.start()

    async def stop(self):
        logger.info("Stopping NomadJobManager...")
        self._shutdown_event.set()
        await self.event_monitor.stop()
        await self.poller.stop()
        logger.info("NomadJobManager stopped.")

    async def dispatch_and_track(
        self,
        job_name: str,
        prefix: str,
        meta: Optional[Dict[str, str]] = None,
    ) -> Tuple[str, int]:
        try:
            job_id = await self.api.dispatch_job(job_name, prefix, meta)
        except Exception as e:
            logger.error(f"Failed to dispatch job {job_name}: {e}")
            raise JobDispatchError(
                f"Failed to dispatch job {job_name}: {e}"
            ) from e

        tracker = JobTracker(
            job_id=job_id, task_name=job_name, stage=(meta or {}).get("stage")
        )
        self._active_jobs[job_id] = tracker
        await self._maybe_restart_event_stream()
        await self._update_db_status(tracker)

        try:
            await self._reconcile(tracker)
            await self._wait_for_allocation(tracker)
            await tracker.completion_event.wait()

            if tracker.error:
                raise JobDispatchError(
                    f"Job {job_id} failed with exit code {tracker.exit_code}: {tracker.error}",
                    job_id=job_id,
                    allocation_id=tracker.allocation_id,
                    exit_code=tracker.exit_code,
                    original_error=tracker.error,
                )
            return job_id, tracker.exit_code or 0
        finally:
            self._active_jobs.pop(job_id, None)
            await self._maybe_restart_event_stream()

    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        if job_id in self._active_jobs:
            t = self._active_jobs[job_id]
            return {
                "job_id": job_id,
                "status": t.status.value,
                "allocation_id": t.allocation_id,
                "exit_code": t.exit_code,
            }
        try:
            job = await self.api.get_job(job_id)
            return {
                "job_id": job_id,
                "status": NOMAD_STATUS_MAP.get(
                    job.get("Status", "").lower(), JobStatus.UNKNOWN
                ).value,
            }
        except Exception as e:
            raise JobNotFoundError(
                f"Job {job_id} not found or API error: {e}"
            ) from e

    async def _handle_allocation_event(self, event: Dict[str, Any]):
        if event.get("Topic") != "Allocation":
            return
        try:
            allocation = event["Payload"]["Allocation"]
            job_id = allocation.get("JobID")
            if not job_id or job_id not in self._active_jobs:
                return
            await self._update_tracker_from_allocation(
                self._active_jobs[job_id], allocation, from_event=True
            )
        except KeyError:
            logger.warning(f"Could not parse allocation from event payload.")

    async def _update_tracker_from_allocation(
        self,
        tracker: JobTracker,
        allocation: Dict[str, Any],
        from_event: bool = False,
    ):
        if tracker.completion_event.is_set():
            return

        old_status = tracker.status
        client_status = allocation.get("ClientStatus", "").lower()
        new_status = NOMAD_STATUS_MAP.get(client_status, JobStatus.UNKNOWN)

        # Handle updates from the poller for lost jobs
        if not allocation and tracker.status == JobStatus.LOST:
            await self._update_db_status(tracker)
            return

        if new_status == old_status:
            return

        source = "event" if from_event else "polling"
        logger.info(
            f"Job {tracker.job_id} status change via {source}: {old_status.name} -> {new_status.name}"
        )

        tracker.status = new_status
        tracker.timestamp = datetime.now(timezone.utc)
        if not tracker.allocation_id:
            tracker.allocation_id = allocation.get("ID")

        is_terminal = new_status in (
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.LOST,
            JobStatus.STOPPED,
            JobStatus.CANCELLED,
        )

        if is_terminal:
            self._extract_final_state(tracker, allocation)
            logger.info(
                f"Job {tracker.job_id} completed via {source} with status JobStatus.{new_status.name} "
                f"(exit_code: {tracker.exit_code})"
            )
            tracker.completion_event.set()

        await self._update_db_status(tracker)

    def _extract_final_state(
        self, tracker: JobTracker, allocation: Dict[str, Any]
    ):
        task_states = allocation.get("TaskStates", {})
        for task_name, task_state in task_states.items():
            if task_state.get("FinishedAt"):
                if tracker.status == JobStatus.FAILED:
                    tracker.exit_code = task_state.get("ExitCode", 1)
                    failure_details = []
                    events = task_state.get("Events", [])
                    for event in reversed(events):
                        if event.get("Type") in [
                            "Terminated",
                            "Task Failed",
                            "Driver Failure",
                            "Killing",
                        ]:
                            reason = event.get(
                                "DisplayMessage", "No reason provided."
                            )
                            failure_details.append(
                                f"Task '{task_name}' failed: {reason}"
                            )
                            break
                    logger.info(
                        f"Job {tracker.job_id} failure details: {'; '.join(failure_details)}"
                    )
                    if not failure_details:
                        failure_details.append(
                            f"Task '{task_name}' failed with exit code {tracker.exit_code}."
                        )
                    tracker.error = Exception("; ".join(failure_details))
                else:
                    tracker.exit_code = task_state.get("ExitCode", 0)
                return

    async def _reconcile(self, tracker: JobTracker):
        logger.debug(f"Reconciling status for new job {tracker.job_id}")
        await self.poller.check_job_status(tracker, force_update=True)

    async def _wait_for_allocation(self, tracker: JobTracker):
        while tracker.allocation_id is None:
            if tracker.completion_event.is_set():
                raise JobDispatchError(
                    f"Job {tracker.job_id} failed before allocation.",
                    job_id=tracker.job_id,
                    exit_code=tracker.exit_code,
                    original_error=tracker.error,
                )
            await asyncio.sleep(0.5)

    async def _maybe_restart_event_stream(self):
        if not self.event_monitor.filter_by_jobs:
            return
        current_jobs = set(self._active_jobs.keys())
        if (
            len(current_jobs.symmetric_difference(self._stream_job_list))
            >= self.job_list_change_threshold
        ):
            logger.info(
                f"Job list changed significantly, triggering event stream restart."
            )
            self._stream_job_list = current_jobs
            self._stream_restart_event.set()

    async def _update_db_status(self, tracker: JobTracker):
        logger.info(
            f"Job {tracker.job_id} status updated: JobStatus.{tracker.status.name}"
        )
        if not self.log_db:
            return
        try:
            await self.log_db.update_job_status(
                job_id=tracker.job_id,
                status=tracker.status.value,
                stage=tracker.stage or "unknown",
            )
        except Exception as e:
            logger.error(
                f"Failed to update job status in DB for {tracker.job_id}: {e}"
            )
