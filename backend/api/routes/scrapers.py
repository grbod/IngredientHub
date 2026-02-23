"""
Scraper management routes.

Endpoints for triggering scrapers, checking status, getting cron suggestions,
and streaming log output via SSE.
"""

import asyncio
import json
import os
import platform
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel


router = APIRouter(prefix="/api/scrapers", tags=["scrapers"])

# Check if we're on Linux (production) for Xvfb support
IS_LINUX = platform.system() == "Linux"
LOG_DIR = Path("/var/log/ingredienthub") if IS_LINUX else Path(__file__).parent.parent.parent / "output"

# Backend directory path
BACKEND_DIR = Path(__file__).parent.parent.parent
STATE_DIR = BACKEND_DIR / ".runtime"

# Vendor configuration
VENDORS = {
    1: {"name": "IngredientsOnline", "script": "IO_scraper.py"},
    2: {"name": "BulkSupplements", "script": "bulksupplements_scraper.py"},
    3: {"name": "BoxNutra", "script": "boxnutra_scraper.py"},
    4: {"name": "TrafaPharma", "script": "trafapharma_scraper.py"},
}

# Track running scraper processes: vendor_id -> (pid, log_file_path)
running_scrapers: Dict[int, Tuple[int, Path]] = {}


class RunScraperRequest(BaseModel):
    """Request body for triggering a scraper run."""
    max_products: Optional[int] = None
    no_playwright: Optional[bool] = None


class RunScraperResponse(BaseModel):
    """Response after triggering a scraper."""
    message: str
    pid: int
    vendor_id: int
    vendor_name: str


class ScraperStatusResponse(BaseModel):
    """Response for scraper status check."""
    vendor_id: int
    vendor_name: str
    is_running: bool
    pid: Optional[int] = None


class CronSuggestion(BaseModel):
    """Cron schedule suggestion for a vendor."""
    vendor_id: int
    vendor_name: str
    cron: str
    description: str
    command: str


class LogFileInfo(BaseModel):
    """Metadata for a scraper log file."""
    filename: str
    modified_at: datetime
    size_bytes: int
    is_active: bool
    summary: Dict[str, str]


def is_process_running(pid: int) -> bool:
    """Check if a process with the given PID is still running (not zombie)."""
    try:
        os.kill(pid, 0)
        # Process exists, but check if it's a zombie
        # Use waitpid with WNOHANG to reap zombies without blocking
        result = os.waitpid(pid, os.WNOHANG)
        if result[0] != 0:
            # Process was reaped - it was a zombie
            return False
        return True
    except ChildProcessError:
        # Not our child process, check via ps command
        import subprocess
        try:
            result = subprocess.run(
                ['ps', '-p', str(pid), '-o', 'state='],
                capture_output=True, text=True, timeout=1
            )
            state = result.stdout.strip()
            # Z = zombie, empty = doesn't exist
            return state and state[0] != 'Z'
        except Exception:
            return False
    except OSError:
        return False


def state_file_for_vendor(vendor_id: int) -> Path:
    """Get state file path for a vendor scraper process."""
    return STATE_DIR / f"scraper_{vendor_id}.json"


def persist_running_scraper(vendor_id: int, pid: int, log_file: Path) -> None:
    """Persist running scraper PID/log file to disk for restart recovery."""
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        state = {"pid": pid, "log_file": str(log_file)}
        state_file_for_vendor(vendor_id).write_text(json.dumps(state))
    except Exception:
        # Persistence is best-effort; scraper execution should not fail on disk-state errors.
        pass


def remove_persisted_scraper(vendor_id: int) -> None:
    """Remove persisted scraper state file if present."""
    try:
        state_file = state_file_for_vendor(vendor_id)
        if state_file.exists():
            state_file.unlink()
    except Exception:
        pass


def hydrate_running_scraper(vendor_id: int) -> None:
    """
    Rehydrate in-memory scraper state from persisted state file if valid.
    """
    if vendor_id in running_scrapers:
        return

    state_file = state_file_for_vendor(vendor_id)
    if not state_file.exists():
        return

    try:
        state = json.loads(state_file.read_text())
        pid = int(state["pid"])
        log_path = Path(state["log_file"])
        if not log_path.is_absolute():
            log_path = LOG_DIR / log_path
    except Exception:
        remove_persisted_scraper(vendor_id)
        return

    if is_process_running(pid):
        running_scrapers[vendor_id] = (pid, log_path)
    else:
        remove_persisted_scraper(vendor_id)


def clean_stale_processes() -> None:
    """Remove PIDs from tracking that are no longer running."""
    # Rehydrate from persisted files first (handles API process restarts).
    for vendor_id in VENDORS:
        hydrate_running_scraper(vendor_id)

    stale_vendors = [
        vendor_id
        for vendor_id, (pid, _) in running_scrapers.items()
        if not is_process_running(pid)
    ]
    for vendor_id in stale_vendors:
        del running_scrapers[vendor_id]
        remove_persisted_scraper(vendor_id)


def get_latest_log_file(vendor_id: int) -> Optional[Path]:
    """Find the most recent log file for a vendor."""
    if vendor_id not in VENDORS:
        return None

    vendor = VENDORS[vendor_id]
    script_base = vendor["script"].replace(".py", "")

    # Find all log files matching the pattern
    log_files = list(LOG_DIR.glob(f"{script_base}_*.log"))
    if not log_files:
        return None

    # Sort by modification time, most recent first
    log_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return log_files[0]


def get_vendor_log_files(vendor_id: int, limit: int = 20) -> List[Path]:
    """Get vendor log files sorted by modified time (newest first)."""
    if vendor_id not in VENDORS:
        return []

    script_base = VENDORS[vendor_id]["script"].replace(".py", "")
    log_files = list(LOG_DIR.glob(f"{script_base}_*.log"))
    log_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return log_files[:limit]


def parse_log_summary(log_path: Path) -> Dict[str, str]:
    """Extract summary key/value metrics from the scraper statistics report."""
    try:
        lines = log_path.read_text(errors="replace").splitlines()
    except Exception:
        return {}

    summary: Dict[str, str] = {}
    in_report = False

    # The report is near the end; scanning tail keeps parsing cheap.
    for line in lines[-1200:]:
        text = line.strip()
        if "SCRAPE STATISTICS REPORT" in text:
            in_report = True
            continue

        if not in_report:
            continue

        if not text or text.startswith("===") or text.startswith("---"):
            continue

        if ":" in text:
            key, value = text.split(":", 1)
            key_norm = re.sub(r"[^a-z0-9]+", "_", key.strip().lower()).strip("_")
            if key_norm:
                summary[key_norm] = value.strip()

    # Useful tail fallback values if report parse is sparse.
    for line in reversed(lines[-200:]):
        text = line.strip()
        if text.startswith("Completed at:"):
            summary.setdefault("completed_at", text.split(":", 1)[1].strip())
        elif text.startswith("Total products scraped:"):
            summary.setdefault("total_products_scraped", text.split(":", 1)[1].strip())
        elif text.startswith("Total variants saved:"):
            summary.setdefault("total_variants_saved", text.split(":", 1)[1].strip())

    return summary


async def tail_log_file(log_path: Path, vendor_id: int):
    """
    Async generator that tails a log file and yields SSE events.
    Continues until the scraper process completes.
    """
    last_position = 0

    # Wait for log file to exist
    for _ in range(50):  # Wait up to 5 seconds
        if log_path.exists():
            break
        await asyncio.sleep(0.1)

    if not log_path.exists():
        yield f"data: [ERROR] Log file not found: {log_path}\n\n"
        return

    yield f"data: [CONNECTED] Streaming logs from {log_path.name}\n\n"

    while True:
        try:
            # Check if process is still running
            clean_stale_processes()
            is_running = vendor_id in running_scrapers

            # Read new content
            with open(log_path, "r") as f:
                f.seek(last_position)
                new_content = f.read()
                last_position = f.tell()

            # Yield new lines
            if new_content:
                for line in new_content.splitlines():
                    if line.strip():
                        # Escape any special characters and format as SSE
                        escaped_line = line.replace("\n", "\\n")
                        yield f"data: {escaped_line}\n\n"

            # If process completed, send final message and exit
            if not is_running:
                yield "data: [COMPLETED] Scraper process finished\n\n"
                break

            # Small delay before next check
            await asyncio.sleep(0.5)

        except Exception as e:
            yield f"data: [ERROR] {str(e)}\n\n"
            break


@router.post("/{vendor_id}/run", response_model=RunScraperResponse)
def run_scraper(vendor_id: int, request: RunScraperRequest = None):
    """
    Trigger a scraper run for the specified vendor.

    Args:
        vendor_id: The vendor ID (1=IO, 2=BS, 3=BN, 4=TP)
        request: Optional parameters for the scraper run

    Returns:
        Response with process ID and status message

    Raises:
        HTTPException: If vendor not found or scraper already running
    """
    if vendor_id not in VENDORS:
        raise HTTPException(
            status_code=404,
            detail=f"Vendor {vendor_id} not found. Valid IDs: {list(VENDORS.keys())}"
        )

    # Clean up stale process entries
    clean_stale_processes()

    # Check if scraper is already running
    if vendor_id in running_scrapers:
        pid, _ = running_scrapers[vendor_id]
        raise HTTPException(
            status_code=409,
            detail=f"Scraper for {VENDORS[vendor_id]['name']} is already running (PID: {pid})"
        )

    vendor = VENDORS[vendor_id]
    script_path = BACKEND_DIR / vendor["script"]

    if not script_path.exists():
        raise HTTPException(
            status_code=500,
            detail=f"Scraper script not found: {script_path}"
        )

    # Build command arguments
    # On Linux, wrap with xvfb-run for headed browser support
    if IS_LINUX:
        cmd = ["xvfb-run", "-a", sys.executable, str(script_path)]
    else:
        cmd = [sys.executable, str(script_path)]

    if request:
        if request.max_products is not None:
            cmd.extend(["--max-products", str(request.max_products)])
        if request.no_playwright:
            cmd.append("--no-playwright")

    # Set up log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"{vendor['script'].replace('.py', '')}_{timestamp}.log"
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Launch scraper as background process
    try:
        with open(log_file, "w") as log_handle:
            process = subprocess.Popen(
                cmd,
                cwd=str(BACKEND_DIR),
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        running_scrapers[vendor_id] = (process.pid, log_file)
        persist_running_scraper(vendor_id, process.pid, log_file)

        return RunScraperResponse(
            message=f"Started {vendor['name']} scraper (log: {log_file})",
            pid=process.pid,
            vendor_id=vendor_id,
            vendor_name=vendor["name"],
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start scraper: {str(e)}"
        )


@router.get("/{vendor_id}/status", response_model=ScraperStatusResponse)
def get_scraper_status(vendor_id: int):
    """
    Check if a scraper is currently running for the specified vendor.

    Args:
        vendor_id: The vendor ID to check

    Returns:
        Status information including whether the scraper is running
    """
    if vendor_id not in VENDORS:
        raise HTTPException(
            status_code=404,
            detail=f"Vendor {vendor_id} not found. Valid IDs: {list(VENDORS.keys())}"
        )

    # Clean up stale process entries
    clean_stale_processes()

    vendor = VENDORS[vendor_id]
    is_running = vendor_id in running_scrapers
    pid = running_scrapers[vendor_id][0] if is_running else None

    return ScraperStatusResponse(
        vendor_id=vendor_id,
        vendor_name=vendor["name"],
        is_running=is_running,
        pid=pid,
    )


class StopScraperResponse(BaseModel):
    """Response after stopping a scraper."""
    message: str
    vendor_id: int
    vendor_name: str
    pid: int


@router.post("/{vendor_id}/stop", response_model=StopScraperResponse)
def stop_scraper(vendor_id: int):
    """
    Stop a running scraper for the specified vendor.

    Args:
        vendor_id: The vendor ID to stop

    Returns:
        Response with stopped process information

    Raises:
        HTTPException: If vendor not found or no scraper running
    """
    if vendor_id not in VENDORS:
        raise HTTPException(
            status_code=404,
            detail=f"Vendor {vendor_id} not found. Valid IDs: {list(VENDORS.keys())}"
        )

    # Clean up stale process entries
    clean_stale_processes()

    if vendor_id not in running_scrapers:
        raise HTTPException(
            status_code=404,
            detail=f"No scraper running for {VENDORS[vendor_id]['name']}"
        )

    pid, _ = running_scrapers[vendor_id]
    vendor = VENDORS[vendor_id]

    try:
        # Send SIGTERM to gracefully stop the process
        os.kill(pid, 15)  # SIGTERM
        del running_scrapers[vendor_id]
        remove_persisted_scraper(vendor_id)

        return StopScraperResponse(
            message=f"Stopped {vendor['name']} scraper",
            vendor_id=vendor_id,
            vendor_name=vendor["name"],
            pid=pid,
        )
    except OSError as e:
        # Process might have already exited
        if vendor_id in running_scrapers:
            del running_scrapers[vendor_id]
        remove_persisted_scraper(vendor_id)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stop scraper: {str(e)}"
        )


@router.get("/{vendor_id}/logs/history", response_model=List[LogFileInfo])
def list_log_history(
    vendor_id: int,
    limit: int = Query(20, ge=1, le=100, description="Max number of files to return"),
):
    """
    List available log files for a vendor with parsed summary metadata.
    """
    if vendor_id not in VENDORS:
        raise HTTPException(
            status_code=404,
            detail=f"Vendor {vendor_id} not found. Valid IDs: {list(VENDORS.keys())}"
        )

    clean_stale_processes()

    active_log: Optional[Path] = None
    if vendor_id in running_scrapers:
        _, active_log = running_scrapers[vendor_id]

    files = get_vendor_log_files(vendor_id, limit=limit)
    items: List[LogFileInfo] = []
    for log_file in files:
        stat = log_file.stat()
        is_active = bool(active_log and active_log.resolve() == log_file.resolve())
        items.append(LogFileInfo(
            filename=log_file.name,
            modified_at=datetime.fromtimestamp(stat.st_mtime),
            size_bytes=stat.st_size,
            is_active=is_active,
            summary=parse_log_summary(log_file),
        ))

    return items


@router.get("/{vendor_id}/logs")
async def stream_logs(
    vendor_id: int,
    file: Optional[str] = Query(None, description="Optional specific log filename to stream"),
):
    """
    Stream scraper logs via Server-Sent Events (SSE).

    If the scraper is currently running, streams the active log file.
    Otherwise, streams the most recent log file for the vendor.

    Args:
        vendor_id: The vendor ID to stream logs for

    Returns:
        StreamingResponse with text/event-stream content type
    """
    if vendor_id not in VENDORS:
        raise HTTPException(
            status_code=404,
            detail=f"Vendor {vendor_id} not found. Valid IDs: {list(VENDORS.keys())}"
        )

    # Clean up stale process entries
    clean_stale_processes()

    # Select log file path:
    # - If explicit file is provided, use it.
    # - Otherwise prefer active run's log, then fall back to most recent.
    if file:
        script_base = VENDORS[vendor_id]["script"].replace(".py", "")
        if Path(file).name != file or not file.startswith(f"{script_base}_") or not file.endswith(".log"):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid log filename for {VENDORS[vendor_id]['name']}"
            )
        log_path = LOG_DIR / file
    elif vendor_id in running_scrapers:
        _, log_path = running_scrapers[vendor_id]
    else:
        log_path = get_latest_log_file(vendor_id)

    if not log_path:
        raise HTTPException(
            status_code=404,
            detail=f"No log files found for {VENDORS[vendor_id]['name']}"
        )

    return StreamingResponse(
        tail_log_file(log_path, vendor_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )


@router.get("/cron-suggestions", response_model=List[CronSuggestion])
def get_cron_suggestions():
    """
    Get recommended crontab entries for scheduling scraper runs.

    Returns:
        List of cron suggestions for each vendor with schedule and command
    """
    # Get the backend directory path for the command
    backend_path = str(BACKEND_DIR)
    venv_activate = f"source {backend_path}/venv/bin/activate"

    suggestions = [
        CronSuggestion(
            vendor_id=1,
            vendor_name="IngredientsOnline",
            cron="0 2 * * 1",
            description="Weekly on Monday at 2:00 AM",
            command=f"cd {backend_path} && {venv_activate} && python IO_scraper.py >> /var/log/ingredienthub/io.log 2>&1",
        ),
        CronSuggestion(
            vendor_id=2,
            vendor_name="BulkSupplements",
            cron="0 3 * * 2",
            description="Weekly on Tuesday at 3:00 AM",
            command=f"cd {backend_path} && {venv_activate} && python bulksupplements_scraper.py >> /var/log/ingredienthub/bs.log 2>&1",
        ),
        CronSuggestion(
            vendor_id=3,
            vendor_name="BoxNutra",
            cron="0 4 * * 3",
            description="Weekly on Wednesday at 4:00 AM",
            command=f"cd {backend_path} && {venv_activate} && python boxnutra_scraper.py >> /var/log/ingredienthub/bn.log 2>&1",
        ),
        CronSuggestion(
            vendor_id=4,
            vendor_name="TrafaPharma",
            cron="0 5 * * 4",
            description="Weekly on Thursday at 5:00 AM",
            command=f"cd {backend_path} && {venv_activate} && python trafapharma_scraper.py >> /var/log/ingredienthub/tp.log 2>&1",
        ),
    ]

    return suggestions
