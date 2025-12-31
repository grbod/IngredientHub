"""
Scraper management routes.

Endpoints for triggering scrapers, checking status, and getting cron suggestions.
"""

import os
import platform
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel


router = APIRouter(prefix="/api/scrapers", tags=["scrapers"])

# Check if we're on Linux (production) for Xvfb support
IS_LINUX = platform.system() == "Linux"
LOG_DIR = Path("/var/log/ingredienthub") if IS_LINUX else Path(__file__).parent.parent.parent / "output"

# Backend directory path
BACKEND_DIR = Path(__file__).parent.parent.parent

# Vendor configuration
VENDORS = {
    1: {"name": "IngredientsOnline", "script": "IO_scraper.py"},
    2: {"name": "BulkSupplements", "script": "bulksupplements_scraper.py"},
    3: {"name": "BoxNutra", "script": "boxnutra_scraper.py"},
    4: {"name": "TrafaPharma", "script": "trafapharma_scraper.py"},
}

# Track running scraper processes: vendor_id -> pid
running_scrapers: Dict[int, int] = {}


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


def is_process_running(pid: int) -> bool:
    """Check if a process with the given PID is still running."""
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def clean_stale_processes() -> None:
    """Remove PIDs from tracking that are no longer running."""
    stale_vendors = [
        vendor_id
        for vendor_id, pid in running_scrapers.items()
        if not is_process_running(pid)
    ]
    for vendor_id in stale_vendors:
        del running_scrapers[vendor_id]


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
        pid = running_scrapers[vendor_id]
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
        running_scrapers[vendor_id] = process.pid

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
    pid = running_scrapers.get(vendor_id) if is_running else None

    return ScraperStatusResponse(
        vendor_id=vendor_id,
        vendor_name=vendor["name"],
        is_running=is_running,
        pid=pid,
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
