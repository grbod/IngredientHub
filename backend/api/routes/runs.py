"""
Scrape run history routes.

Endpoints for viewing scrape run history, details, and per-run alerts.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from ..services.database import db_pool


router = APIRouter(prefix="/api/runs", tags=["runs"])


class ScrapeRun(BaseModel):
    """Scrape run summary."""
    run_id: int
    vendor_id: int
    vendor_name: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: Optional[str] = None
    products_discovered: Optional[int] = None
    products_processed: Optional[int] = None
    products_skipped: Optional[int] = None
    products_failed: Optional[int] = None
    variants_new: Optional[int] = None
    variants_updated: Optional[int] = None
    variants_unchanged: Optional[int] = None
    variants_stale: Optional[int] = None
    variants_reactivated: Optional[int] = None
    price_alerts: Optional[int] = None
    stock_alerts: Optional[int] = None
    data_quality_alerts: Optional[int] = None
    is_full_scrape: Optional[bool] = None
    max_products_limit: Optional[int] = None


class ScrapeRunListResponse(BaseModel):
    """Response for list of scrape runs."""
    runs: List[ScrapeRun]
    total: int
    limit: int
    offset: int


class RunAlert(BaseModel):
    """Alert associated with a scrape run."""
    alert_id: int
    run_id: int
    vendor_ingredient_id: Optional[int] = None
    alert_type: str
    severity: str
    sku: Optional[str] = None
    product_name: Optional[str] = None
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    change_percent: Optional[float] = None
    message: Optional[str] = None
    created_at: Optional[datetime] = None
    product_url: Optional[str] = None
    ingredient_id: Optional[int] = None


class RunAlertsResponse(BaseModel):
    """Response for alerts of a specific run."""
    alerts: List[RunAlert]
    total: int


def row_to_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a database row to a dictionary, handling special types."""
    result = dict(row)
    # Convert datetime objects to ISO format strings if needed
    for key, value in result.items():
        if isinstance(value, datetime):
            result[key] = value
    return result


@router.get("/", response_model=ScrapeRunListResponse)
def list_runs(
    vendor_id: Optional[int] = Query(None, description="Filter by vendor ID"),
    limit: int = Query(20, ge=1, le=100, description="Number of results to return"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
):
    """
    List scrape runs with optional vendor filter.

    Args:
        vendor_id: Optional vendor ID to filter by
        limit: Maximum number of runs to return (default 20, max 100)
        offset: Number of runs to skip for pagination

    Returns:
        List of scrape runs with pagination info
    """
    with db_pool.get_cursor() as cursor:
        # Build query with optional vendor filter
        where_clause = ""
        params: List[Any] = []

        if vendor_id is not None:
            where_clause = "WHERE sr.vendor_id = %s"
            params.append(vendor_id)

        # Get total count
        count_query = f"""
            SELECT COUNT(*) as total
            FROM scraperuns sr
            {where_clause}
        """
        cursor.execute(count_query, params)
        total = cursor.fetchone()["total"]

        # Get runs with vendor name
        query = f"""
            SELECT
                sr.run_id,
                sr.vendor_id,
                v.name as vendor_name,
                sr.started_at,
                sr.completed_at,
                sr.status,
                sr.products_discovered,
                sr.products_processed,
                sr.products_skipped,
                sr.products_failed,
                sr.variants_new,
                sr.variants_updated,
                sr.variants_unchanged,
                sr.variants_stale,
                sr.variants_reactivated,
                sr.price_alerts,
                sr.stock_alerts,
                sr.data_quality_alerts,
                sr.is_full_scrape,
                sr.max_products_limit
            FROM scraperuns sr
            LEFT JOIN vendors v ON sr.vendor_id = v.vendor_id
            {where_clause}
            ORDER BY sr.started_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        cursor.execute(query, params)
        rows = cursor.fetchall()

        runs = [ScrapeRun(**row_to_dict(row)) for row in rows]

        return ScrapeRunListResponse(
            runs=runs,
            total=total,
            limit=limit,
            offset=offset,
        )


@router.get("/{run_id}", response_model=ScrapeRun)
def get_run(run_id: int):
    """
    Get details of a specific scrape run.

    Args:
        run_id: The ID of the scrape run

    Returns:
        Scrape run details

    Raises:
        HTTPException: If run not found
    """
    with db_pool.get_cursor() as cursor:
        query = """
            SELECT
                sr.run_id,
                sr.vendor_id,
                v.name as vendor_name,
                sr.started_at,
                sr.completed_at,
                sr.status,
                sr.products_discovered,
                sr.products_processed,
                sr.products_skipped,
                sr.products_failed,
                sr.variants_new,
                sr.variants_updated,
                sr.variants_unchanged,
                sr.variants_stale,
                sr.variants_reactivated,
                sr.price_alerts,
                sr.stock_alerts,
                sr.data_quality_alerts,
                sr.is_full_scrape,
                sr.max_products_limit
            FROM scraperuns sr
            LEFT JOIN vendors v ON sr.vendor_id = v.vendor_id
            WHERE sr.run_id = %s
        """
        cursor.execute(query, (run_id,))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Scrape run {run_id} not found"
            )

        return ScrapeRun(**row_to_dict(row))


@router.get("/{run_id}/alerts", response_model=RunAlertsResponse)
def get_run_alerts(run_id: int):
    """
    Get alerts for a specific scrape run.

    Args:
        run_id: The ID of the scrape run

    Returns:
        List of alerts for the run, ordered by severity (critical first) then created_at

    Raises:
        HTTPException: If run not found
    """
    with db_pool.get_cursor() as cursor:
        # First verify the run exists
        cursor.execute("SELECT run_id FROM scraperuns WHERE run_id = %s", (run_id,))
        if not cursor.fetchone():
            raise HTTPException(
                status_code=404,
                detail=f"Scrape run {run_id} not found"
            )

        # Get alerts with product URL from scrapesources via vendoringredients
        query = """
            SELECT
                sa.alert_id,
                sa.run_id,
                sa.vendor_ingredient_id,
                sa.alert_type,
                sa.severity,
                sa.sku,
                sa.product_name,
                sa.old_value,
                sa.new_value,
                sa.change_percent,
                sa.message,
                sa.created_at,
                ss.product_url,
                iv.ingredient_id
            FROM scrapealerts sa
            LEFT JOIN vendoringredients vi ON sa.vendor_ingredient_id = vi.vendor_ingredient_id
            LEFT JOIN ingredientvariants iv ON vi.variant_id = iv.variant_id
            LEFT JOIN scrapesources ss ON vi.current_source_id = ss.source_id
            WHERE sa.run_id = %s
            ORDER BY
                CASE sa.severity
                    WHEN 'critical' THEN 1
                    WHEN 'warning' THEN 2
                    WHEN 'info' THEN 3
                    ELSE 4
                END,
                sa.created_at DESC
        """
        cursor.execute(query, (run_id,))
        rows = cursor.fetchall()

        alerts = [RunAlert(**row_to_dict(row)) for row in rows]

        return RunAlertsResponse(
            alerts=alerts,
            total=len(alerts),
        )
