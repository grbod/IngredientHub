"""
Alert management routes.

Endpoints for viewing and filtering alerts across all scrape runs.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from ..services.database import db_pool


router = APIRouter(prefix="/api/alerts", tags=["alerts"])


class Alert(BaseModel):
    """Alert with full context including ingredient and vendor info."""
    alert_id: int
    run_id: int
    vendor_id: Optional[int] = None
    vendor_name: Optional[str] = None
    vendor_ingredient_id: Optional[int] = None
    ingredient_id: Optional[int] = None
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


class AlertListResponse(BaseModel):
    """Response for list of alerts."""
    alerts: List[Alert]
    total: int
    limit: int
    offset: int


class AlertTypeSummary(BaseModel):
    """Summary counts for a specific alert type."""
    alert_type: str
    count: int


class VendorAlertSummary(BaseModel):
    """Alert summary for a specific vendor."""
    vendor_id: int
    vendor_name: str
    total_alerts: int
    by_type: List[AlertTypeSummary]
    by_severity: Dict[str, int]


class AlertSummaryResponse(BaseModel):
    """Response for alert summary across all vendors."""
    period_days: int
    total_alerts: int
    by_vendor: List[VendorAlertSummary]
    by_type: List[AlertTypeSummary]
    by_severity: Dict[str, int]


def row_to_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a database row to a dictionary."""
    return dict(row)


@router.get("/", response_model=AlertListResponse)
def list_alerts(
    vendor_id: Optional[int] = Query(None, description="Filter by vendor ID"),
    alert_types: Optional[List[str]] = Query(
        None,
        description="Filter by alert types (e.g., price_increase_major, stock_out)"
    ),
    severity: Optional[str] = Query(
        None,
        description="Filter by severity (info, warning, critical)"
    ),
    limit: int = Query(50, ge=1, le=200, description="Number of results to return"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
):
    """
    List alerts with optional filters.

    Args:
        vendor_id: Optional vendor ID to filter by
        alert_types: Optional list of alert types to filter by
        severity: Optional severity level to filter by
        limit: Maximum number of alerts to return (default 50, max 200)
        offset: Number of alerts to skip for pagination

    Returns:
        List of alerts with full context including ingredient_id for frontend linking
    """
    with db_pool.get_cursor() as cursor:
        # Build WHERE clause
        conditions = []
        params: List[Any] = []

        if vendor_id is not None:
            conditions.append("sr.vendor_id = %s")
            params.append(vendor_id)

        if alert_types:
            placeholders = ", ".join(["%s"] * len(alert_types))
            conditions.append(f"sa.alert_type IN ({placeholders})")
            params.extend(alert_types)

        if severity:
            conditions.append("sa.severity = %s")
            params.append(severity)

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # Get total count
        count_query = f"""
            SELECT COUNT(*) as total
            FROM scrapealerts sa
            JOIN scraperuns sr ON sa.run_id = sr.run_id
            {where_clause}
        """
        cursor.execute(count_query, params)
        total = cursor.fetchone()["total"]

        # Get alerts with full context
        # Join through vendoringredients -> ingredientvariants to get ingredient_id
        query = f"""
            SELECT
                sa.alert_id,
                sa.run_id,
                sr.vendor_id,
                v.name as vendor_name,
                sa.vendor_ingredient_id,
                iv.ingredient_id,
                sa.alert_type,
                sa.severity,
                sa.sku,
                sa.product_name,
                sa.old_value,
                sa.new_value,
                sa.change_percent,
                sa.message,
                sa.created_at,
                ss.product_url
            FROM scrapealerts sa
            JOIN scraperuns sr ON sa.run_id = sr.run_id
            LEFT JOIN vendors v ON sr.vendor_id = v.vendor_id
            LEFT JOIN vendoringredients vi ON sa.vendor_ingredient_id = vi.vendor_ingredient_id
            LEFT JOIN ingredientvariants iv ON vi.variant_id = iv.variant_id
            LEFT JOIN scrapesources ss ON vi.current_source_id = ss.source_id
            {where_clause}
            ORDER BY
                CASE sa.severity
                    WHEN 'critical' THEN 1
                    WHEN 'warning' THEN 2
                    WHEN 'info' THEN 3
                    ELSE 4
                END,
                sa.created_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        cursor.execute(query, params)
        rows = cursor.fetchall()

        alerts = [Alert(**row_to_dict(row)) for row in rows]

        return AlertListResponse(
            alerts=alerts,
            total=total,
            limit=limit,
            offset=offset,
        )


@router.get("/summary", response_model=AlertSummaryResponse)
def get_alert_summary(
    days: int = Query(7, ge=1, le=30, description="Number of days to look back"),
):
    """
    Get alert summary statistics for the specified period.

    Args:
        days: Number of days to look back (default 7, max 30)

    Returns:
        Summary of alerts grouped by vendor, type, and severity
    """
    with db_pool.get_cursor() as cursor:
        # Get total count for period
        total_query = """
            SELECT COUNT(*) as total
            FROM scrapealerts sa
            WHERE sa.created_at >= NOW() - INTERVAL '%s days'
        """
        cursor.execute(total_query, (days,))
        total_alerts = cursor.fetchone()["total"]

        # Get counts by alert type
        type_query = """
            SELECT
                sa.alert_type,
                COUNT(*) as count
            FROM scrapealerts sa
            WHERE sa.created_at >= NOW() - INTERVAL '%s days'
            GROUP BY sa.alert_type
            ORDER BY count DESC
        """
        cursor.execute(type_query, (days,))
        by_type = [
            AlertTypeSummary(alert_type=row["alert_type"], count=row["count"])
            for row in cursor.fetchall()
        ]

        # Get counts by severity
        severity_query = """
            SELECT
                sa.severity,
                COUNT(*) as count
            FROM scrapealerts sa
            WHERE sa.created_at >= NOW() - INTERVAL '%s days'
            GROUP BY sa.severity
        """
        cursor.execute(severity_query, (days,))
        by_severity = {row["severity"]: row["count"] for row in cursor.fetchall()}

        # Get counts by vendor with breakdowns
        vendor_query = """
            SELECT
                sr.vendor_id,
                v.name as vendor_name,
                sa.alert_type,
                sa.severity,
                COUNT(*) as count
            FROM scrapealerts sa
            JOIN scraperuns sr ON sa.run_id = sr.run_id
            LEFT JOIN vendors v ON sr.vendor_id = v.vendor_id
            WHERE sa.created_at >= NOW() - INTERVAL '%s days'
            GROUP BY sr.vendor_id, v.name, sa.alert_type, sa.severity
            ORDER BY sr.vendor_id, count DESC
        """
        cursor.execute(vendor_query, (days,))
        vendor_rows = cursor.fetchall()

        # Aggregate vendor data
        vendor_data: Dict[int, Dict[str, Any]] = {}
        for row in vendor_rows:
            vid = row["vendor_id"]
            if vid not in vendor_data:
                vendor_data[vid] = {
                    "vendor_id": vid,
                    "vendor_name": row["vendor_name"] or f"Vendor {vid}",
                    "total_alerts": 0,
                    "by_type": {},
                    "by_severity": {},
                }

            count = row["count"]
            vendor_data[vid]["total_alerts"] += count

            # Aggregate by type
            alert_type = row["alert_type"]
            if alert_type not in vendor_data[vid]["by_type"]:
                vendor_data[vid]["by_type"][alert_type] = 0
            vendor_data[vid]["by_type"][alert_type] += count

            # Aggregate by severity
            sev = row["severity"]
            if sev not in vendor_data[vid]["by_severity"]:
                vendor_data[vid]["by_severity"][sev] = 0
            vendor_data[vid]["by_severity"][sev] += count

        # Convert to response format
        by_vendor = [
            VendorAlertSummary(
                vendor_id=data["vendor_id"],
                vendor_name=data["vendor_name"],
                total_alerts=data["total_alerts"],
                by_type=[
                    AlertTypeSummary(alert_type=t, count=c)
                    for t, c in data["by_type"].items()
                ],
                by_severity=data["by_severity"],
            )
            for data in sorted(
                vendor_data.values(),
                key=lambda x: x["total_alerts"],
                reverse=True
            )
        ]

        return AlertSummaryResponse(
            period_days=days,
            total_alerts=total_alerts,
            by_vendor=by_vendor,
            by_type=by_type,
            by_severity=by_severity,
        )
