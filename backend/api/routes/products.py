"""
Product routes for single-product updates.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import psycopg2.extras

from ..services.database import db_pool
from ..services.product_updater import update_single_product

router = APIRouter(prefix="/api/products", tags=["products"])


class UpdateProductRequest(BaseModel):
    """Request body for updating a single product."""
    vendor_ingredient_id: int


class UpdateProductResponse(BaseModel):
    """Response after updating a single product."""
    success: bool
    vendor_ingredient_id: int
    vendor_id: Optional[int] = None
    vendor_name: Optional[str] = None
    sku: Optional[str] = None
    old_values: Dict[str, Any] = {}
    new_values: Dict[str, Any] = {}
    changed_fields: Dict[str, Any] = {}
    message: str
    duration_ms: int
    error: Optional[str] = None


@router.post("/update-single", response_model=UpdateProductResponse)
def update_product(request: UpdateProductRequest):
    """
    Update a single product's price and inventory from its vendor source.

    This fetches fresh data from the vendor website and updates the database.
    Typically takes 5-30 seconds depending on the vendor.

    Args:
        request: Contains vendor_ingredient_id to identify the product

    Returns:
        Response with old/new values and what changed

    Raises:
        HTTPException: If product not found or update fails
    """
    try:
        with db_pool.get_connection() as conn:
            result = update_single_product(conn, request.vendor_ingredient_id)

        if not result['success']:
            return UpdateProductResponse(
                success=False,
                vendor_ingredient_id=request.vendor_ingredient_id,
                vendor_id=result.get('vendor_id'),
                vendor_name=result.get('vendor_name'),
                sku=result.get('sku'),
                old_values=result.get('old_values', {}),
                new_values=result.get('new_values', {}),
                changed_fields=result.get('changed_fields', {}),
                message=result.get('error', 'Update failed'),
                duration_ms=result.get('duration_ms', 0),
                error=result.get('error')
            )

        # Build success message
        changes = result.get('changed_fields', {})
        if changes:
            change_parts = []
            if 'price' in changes:
                old_p = changes['price']['old']
                new_p = changes['price']['new']
                change_parts.append(f"price: ${old_p:.2f} -> ${new_p:.2f}" if old_p and new_p else "price updated")
            if 'stock_status' in changes:
                change_parts.append(f"stock: {changes['stock_status']['old']} -> {changes['stock_status']['new']}")
            if 'warehouse_inventory' in changes:
                change_parts.append("inventory updated")
            message = f"Updated: {', '.join(change_parts)}"
        else:
            message = "No changes detected"

        return UpdateProductResponse(
            success=True,
            vendor_ingredient_id=request.vendor_ingredient_id,
            vendor_id=result['vendor_id'],
            vendor_name=result['vendor_name'],
            sku=result['sku'],
            old_values=result['old_values'],
            new_values=result['new_values'],
            changed_fields=result['changed_fields'],
            message=message,
            duration_ms=result['duration_ms']
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Update failed: {str(e)}"
        )


@router.get("/{vendor_ingredient_id}")
def get_product_info(vendor_ingredient_id: int):
    """
    Get basic product info for a vendor_ingredient_id.

    Used by frontend to verify product exists before triggering update.
    """
    try:
        with db_pool.get_cursor() as cursor:
            cursor.execute('''
                SELECT
                    vi.vendor_ingredient_id,
                    vi.vendor_id,
                    vi.sku,
                    vi.raw_product_name,
                    vi.last_seen_at,
                    v.name as vendor_name,
                    ss.product_url
                FROM vendoringredients vi
                JOIN vendors v ON vi.vendor_id = v.vendor_id
                LEFT JOIN scrapesources ss ON vi.current_source_id = ss.source_id
                WHERE vi.vendor_ingredient_id = %s
            ''', (vendor_ingredient_id,))

            row = cursor.fetchone()

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Product not found: {vendor_ingredient_id}"
                )

            return dict(row)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching product: {str(e)}"
        )
