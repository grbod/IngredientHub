"""
Data routes for frontend consumption.

Replaces direct Supabase JS SDK queries with server-side SQL.
"""

from typing import Optional

from fastapi import APIRouter, Depends, Query

from ..services.database import get_db

router = APIRouter(prefix="/api", tags=["data"])


# ============================================================================
# GET /api/ingredients
# ============================================================================

@router.get("/ingredients")
def get_ingredients(
    search: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db=Depends(get_db),
):
    """
    Paginated ingredient list with category names, vendor list, and stock status.
    Replaces useIngredients + useIngredientCount hooks.
    """
    with db() as cursor:
        # Build WHERE clause
        where = ""
        params = []
        if search:
            where = "WHERE i.name ILIKE %s"
            params.append(f"%{search}%")

        # Get total count
        cursor.execute(
            f"SELECT COUNT(*) AS total FROM ingredients i {where}",
            params,
        )
        total = cursor.fetchone()["total"]

        # Get paginated ingredients with category name, vendor info, and stock
        cursor.execute(
            f"""
            SELECT
                i.ingredient_id,
                i.name,
                i.category_id,
                c.name AS category_name,
                i.status,
                COALESCE(agg.vendors, '{{}}') AS vendors,
                COALESCE(agg.vendor_count, 0) AS vendor_count,
                COALESCE(agg.stock_status, 'unknown') AS stock_status
            FROM ingredients i
            LEFT JOIN categories c ON c.category_id = i.category_id
            LEFT JOIN LATERAL (
                SELECT
                    ARRAY_AGG(DISTINCT v.name ORDER BY v.name) AS vendors,
                    COUNT(DISTINCT v.vendor_id) AS vendor_count,
                    CASE
                        WHEN BOOL_OR(
                            COALESCE(vi_inv.stock_status, '') = 'in_stock'
                            OR COALESCE(il_stock.has_stock, false)
                        ) THEN 'in_stock'
                        WHEN BOOL_OR(
                            COALESCE(vi_inv.stock_status, '') = 'out_of_stock'
                            OR COALESCE(il_stock.has_stock, false) = false
                        ) THEN 'out_of_stock'
                        ELSE 'unknown'
                    END AS stock_status
                FROM ingredientvariants iv
                JOIN vendoringredients vi ON vi.variant_id = iv.variant_id
                JOIN vendors v ON v.vendor_id = vi.vendor_id
                LEFT JOIN vendorinventory vi_inv ON vi_inv.vendor_ingredient_id = vi.vendor_ingredient_id
                LEFT JOIN LATERAL (
                    SELECT BOOL_OR(
                        invl.stock_status = 'in_stock' OR COALESCE(invl.quantity_available, 0) > 0
                    ) AS has_stock
                    FROM inventorylocations iloc
                    JOIN inventorylevels invl ON invl.inventory_location_id = iloc.inventory_location_id
                    WHERE iloc.vendor_ingredient_id = vi.vendor_ingredient_id
                ) il_stock ON true
                WHERE iv.ingredient_id = i.ingredient_id
            ) agg ON true
            {where}
            ORDER BY i.name
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
        rows = cursor.fetchall()

        data = []
        for r in rows:
            vendors = r["vendors"] if r["vendors"] else []
            # PostgreSQL returns array, ensure it's a list
            if isinstance(vendors, str):
                vendors = [v.strip() for v in vendors.strip("{}").split(",") if v.strip()]
            data.append({
                "ingredient_id": r["ingredient_id"],
                "name": r["name"],
                "category_id": r["category_id"],
                "category_name": r["category_name"],
                "status": r["status"],
                "vendors": vendors,
                "vendor_count": r["vendor_count"],
                "stock_status": r["stock_status"],
            })

        return {"data": data, "total": total}


# ============================================================================
# GET /api/ingredients/{ingredient_id}
# ============================================================================

@router.get("/ingredients/{ingredient_id}")
def get_ingredient_detail(ingredient_id: int, db=Depends(get_db)):
    """
    Full ingredient detail: price tiers, warehouse inventory, simple inventory.
    Replaces useIngredientDetail hook.
    """
    with db() as cursor:
        # Get ingredient + category
        cursor.execute(
            """
            SELECT i.ingredient_id, i.name, c.name AS category_name
            FROM ingredients i
            LEFT JOIN categories c ON c.category_id = i.category_id
            WHERE i.ingredient_id = %s
            """,
            [ingredient_id],
        )
        ingredient = cursor.fetchone()
        if not ingredient:
            return None

        # Get price tiers with vendor info, packaging, and product URLs
        cursor.execute(
            """
            SELECT
                pt.vendor_ingredient_id,
                vi.vendor_id,
                v.name AS vendor_name,
                vi.sku,
                ps.description AS packaging,
                COALESCE(ps.quantity, 0) AS pack_size,
                pt.min_quantity,
                pt.price,
                pt.price_per_kg,
                ss.product_url,
                vi.last_seen_at
            FROM ingredientvariants iv
            JOIN vendoringredients vi ON vi.variant_id = iv.variant_id
                AND (vi.status = 'active' OR vi.status IS NULL)
            JOIN vendors v ON v.vendor_id = vi.vendor_id
            JOIN pricetiers pt ON pt.vendor_ingredient_id = vi.vendor_ingredient_id
            LEFT JOIN packagingsizes ps ON ps.vendor_ingredient_id = vi.vendor_ingredient_id
            LEFT JOIN scrapesources ss ON ss.source_id = vi.current_source_id
            WHERE iv.ingredient_id = %s
            ORDER BY v.name, pt.min_quantity
            """,
            [ingredient_id],
        )
        price_tiers = [
            {
                "vendor_ingredient_id": r["vendor_ingredient_id"],
                "vendor_id": r["vendor_id"],
                "vendor_name": r["vendor_name"],
                "sku": r["sku"],
                "packaging": r["packaging"],
                "pack_size": r["pack_size"],
                "min_quantity": r["min_quantity"],
                "price": r["price"],
                "price_per_kg": r["price_per_kg"],
                "product_url": r["product_url"],
                "last_seen_at": r["last_seen_at"],
            }
            for r in cursor.fetchall()
        ]

        # Get warehouse inventory (IO - multi-warehouse)
        cursor.execute(
            """
            SELECT
                iloc.vendor_ingredient_id,
                v.name AS vendor_name,
                vi.sku,
                loc.name AS warehouse,
                invl.quantity_available,
                invl.stock_status
            FROM ingredientvariants iv
            JOIN vendoringredients vi ON vi.variant_id = iv.variant_id
                AND (vi.status = 'active' OR vi.status IS NULL)
            JOIN vendors v ON v.vendor_id = vi.vendor_id
            JOIN inventorylocations iloc ON iloc.vendor_ingredient_id = vi.vendor_ingredient_id
            JOIN inventorylevels invl ON invl.inventory_location_id = iloc.inventory_location_id
            JOIN locations loc ON loc.location_id = iloc.location_id
            WHERE iv.ingredient_id = %s
            ORDER BY v.name, loc.name
            """,
            [ingredient_id],
        )
        warehouse_inventory = [
            {
                "vendor_ingredient_id": r["vendor_ingredient_id"],
                "vendor_name": r["vendor_name"],
                "sku": r["sku"],
                "warehouse": r["warehouse"],
                "quantity_available": r["quantity_available"],
                "stock_status": r["stock_status"],
            }
            for r in cursor.fetchall()
        ]

        # Get simple inventory (BS/BN/TP - in stock / out of stock)
        cursor.execute(
            """
            SELECT
                vinv.vendor_ingredient_id,
                v.name AS vendor_name,
                vi.sku,
                vinv.stock_status
            FROM ingredientvariants iv
            JOIN vendoringredients vi ON vi.variant_id = iv.variant_id
                AND (vi.status = 'active' OR vi.status IS NULL)
            JOIN vendors v ON v.vendor_id = vi.vendor_id
            JOIN vendorinventory vinv ON vinv.vendor_ingredient_id = vi.vendor_ingredient_id
            WHERE iv.ingredient_id = %s
            ORDER BY v.name
            """,
            [ingredient_id],
        )
        simple_inventory = [
            {
                "vendor_ingredient_id": r["vendor_ingredient_id"],
                "vendor_name": r["vendor_name"],
                "sku": r["sku"],
                "stock_status": r["stock_status"],
            }
            for r in cursor.fetchall()
        ]

        return {
            "ingredient_id": ingredient["ingredient_id"],
            "name": ingredient["name"],
            "category_name": ingredient["category_name"],
            "priceTiers": price_tiers,
            "warehouseInventory": warehouse_inventory,
            "simpleInventory": simple_inventory,
        }


# ============================================================================
# GET /api/price-comparison
# ============================================================================

@router.get("/price-comparison")
def get_price_comparison(search: Optional[str] = None, db=Depends(get_db)):
    """
    Cross-vendor price comparison with best price per kg.
    Replaces usePriceComparison hook.
    """
    with db() as cursor:
        where = ""
        params = []
        if search:
            where = "AND i.name ILIKE %s"
            params.append(f"%{search}%")

        cursor.execute(
            f"""
            SELECT
                i.ingredient_id,
                i.name AS ingredient_name,
                vi.vendor_id,
                v.name AS vendor_name,
                vi.sku,
                vi.raw_product_name,
                vi.last_seen_at,
                MIN(pt.min_quantity) AS min_order_qty,
                MIN(pt.price_per_kg) FILTER (WHERE pt.price_per_kg IS NOT NULL) AS best_price_per_kg
            FROM ingredients i
            JOIN ingredientvariants iv ON iv.ingredient_id = i.ingredient_id
            JOIN vendoringredients vi ON vi.variant_id = iv.variant_id
                AND (vi.status = 'active' OR vi.status IS NULL)
            JOIN vendors v ON v.vendor_id = vi.vendor_id
            LEFT JOIN pricetiers pt ON pt.vendor_ingredient_id = vi.vendor_ingredient_id
            WHERE 1=1 {where}
            GROUP BY i.ingredient_id, i.name, vi.vendor_id, v.name, vi.sku, vi.raw_product_name, vi.last_seen_at,
                     vi.vendor_ingredient_id
            ORDER BY i.name, best_price_per_kg NULLS LAST
            LIMIT 2500
            """,
            params,
        )
        rows = cursor.fetchall()

        # Group by ingredient, then pick best vendor offering per vendor_id
        ingredients = {}
        for r in rows:
            ing_id = r["ingredient_id"]
            if ing_id not in ingredients:
                ingredients[ing_id] = {
                    "ingredient_id": ing_id,
                    "ingredient_name": r["ingredient_name"],
                    "vendors_map": {},
                }

            vid = r["vendor_id"]
            existing = ingredients[ing_id]["vendors_map"].get(vid)
            best_price = r["best_price_per_kg"]

            if not existing or (
                best_price is not None
                and (existing["best_price_per_kg"] is None or best_price < existing["best_price_per_kg"])
            ):
                ingredients[ing_id]["vendors_map"][vid] = {
                    "vendor_id": vid,
                    "vendor_name": r["vendor_name"],
                    "sku": r["sku"],
                    "product_name": r["raw_product_name"],
                    "best_price_per_kg": float(best_price) if best_price is not None else None,
                    "min_order_qty": float(r["min_order_qty"]) if r["min_order_qty"] is not None else None,
                    "last_seen": r["last_seen_at"],
                }

        result = []
        for ing in ingredients.values():
            vendors = sorted(
                ing["vendors_map"].values(),
                key=lambda v: (v["best_price_per_kg"] is None, v["best_price_per_kg"] or 0),
            )
            if vendors:
                result.append({
                    "ingredient_id": ing["ingredient_id"],
                    "ingredient_name": ing["ingredient_name"],
                    "vendors": vendors,
                })

        result.sort(key=lambda x: x["ingredient_name"])
        return result


# ============================================================================
# GET /api/vendor-ingredients
# ============================================================================

@router.get("/vendor-ingredients")
def get_vendor_ingredients(
    vendor_id: Optional[int] = None,
    search: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db=Depends(get_db),
):
    """
    Paginated vendor ingredient list with vendor names.
    Replaces useProducts + useProductCount hooks.
    """
    with db() as cursor:
        conditions = []
        params = []
        if vendor_id:
            conditions.append("vi.vendor_id = %s")
            params.append(vendor_id)
        if search:
            conditions.append("(vi.raw_product_name ILIKE %s OR vi.sku ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Total count
        cursor.execute(
            f"SELECT COUNT(*) AS total FROM vendoringredients vi {where}",
            params,
        )
        total = cursor.fetchone()["total"]

        # Paginated data
        cursor.execute(
            f"""
            SELECT
                vi.vendor_ingredient_id,
                vi.sku,
                vi.raw_product_name,
                vi.status,
                vi.last_seen_at,
                vi.vendor_id,
                v.name AS vendor_name
            FROM vendoringredients vi
            JOIN vendors v ON v.vendor_id = vi.vendor_id
            {where}
            ORDER BY vi.last_seen_at DESC NULLS LAST
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
        data = [dict(r) for r in cursor.fetchall()]

        return {"data": data, "total": total}


# ============================================================================
# GET /api/vendors
# ============================================================================

@router.get("/vendors")
def get_vendors(db=Depends(get_db)):
    """List of all vendors. Replaces useVendors hook."""
    with db() as cursor:
        cursor.execute("SELECT * FROM vendors ORDER BY name")
        return [dict(r) for r in cursor.fetchall()]


# ============================================================================
# GET /api/vendors/stats
# ============================================================================

@router.get("/vendors/stats")
def get_vendor_stats(db=Depends(get_db)):
    """
    Per-vendor product counts, unique ingredients, and last scraped time.
    Replaces useVendorStats hook.
    """
    with db() as cursor:
        cursor.execute(
            """
            SELECT
                v.vendor_id,
                v.name,
                v.pricing_model,
                v.status,
                COUNT(DISTINCT vi.variant_id) AS "productCount",
                COUNT(vi.vendor_ingredient_id) AS "variantCount",
                MAX(ss.scraped_at) AS "lastScraped"
            FROM vendors v
            LEFT JOIN vendoringredients vi ON vi.vendor_id = v.vendor_id
            LEFT JOIN scrapesources ss ON ss.vendor_id = v.vendor_id
            GROUP BY v.vendor_id, v.name, v.pricing_model, v.status
            ORDER BY v.name
            """
        )
        return [dict(r) for r in cursor.fetchall()]


# ============================================================================
# GET /api/categories
# ============================================================================

@router.get("/categories")
def get_categories(db=Depends(get_db)):
    """List of all categories. Replaces useCategories hook."""
    with db() as cursor:
        cursor.execute(
            "SELECT category_id, name, description FROM categories ORDER BY name"
        )
        return [dict(r) for r in cursor.fetchall()]
