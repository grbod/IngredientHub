"""
Product update service for single-product refreshes.
Provides vendor-specific functions to fetch and update individual products
from their source websites without running a full scrape.
"""
import os
import sys
import time
import re
import requests
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# Add backend directory to path for imports
BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)


# Vendor IDs
VENDOR_IO = 1
VENDOR_BS = 4
VENDOR_BN = 25
VENDOR_TP = 26


# HTTP Headers
BS_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'application/json',
}

BN_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'application/json',
}

TP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
}


def extract_handle_from_url(url: str, vendor_id: int) -> Optional[str]:
    """Extract product handle/slug from a vendor URL."""
    if not url:
        return None

    try:
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.split('/') if p]

        if vendor_id in [VENDOR_BS, VENDOR_BN]:
            # Shopify URLs: /products/handle
            if 'products' in path_parts:
                idx = path_parts.index('products')
                if idx + 1 < len(path_parts):
                    return path_parts[idx + 1]

        elif vendor_id == VENDOR_TP:
            # TrafaPharma: /products/slug or full path
            if 'products' in path_parts:
                idx = path_parts.index('products')
                if idx + 1 < len(path_parts):
                    return path_parts[idx + 1]
            return path_parts[-1] if path_parts else None

        elif vendor_id == VENDOR_IO:
            # IO URLs vary - return full path
            return '/'.join(path_parts)

        return path_parts[-1] if path_parts else None
    except Exception:
        return None


def get_product_info(cursor, vendor_ingredient_id: int) -> Optional[Dict]:
    """Get product info from database."""
    import psycopg2.extras

    cursor.execute('''
        SELECT
            vi.vendor_ingredient_id,
            vi.vendor_id,
            vi.sku,
            vi.raw_product_name,
            v.name as vendor_name,
            ss.product_url
        FROM vendoringredients vi
        JOIN vendors v ON vi.vendor_id = v.vendor_id
        LEFT JOIN scrapesources ss ON vi.current_source_id = ss.source_id
        WHERE vi.vendor_ingredient_id = %s
    ''', (vendor_ingredient_id,))

    row = cursor.fetchone()
    if not row:
        return None

    return dict(row)


def get_current_values(cursor, vendor_ingredient_id: int) -> Dict[str, Any]:
    """Get current price and stock values from database."""
    values = {
        'price': None,
        'price_per_kg': None,
        'stock_status': None,
        'quantity': None
    }

    # Get price
    cursor.execute('''
        SELECT price, price_per_kg FROM pricetiers
        WHERE vendor_ingredient_id = %s
        ORDER BY min_quantity ASC LIMIT 1
    ''', (vendor_ingredient_id,))
    price_row = cursor.fetchone()
    if price_row:
        values['price'] = price_row['price']
        values['price_per_kg'] = price_row['price_per_kg']

    # Get inventory (join through inventorylocations)
    cursor.execute('''
        SELECT il.stock_status, il.quantity_available
        FROM inventorylevels il
        JOIN inventorylocations iloc ON il.inventory_location_id = iloc.inventory_location_id
        WHERE iloc.vendor_ingredient_id = %s LIMIT 1
    ''', (vendor_ingredient_id,))
    inv_row = cursor.fetchone()
    if inv_row:
        values['stock_status'] = inv_row['stock_status']
        values['quantity'] = inv_row['quantity_available']

    return values


# =============================================================================
# BulkSupplements Update
# =============================================================================

def update_bs_product(conn, vendor_ingredient_id: int, handle: str) -> Dict[str, Any]:
    """
    Update a single BulkSupplements product.
    Fetches fresh data from Shopify JSON API and updates price/stock.
    """
    import psycopg2.extras

    url = f'https://www.bulksupplements.com/products/{handle}.json'

    try:
        response = requests.get(url, headers=BS_HEADERS, timeout=30)
        if response.status_code != 200:
            return {
                'success': False,
                'error': f'HTTP {response.status_code} fetching product',
                'old_values': {},
                'new_values': {},
                'changed_fields': {}
            }

        data = response.json()
        product = data.get('product', {})
        variants = product.get('variants', [])

    except Exception as e:
        return {
            'success': False,
            'error': f'Error fetching product: {str(e)}',
            'old_values': {},
            'new_values': {},
            'changed_fields': {}
        }

    if not variants:
        return {
            'success': False,
            'error': 'No variants found in product data',
            'old_values': {},
            'new_values': {},
            'changed_fields': {}
        }

    # Get current values before update
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    old_values = get_current_values(cursor, vendor_ingredient_id)

    # Get target SKU
    cursor.execute('SELECT sku FROM vendoringredients WHERE vendor_ingredient_id = %s',
                   (vendor_ingredient_id,))
    sku_row = cursor.fetchone()
    target_sku = sku_row['sku'] if sku_row else None

    # Find matching variant
    matched_variant = None
    for v in variants:
        if v.get('sku') == target_sku:
            matched_variant = v
            break
    if not matched_variant and variants:
        # Use first powder variant if no exact SKU match
        for v in variants:
            if v.get('option1', '').lower() == 'powder':
                matched_variant = v
                break
        if not matched_variant:
            matched_variant = variants[0]

    if matched_variant:
        new_price = float(matched_variant.get('price', 0))
        is_available = matched_variant.get('available', False)
        grams = matched_variant.get('grams', 0) or 0
        price_per_kg = (new_price / grams * 1000) if grams > 0 else None
        stock_status = 'in_stock' if is_available else 'out_of_stock'
        now_iso = datetime.now(timezone.utc).isoformat()

        # Update price tier
        cursor.execute('''
            UPDATE pricetiers SET price = %s, price_per_kg = %s, effective_date = %s
            WHERE vendor_ingredient_id = %s
        ''', (new_price, price_per_kg, now_iso, vendor_ingredient_id))

        # Update inventory (via inventorylocations join)
        cursor.execute('''
            UPDATE inventorylevels SET stock_status = %s, last_updated = %s
            WHERE inventory_location_id IN (
                SELECT inventory_location_id FROM inventorylocations
                WHERE vendor_ingredient_id = %s
            )
        ''', (stock_status, now_iso, vendor_ingredient_id))

        # Update last_seen_at
        cursor.execute('''
            UPDATE vendoringredients SET last_seen_at = %s WHERE vendor_ingredient_id = %s
        ''', (now_iso, vendor_ingredient_id))

        conn.commit()

    # Get new values after update
    new_values = get_current_values(cursor, vendor_ingredient_id)
    cursor.close()

    # Calculate changes
    changed_fields = {}
    for key in ['price', 'price_per_kg', 'stock_status']:
        if old_values.get(key) != new_values.get(key):
            changed_fields[key] = {'old': old_values.get(key), 'new': new_values.get(key)}

    return {
        'success': True,
        'old_values': old_values,
        'new_values': new_values,
        'changed_fields': changed_fields
    }


# =============================================================================
# BoxNutra Update
# =============================================================================

def update_bn_product(conn, vendor_ingredient_id: int, handle: str) -> Dict[str, Any]:
    """
    Update a single BoxNutra product.
    Same as BS - Shopify JSON API.
    """
    import psycopg2.extras

    url = f'https://www.boxnutra.com/products/{handle}.json'

    try:
        response = requests.get(url, headers=BN_HEADERS, timeout=30)
        if response.status_code != 200:
            return {
                'success': False,
                'error': f'HTTP {response.status_code} fetching product',
                'old_values': {},
                'new_values': {},
                'changed_fields': {}
            }

        data = response.json()
        product = data.get('product', {})
        variants = product.get('variants', [])

    except Exception as e:
        return {
            'success': False,
            'error': f'Error fetching product: {str(e)}',
            'old_values': {},
            'new_values': {},
            'changed_fields': {}
        }

    if not variants:
        return {
            'success': False,
            'error': 'No variants found in product data',
            'old_values': {},
            'new_values': {},
            'changed_fields': {}
        }

    # Get current values before update
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    old_values = get_current_values(cursor, vendor_ingredient_id)

    # Get target SKU
    cursor.execute('SELECT sku FROM vendoringredients WHERE vendor_ingredient_id = %s',
                   (vendor_ingredient_id,))
    sku_row = cursor.fetchone()
    target_sku = sku_row['sku'] if sku_row else None

    # Find matching variant
    matched_variant = None
    for v in variants:
        if v.get('sku') == target_sku:
            matched_variant = v
            break
    if not matched_variant and variants:
        matched_variant = variants[0]

    if matched_variant:
        new_price = float(matched_variant.get('price', 0))
        is_available = matched_variant.get('available', False)
        grams = matched_variant.get('grams', 0) or 0
        price_per_kg = (new_price / grams * 1000) if grams > 0 else None
        stock_status = 'in_stock' if is_available else 'out_of_stock'
        now_iso = datetime.now(timezone.utc).isoformat()

        # Update price tier
        cursor.execute('''
            UPDATE pricetiers SET price = %s, price_per_kg = %s, effective_date = %s
            WHERE vendor_ingredient_id = %s
        ''', (new_price, price_per_kg, now_iso, vendor_ingredient_id))

        # Update inventory (via inventorylocations join)
        cursor.execute('''
            UPDATE inventorylevels SET stock_status = %s, last_updated = %s
            WHERE inventory_location_id IN (
                SELECT inventory_location_id FROM inventorylocations
                WHERE vendor_ingredient_id = %s
            )
        ''', (stock_status, now_iso, vendor_ingredient_id))

        # Update last_seen_at
        cursor.execute('''
            UPDATE vendoringredients SET last_seen_at = %s WHERE vendor_ingredient_id = %s
        ''', (now_iso, vendor_ingredient_id))

        conn.commit()

    # Get new values after update
    new_values = get_current_values(cursor, vendor_ingredient_id)
    cursor.close()

    # Calculate changes
    changed_fields = {}
    for key in ['price', 'price_per_kg', 'stock_status']:
        if old_values.get(key) != new_values.get(key):
            changed_fields[key] = {'old': old_values.get(key), 'new': new_values.get(key)}

    return {
        'success': True,
        'old_values': old_values,
        'new_values': new_values,
        'changed_fields': changed_fields
    }


# =============================================================================
# TrafaPharma Update
# =============================================================================

def update_tp_product(conn, vendor_ingredient_id: int, slug: str) -> Dict[str, Any]:
    """
    Update a single TrafaPharma product.
    Scrapes HTML page and extracts price/stock.
    """
    import psycopg2.extras

    # Get current product URL from database
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute('''
        SELECT ss.product_url FROM vendoringredients vi
        LEFT JOIN scrapesources ss ON vi.current_source_id = ss.source_id
        WHERE vi.vendor_ingredient_id = %s
    ''', (vendor_ingredient_id,))
    url_row = cursor.fetchone()
    url = url_row['product_url'] if url_row else f'https://trafapharma.com/products/{slug}'

    # Get current values before update
    old_values = get_current_values(cursor, vendor_ingredient_id)

    try:
        response = requests.get(url, headers=TP_HEADERS, timeout=30)
        if response.status_code != 200:
            return {
                'success': False,
                'error': f'HTTP {response.status_code} fetching product',
                'old_values': old_values,
                'new_values': {},
                'changed_fields': {}
            }

        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract price from page
        price_elem = soup.find('span', class_='price') or soup.find('div', class_='price')
        new_price = None
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            price_match = re.search(r'\$?([\d,]+\.?\d*)', price_text)
            if price_match:
                new_price = float(price_match.group(1).replace(',', ''))

        # Check for "Inquire" or out of stock indicators
        page_text = response.text.lower()
        if 'inquire' in page_text or 'out of stock' in page_text:
            stock_status = 'inquire' if 'inquire' in page_text else 'out_of_stock'
        else:
            stock_status = 'in_stock' if new_price else 'inquire'

        now_iso = datetime.now(timezone.utc).isoformat()

        if new_price:
            # Get packaging size to calculate price_per_kg
            cursor.execute('''
                SELECT size_kg FROM packagingsizes WHERE vendor_ingredient_id = %s LIMIT 1
            ''', (vendor_ingredient_id,))
            pkg_row = cursor.fetchone()
            size_kg = pkg_row['size_kg'] if pkg_row else None
            price_per_kg = (new_price / size_kg) if size_kg and size_kg > 0 else None

            # Update price tier
            cursor.execute('''
                UPDATE pricetiers SET price = %s, price_per_kg = %s, effective_date = %s
                WHERE vendor_ingredient_id = %s
            ''', (new_price, price_per_kg, now_iso, vendor_ingredient_id))

        # Update inventory (via inventorylocations join)
        cursor.execute('''
            UPDATE inventorylevels SET stock_status = %s, last_updated = %s
            WHERE inventory_location_id IN (
                SELECT inventory_location_id FROM inventorylocations
                WHERE vendor_ingredient_id = %s
            )
        ''', (stock_status, now_iso, vendor_ingredient_id))

        # Update last_seen_at
        cursor.execute('''
            UPDATE vendoringredients SET last_seen_at = %s WHERE vendor_ingredient_id = %s
        ''', (now_iso, vendor_ingredient_id))

        conn.commit()

    except Exception as e:
        cursor.close()
        return {
            'success': False,
            'error': f'Error fetching product: {str(e)}',
            'old_values': old_values,
            'new_values': {},
            'changed_fields': {}
        }

    # Get new values after update
    new_values = get_current_values(cursor, vendor_ingredient_id)
    cursor.close()

    # Calculate changes
    changed_fields = {}
    for key in ['price', 'price_per_kg', 'stock_status']:
        if old_values.get(key) != new_values.get(key):
            changed_fields[key] = {'old': old_values.get(key), 'new': new_values.get(key)}

    return {
        'success': True,
        'old_values': old_values,
        'new_values': new_values,
        'changed_fields': changed_fields
    }


# =============================================================================
# IngredientsOnline Update
# =============================================================================

def update_io_product(conn, vendor_ingredient_id: int, sku: str) -> Dict[str, Any]:
    """
    Update a single IngredientsOnline product.
    Note: IO requires authentication - returns not implemented for now.
    """
    return {
        'success': False,
        'error': 'IngredientsOnline single-product update not yet implemented (requires auth)',
        'old_values': {},
        'new_values': {},
        'changed_fields': {}
    }


# =============================================================================
# Main Update Function
# =============================================================================

def update_single_product(conn, vendor_ingredient_id: int) -> Dict[str, Any]:
    """
    Update a single product from its vendor source.

    Args:
        conn: Database connection
        vendor_ingredient_id: The ID of the product to update

    Returns:
        dict with keys: {
            'success': bool,
            'vendor_id': int or None,
            'vendor_name': str or None,
            'sku': str or None,
            'old_values': dict,
            'new_values': dict,
            'changed_fields': dict,
            'error': str or None,
            'duration_ms': int
        }
    """
    import psycopg2.extras

    start_time = time.time()

    # Get product info
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    product_info = get_product_info(cursor, vendor_ingredient_id)
    cursor.close()

    if not product_info:
        return {
            'success': False,
            'vendor_id': None,
            'vendor_name': None,
            'sku': None,
            'error': f'Product not found: vendor_ingredient_id={vendor_ingredient_id}',
            'duration_ms': int((time.time() - start_time) * 1000)
        }

    vendor_id = product_info['vendor_id']
    vendor_name = product_info['vendor_name']
    sku = product_info['sku']
    product_url = product_info.get('product_url')

    # Extract handle/slug from URL
    handle = extract_handle_from_url(product_url, vendor_id)

    if not handle and vendor_id != VENDOR_IO:
        return {
            'success': False,
            'vendor_id': vendor_id,
            'vendor_name': vendor_name,
            'sku': sku,
            'error': f'Could not extract product handle from URL: {product_url}',
            'duration_ms': int((time.time() - start_time) * 1000)
        }

    # Route to vendor-specific update function
    if vendor_id == VENDOR_BS:
        result = update_bs_product(conn, vendor_ingredient_id, handle)
    elif vendor_id == VENDOR_BN:
        result = update_bn_product(conn, vendor_ingredient_id, handle)
    elif vendor_id == VENDOR_TP:
        result = update_tp_product(conn, vendor_ingredient_id, handle)
    elif vendor_id == VENDOR_IO:
        result = update_io_product(conn, vendor_ingredient_id, sku)
    else:
        result = {
            'success': False,
            'error': f'Unsupported vendor: {vendor_name} (id={vendor_id})',
            'old_values': {},
            'new_values': {},
            'changed_fields': {}
        }

    duration_ms = int((time.time() - start_time) * 1000)

    return {
        'success': result.get('success', False),
        'vendor_id': vendor_id,
        'vendor_name': vendor_name,
        'sku': sku,
        'old_values': result.get('old_values', {}),
        'new_values': result.get('new_values', {}),
        'changed_fields': result.get('changed_fields', {}),
        'error': result.get('error'),
        'duration_ms': duration_ms
    }
