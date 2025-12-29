#!/usr/bin/env python3
"""
TrafaPharma.com Product Scraper

Scrapes all products from TrafaPharma.com including size variants and pricing.
Uses HTML parsing (no API available - custom PHP site with server-side rendering).
Output is saved to a timestamped CSV file with checkpoint support.

Site Details:
- Platform: Custom PHP (likely CodeIgniter)
- Total Products: ~663
- Pricing: Per-size (requires POST to fetch each size variant's price)
- No authentication required
- No inventory/stock data available
"""

import os
import sys
import json
import time
import re
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Database support - PostgreSQL (Supabase) or SQLite fallback
import sqlite3  # Always available for fallback/reconnect
try:
    import psycopg2
    import psycopg2.extras
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False


# =============================================================================
# Configuration
# =============================================================================

BASE_URL = "https://trafapharma.com"
PRODUCTS_AJAX_URL = f"{BASE_URL}/products/index/pg/"

# Rate limiting
REQUEST_DELAY = 0.5  # Seconds between requests

# Retry configuration (exponential backoff)
MAX_RETRIES = 7
INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 60

# Checkpoint configuration
CHECKPOINT_INTERVAL = 25
CHECKPOINT_FILE = ".trafapharma_checkpoint.json"

# Request headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
}

# AJAX headers (for pagination requests)
AJAX_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'X-Requested-With': 'XMLHttpRequest',
}

# Pagination settings
PRODUCTS_PER_PAGE = 12

# Database settings (shared with IO/BS scrapers)
DATABASE_FILE = "ingredients.db"  # SQLite fallback
USE_POSTGRES = True  # Set to False to force SQLite

# TrafaPharma Business Model Constants
TRAFA_BUSINESS_MODEL = {
    'order_rule_type': 'fixed_pack',     # Fixed price per package/size
    'shipping_responsibility': 'buyer',   # Shipping not included
    'min_order_qty': 1,                   # Can order single units
}


# =============================================================================
# Database Connection Wrapper (auto-reconnect)
# =============================================================================

class DatabaseConnection:
    """
    Wrapper for database connection that handles automatic reconnection.
    Detects closed connections and reconnects transparently.
    """

    def __init__(self, db_path: str = None):
        self.db_path = db_path or DATABASE_FILE
        self.postgres_url = None
        self._conn = None
        self._is_postgres = False

    def connect(self):
        """Establish database connection."""
        self.postgres_url = get_postgres_url()
        if USE_POSTGRES and HAS_POSTGRES and self.postgres_url:
            self._conn = init_postgres_database(self.postgres_url)
            self._is_postgres = True
        else:
            self._conn = init_sqlite_database(self.db_path)
            self._is_postgres = False
        return self._conn

    def reconnect(self):
        """Reconnect to database after connection loss."""
        print("  Reconnecting to database...", flush=True)
        try:
            if self._conn:
                try:
                    self._conn.close()
                except:
                    pass
        except:
            pass

        # Re-establish connection
        if self._is_postgres and self.postgres_url:
            self._conn = psycopg2.connect(self.postgres_url)
            print("  Database reconnected (PostgreSQL)", flush=True)
        else:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            print(f"  Database reconnected (SQLite: {self.db_path})", flush=True)
        return self._conn

    def is_connection_error(self, error: Exception) -> bool:
        """Check if exception is a connection-related error."""
        error_str = str(error).lower()
        connection_errors = [
            'connection already closed',
            'connection is closed',
            'server closed the connection',
            'could not receive data',
            'ssl syscall error',
            'operation timed out',
            'connection refused',
            'connection reset',
            'broken pipe',
            'network is unreachable',
        ]
        return any(err in error_str for err in connection_errors)

    def execute_with_retry(self, func, *args, max_retries: int = 3, **kwargs):
        """Execute a database function with automatic reconnection on failure."""
        last_error = None
        for attempt in range(max_retries):
            try:
                return func(self._conn, *args, **kwargs)
            except Exception as e:
                last_error = e
                if self.is_connection_error(e):
                    if attempt < max_retries - 1:
                        print(f"  Database error: {e}", flush=True)
                        self.reconnect()
                        time.sleep(1)
                    else:
                        raise
                else:
                    raise
        raise last_error

    @property
    def conn(self):
        """Get the underlying connection."""
        return self._conn

    def cursor(self):
        """Get a cursor from the connection."""
        return self._conn.cursor()

    def commit(self):
        """Commit the current transaction with retry."""
        for attempt in range(3):
            try:
                self._conn.commit()
                return
            except Exception as e:
                if self.is_connection_error(e) and attempt < 2:
                    self.reconnect()
                else:
                    raise

    def close(self):
        """Close the database connection."""
        if self._conn:
            try:
                self._conn.close()
            except:
                pass
            self._conn = None


# =============================================================================
# Parsing Functions
# =============================================================================

def extract_ingredient_name(product_name: str) -> str:
    """
    Extract a clean ingredient name from the product name.
    Removes percentages, standardizations, and other specifics.

    Examples:
    - "5-Hydroxytryptophan (5-HTP) 98%" → "5-Hydroxytryptophan (5-HTP)"
    - "Ashwagandha Root P.E. 5% Withanolides" → "Ashwagandha Root"
    - "Vitamin D3 (Cholecalciferol) 100,000 IU/g" → "Vitamin D3 (Cholecalciferol)"
    - "Green Tea Extract 98% Polyphenols 80% Catechins 50% EGCG" → "Green Tea Extract"
    """
    if not product_name:
        return ""

    name = product_name.strip()

    # Remove percentage specifications (e.g., "98%", "5% Withanolides")
    name = re.sub(r'\s+\d+(?:\.\d+)?%.*$', '', name)

    # Remove IU/g specifications
    name = re.sub(r'\s+[\d,]+\s*IU/g.*$', '', name, flags=re.IGNORECASE)

    # Remove P.E. (Powder Extract) and ratio specifications (4:1, 10:1)
    name = re.sub(r'\s+P\.?E\.?\s*\d*:?\d*.*$', '', name, flags=re.IGNORECASE)

    # Remove ratio specifications standalone (4:1, 10:1)
    name = re.sub(r'\s+\d+:\d+.*$', '', name)

    # Remove USP/NF/FCC grade specifications
    name = re.sub(r'\s+(USP|NF|FCC|BP|EP)(\s+.*)?$', '', name, flags=re.IGNORECASE)

    # Remove "Powder" or "Extract" suffixes if they're at the very end
    # (but keep them if they're part of the name like "Garlic Extract")

    # Clean up whitespace
    name = ' '.join(name.split())

    return name.strip()


def parse_size_to_kg(size_str: str) -> Optional[float]:
    """
    Parse size string to kg.
    Examples:
    - "2.2 lbs/1 kg" → 1.0
    - "25kgs" → 25.0
    - "10g" → 0.01
    - "100g" → 0.1
    - "1 lb" → 0.45359237
    - "Bulk Price" → None
    """
    if not size_str or size_str.lower() in ['select size', 'bulk price', 'bulk']:
        return None

    size_lower = size_str.lower().strip()

    # Try to find kg first (most reliable)
    kg_match = re.search(r'(\d+(?:\.\d+)?)\s*kg', size_lower)
    if kg_match:
        return float(kg_match.group(1))

    # Try grams
    g_match = re.search(r'(\d+(?:\.\d+)?)\s*g(?:ram)?s?(?:\s|$)', size_lower)
    if g_match:
        return float(g_match.group(1)) / 1000

    # Try pounds
    lb_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:lb|pound)s?', size_lower)
    if lb_match:
        return float(lb_match.group(1)) * 0.45359237

    return None


def parse_price(price_str: str) -> Optional[float]:
    """
    Parse price string to float.
    Examples:
    - "$ 795.00" → 795.00
    - "$24.99" → 24.99
    - "$ 1,195.00" → 1195.00
    - "Inquire Bulk Price" → None
    """
    if not price_str:
        return None

    price_str = price_str.strip()

    if 'inquire' in price_str.lower() or 'bulk price' in price_str.lower():
        return None

    # Remove $ and commas, then parse
    cleaned = re.sub(r'[,$\s]', '', price_str)
    try:
        return float(cleaned)
    except ValueError:
        return None


def calculate_price_per_kg(price: Optional[float], size_kg: Optional[float]) -> Optional[float]:
    """Calculate price per kg from price and size in kg."""
    if price is None or size_kg is None or size_kg <= 0:
        return None
    return round(price / size_kg, 2)


def extract_product_id_from_url(url: str) -> Optional[int]:
    """
    Extract product ID from wishlist/inquiry URLs.
    Examples:
    - "/cart/add_to_wishlist/889" → 889
    - "/products/enquiry_now/716" → 716
    """
    match = re.search(r'/(?:add_to_wishlist|enquiry_now)/(\d+)', url)
    if match:
        return int(match.group(1))
    return None


def format_product_details(rows: List[Dict], verbose: bool = True) -> str:
    """Format product details as a table for console output."""
    if not rows or not verbose:
        return ""

    lines = []
    lines.append(f"    {'Size':<25} {'Price':>12} {'$/kg':>12}")
    lines.append(f"    {'-'*25} {'-'*12} {'-'*12}")

    sorted_rows = sorted(rows, key=lambda r: r.get('size_kg') or 0)

    for row in sorted_rows:
        size = row.get('size_name', 'N/A')
        if len(size) > 25:
            size = size[:23] + '..'

        price = row.get('price')
        price_str = f"${price:,.2f}" if price else "Inquire"

        price_per_kg = row.get('price_per_kg')
        ppk_str = f"${price_per_kg:,.2f}" if price_per_kg else "-"

        lines.append(f"    {size:<25} {price_str:>12} {ppk_str:>12}")

    return '\n'.join(lines)


# =============================================================================
# Database Functions
# =============================================================================

DbConnection = Union['psycopg2.connection', 'sqlite3.Connection'] if HAS_POSTGRES else 'sqlite3.Connection'


def load_env_file():
    """Load environment variables from .env file if it exists."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()


def get_postgres_url() -> Optional[str]:
    """Get PostgreSQL connection URL from environment."""
    load_env_file()
    return os.environ.get('SUPABASE_DB_URL')


def is_postgres(conn) -> bool:
    """Check if connection is PostgreSQL."""
    return HAS_POSTGRES and hasattr(conn, 'info')


def db_placeholder(conn) -> str:
    """Return the correct placeholder for the database type."""
    return '%s' if is_postgres(conn) else '?'


def init_database(db_path: str = None) -> DbConnection:
    """Initialize database with schema and seed data."""
    postgres_url = get_postgres_url()
    if USE_POSTGRES and HAS_POSTGRES and postgres_url:
        return init_postgres_database(postgres_url)
    else:
        if not HAS_POSTGRES:
            print("  (psycopg2 not installed, using SQLite)")
        elif not postgres_url:
            print("  (SUPABASE_DB_URL not set, using SQLite)")
        return init_sqlite_database(db_path or DATABASE_FILE)


def init_postgres_database(db_url: str):
    """Initialize PostgreSQL database with TrafaPharma vendor."""
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    # Ensure TrafaPharma vendor exists
    cursor.execute('''
        INSERT INTO Vendors (name, pricing_model, status)
        VALUES (%s, %s, %s)
        ON CONFLICT (name) DO NOTHING
    ''', ('TrafaPharma', 'fixed_pack', 'active'))

    conn.commit()
    print("  PostgreSQL database initialized (Supabase) - TrafaPharma vendor added")
    return conn


def init_sqlite_database(db_path: str):
    """Initialize SQLite database with TrafaPharma vendor."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Basic schema if not exists
    cursor.execute('''CREATE TABLE IF NOT EXISTS Vendors (
        vendor_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        pricing_model TEXT,
        status TEXT DEFAULT 'active'
    )''')

    cursor.execute('INSERT OR IGNORE INTO Vendors (name, pricing_model, status) VALUES (?, ?, ?)',
                   ('TrafaPharma', 'fixed_pack', 'active'))

    conn.commit()
    print(f"  SQLite database initialized: {db_path}")
    return conn


# =============================================================================
# Relational Table Helper Functions
# =============================================================================

def get_or_create_category(conn, name: str) -> Optional[int]:
    """Get existing category_id or create new one."""
    if not name:
        return None
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(f'SELECT category_id FROM categories WHERE name = {ph}', (name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    if is_postgres(conn):
        cursor.execute(f'INSERT INTO categories (name) VALUES ({ph}) RETURNING category_id', (name,))
        return cursor.fetchone()[0]
    else:
        cursor.execute(f'INSERT INTO categories (name) VALUES ({ph})', (name,))
        return cursor.lastrowid


def get_or_create_ingredient(conn, name: str, category_id: Optional[int]) -> int:
    """Get existing ingredient_id or create new one."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(f'SELECT ingredient_id FROM ingredients WHERE name = {ph}', (name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    if is_postgres(conn):
        cursor.execute(f'INSERT INTO ingredients (name, category_id) VALUES ({ph}, {ph}) RETURNING ingredient_id', (name, category_id))
        return cursor.fetchone()[0]
    else:
        cursor.execute(f'INSERT INTO ingredients (name, category_id) VALUES ({ph}, {ph})', (name, category_id))
        return cursor.lastrowid


def get_or_create_manufacturer(conn, name: str) -> int:
    """Get existing manufacturer_id or create new one."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(f'SELECT manufacturer_id FROM manufacturers WHERE name = {ph}', (name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    if is_postgres(conn):
        cursor.execute(f'INSERT INTO manufacturers (name) VALUES ({ph}) RETURNING manufacturer_id', (name,))
        return cursor.fetchone()[0]
    else:
        cursor.execute(f'INSERT INTO manufacturers (name) VALUES ({ph})', (name,))
        return cursor.lastrowid


def get_or_create_variant(conn, ingredient_id: int, manufacturer_id: int, variant_name: str) -> int:
    """Get existing variant_id or create new one."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(
        f'SELECT variant_id FROM ingredientvariants WHERE ingredient_id = {ph} AND manufacturer_id = {ph} AND variant_name = {ph}',
        (ingredient_id, manufacturer_id, variant_name)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    if is_postgres(conn):
        cursor.execute(
            f'INSERT INTO ingredientvariants (ingredient_id, manufacturer_id, variant_name) VALUES ({ph}, {ph}, {ph}) RETURNING variant_id',
            (ingredient_id, manufacturer_id, variant_name)
        )
        return cursor.fetchone()[0]
    else:
        cursor.execute(
            f'INSERT INTO ingredientvariants (ingredient_id, manufacturer_id, variant_name) VALUES ({ph}, {ph}, {ph})',
            (ingredient_id, manufacturer_id, variant_name)
        )
        return cursor.lastrowid


def insert_scrape_source(conn, vendor_id: int, url: str, scraped_at: str) -> int:
    """Insert scrape source record, return source_id."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    if is_postgres(conn):
        cursor.execute(
            f'INSERT INTO scrapesources (vendor_id, product_url, scraped_at) VALUES ({ph}, {ph}, {ph}) RETURNING source_id',
            (vendor_id, url, scraped_at)
        )
        return cursor.fetchone()[0]
    else:
        cursor.execute(
            f'INSERT INTO scrapesources (vendor_id, product_url, scraped_at) VALUES ({ph}, {ph}, {ph})',
            (vendor_id, url, scraped_at)
        )
        return cursor.lastrowid


def upsert_vendor_ingredient(conn, vendor_id: int, variant_id: int,
                             sku: str, raw_name: str, source_id: int) -> int:
    """Insert or update vendor ingredient, return vendor_ingredient_id."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    now = datetime.now().isoformat()

    cursor.execute(
        f'''SELECT vendor_ingredient_id FROM vendoringredients
           WHERE vendor_id = {ph} AND variant_id = {ph} AND sku = {ph}''',
        (vendor_id, variant_id, sku)
    )
    row = cursor.fetchone()
    if row:
        vendor_ingredient_id = row[0]
        cursor.execute(
            f'''UPDATE vendoringredients SET raw_product_name = {ph},
               shipping_responsibility = {ph}, current_source_id = {ph},
               last_seen_at = {ph}, status = 'active'
               WHERE vendor_ingredient_id = {ph}''',
            (raw_name, TRAFA_BUSINESS_MODEL['shipping_responsibility'],
             source_id, now, vendor_ingredient_id)
        )
        return vendor_ingredient_id
    if is_postgres(conn):
        cursor.execute(
            f'''INSERT INTO vendoringredients
               (vendor_id, variant_id, sku, raw_product_name, shipping_responsibility,
                current_source_id, last_seen_at, status)
               VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, 'active')
               RETURNING vendor_ingredient_id''',
            (vendor_id, variant_id, sku, raw_name,
             TRAFA_BUSINESS_MODEL['shipping_responsibility'], source_id, now)
        )
        return cursor.fetchone()[0]
    else:
        cursor.execute(
            f'''INSERT INTO vendoringredients
               (vendor_id, variant_id, sku, raw_product_name, shipping_responsibility,
                current_source_id, last_seen_at, status)
               VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, 'active')''',
            (vendor_id, variant_id, sku, raw_name,
             TRAFA_BUSINESS_MODEL['shipping_responsibility'], source_id, now)
        )
        return cursor.lastrowid


def delete_old_price_tiers(conn, vendor_ingredient_id: int) -> None:
    """Delete existing price tiers for a vendor ingredient."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(f'DELETE FROM pricetiers WHERE vendor_ingredient_id = {ph}', (vendor_ingredient_id,))


def insert_price_tier(conn, vendor_ingredient_id: int, row_data: dict, source_id: int) -> None:
    """Insert price tier record for TrafaPharma (per_package pricing)."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)

    # Get kg unit_id
    cursor.execute(f'SELECT unit_id FROM units WHERE name = {ph}', ('kg',))
    unit_row = cursor.fetchone()
    unit_id = unit_row[0] if unit_row else None

    # Get per_package pricing model id
    cursor.execute(f'SELECT model_id FROM pricingmodels WHERE name = {ph}', ('per_package',))
    model_row = cursor.fetchone()
    pricing_model_id = model_row[0] if model_row else 2

    # Parse price
    price = row_data.get('price')
    if price is None:
        return  # Skip if no price (Inquire products)

    # Size in kg for min_quantity
    size_kg = row_data.get('size_kg') or 0

    cursor.execute(
        f'''INSERT INTO pricetiers
           (vendor_ingredient_id, pricing_model_id, unit_id, source_id, min_quantity,
            price, price_per_kg, effective_date, includes_shipping)
           VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})''',
        (vendor_ingredient_id, pricing_model_id, unit_id, source_id,
         size_kg,
         price,
         row_data.get('price_per_kg'),
         row_data.get('scraped_at', datetime.now().isoformat()),
         0)  # TrafaPharma: shipping_responsibility = 'buyer', so includes_shipping = 0
    )


def upsert_packaging_size(conn, vendor_ingredient_id: int, pack_size_kg: float, description: str) -> None:
    """Insert or update packaging size."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)

    # Get kg unit_id
    cursor.execute(f'SELECT unit_id FROM units WHERE name = {ph}', ('kg',))
    unit_row = cursor.fetchone()
    unit_id = unit_row[0] if unit_row else None

    # Delete existing and insert new
    cursor.execute(f'DELETE FROM packagingsizes WHERE vendor_ingredient_id = {ph}', (vendor_ingredient_id,))
    cursor.execute(
        f'''INSERT INTO packagingsizes (vendor_ingredient_id, unit_id, description, quantity)
           VALUES ({ph}, {ph}, {ph}, {ph})''',
        (vendor_ingredient_id, unit_id, description, pack_size_kg)
    )


def upsert_order_rule(conn, vendor_ingredient_id: int, pack_size_kg: float, scraped_at: str) -> None:
    """Insert or update order rule for TrafaPharma fixed_pack."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)

    # Get rule type id for fixed_pack
    cursor.execute(f'SELECT type_id FROM orderruletypes WHERE name = {ph}', ('fixed_pack',))
    type_row = cursor.fetchone()
    rule_type_id = type_row[0] if type_row else 2

    # Get kg unit_id
    cursor.execute(f'SELECT unit_id FROM units WHERE name = {ph}', ('kg',))
    unit_row = cursor.fetchone()
    unit_id = unit_row[0] if unit_row else None

    # Delete existing and insert new
    cursor.execute(f'DELETE FROM orderrules WHERE vendor_ingredient_id = {ph}', (vendor_ingredient_id,))
    cursor.execute(
        f'''INSERT INTO orderrules
           (vendor_ingredient_id, rule_type_id, unit_id, base_quantity, min_quantity, effective_date)
           VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})''',
        (vendor_ingredient_id, rule_type_id, unit_id, pack_size_kg, pack_size_kg, scraped_at)
    )


def upsert_inventory_simple(conn, vendor_ingredient_id: int, stock_status: str, source_id: int) -> None:
    """Insert or update simple inventory status (no warehouse location)."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    if is_postgres(conn):
        cursor.execute(
            f'''INSERT INTO vendorinventory (vendor_ingredient_id, source_id, stock_status, last_updated)
               VALUES ({ph}, {ph}, {ph}, {ph})
               ON CONFLICT (vendor_ingredient_id) DO UPDATE SET source_id = EXCLUDED.source_id, stock_status = EXCLUDED.stock_status, last_updated = EXCLUDED.last_updated''',
            (vendor_ingredient_id, source_id, stock_status, datetime.now().isoformat())
        )
    else:
        cursor.execute(
            f'''INSERT OR REPLACE INTO vendorinventory
               (vendor_ingredient_id, source_id, stock_status, last_updated)
               VALUES ({ph}, {ph}, {ph}, {ph})''',
            (vendor_ingredient_id, source_id, stock_status, datetime.now().isoformat())
        )


def mark_stale_variants(conn, vendor_id: int, scrape_start_time: str) -> int:
    """Mark variants not seen in this scrape as inactive.

    Call this after a FULL scrape (not --max-products) to detect products
    that have been removed from the vendor's site.
    """
    cursor = conn.cursor()
    ph = db_placeholder(conn)

    # Variants with last_seen_at BEFORE this scrape started are stale
    cursor.execute(
        f'''UPDATE vendoringredients
           SET status = 'inactive'
           WHERE vendor_id = {ph}
           AND status = 'active'
           AND (last_seen_at IS NULL OR last_seen_at < {ph})''',
        (vendor_id, scrape_start_time)
    )

    stale_count = cursor.rowcount
    if stale_count > 0:
        print(f"  Marked {stale_count} variants as inactive (not seen in this scrape)")

    return stale_count


def mark_missing_variants_for_product(conn, vendor_id: int, variant_id: int,
                                       seen_skus: List[str], scrape_time: str) -> int:
    """Mark variants of this product that weren't in current scrape as inactive."""
    if not seen_skus:
        return 0

    cursor = conn.cursor()
    ph = db_placeholder(conn)

    # Mark variants for this product NOT in seen_skus as inactive
    placeholders = ','.join([ph] * len(seen_skus))
    cursor.execute(
        f'''UPDATE vendoringredients
           SET status = 'inactive'
           WHERE vendor_id = {ph}
           AND variant_id = {ph}
           AND sku NOT IN ({placeholders})
           AND status = 'active' ''',
        (vendor_id, variant_id, *seen_skus)
    )

    return cursor.rowcount


def save_to_relational_tables(conn, rows: List[Dict]) -> None:
    """Save processed product rows to the relational tables."""
    if not rows:
        return

    cursor = conn.cursor()
    ph = db_placeholder(conn)

    # Get vendor_id for TrafaPharma
    cursor.execute(f'SELECT vendor_id FROM vendors WHERE name = {ph}', ('TrafaPharma',))
    vendor_row = cursor.fetchone()
    if not vendor_row:
        print("  Warning: TrafaPharma vendor not found, skipping relational tables")
        return
    vendor_id = vendor_row[0]

    # All rows for same product share same base info
    first_row = rows[0]
    product_name = first_row.get('product_name', '')
    ingredient_name = first_row.get('ingredient_name', '') or product_name
    category = first_row.get('category')
    url = first_row.get('url', '')
    scraped_at = first_row.get('scraped_at', datetime.now().isoformat())

    # Create source record
    source_id = insert_scrape_source(conn, vendor_id, url, scraped_at)

    # Create category if available
    category_id = get_or_create_category(conn, category) if category else None

    # Create ingredient using cleaned ingredient name
    ingredient_id = get_or_create_ingredient(conn, ingredient_name, category_id)

    # Create manufacturer (TrafaPharma products don't have manufacturer info, use "Unknown")
    manufacturer_id = get_or_create_manufacturer(conn, 'Unknown')

    # Create variant
    variant_id = get_or_create_variant(conn, ingredient_id, manufacturer_id, product_name)

    # Process each variant row (different sizes)
    seen_skus = []
    for row in rows:
        product_id = row.get('product_id')
        size_id = row.get('size_id') or 'default'
        # Generate SKU from product_id and size_id
        sku = f"{product_id}-{size_id}"
        seen_skus.append(sku)

        size_kg = row.get('size_kg') or 0
        size_description = row.get('size_name', '')
        stock_status = row.get('stock_status', 'unknown')

        # Create/update vendor ingredient
        vendor_ingredient_id = upsert_vendor_ingredient(
            conn, vendor_id, variant_id, sku, product_name, source_id
        )

        # Delete old price tier and insert new (only if price exists)
        delete_old_price_tiers(conn, vendor_ingredient_id)
        if row.get('price') is not None:
            insert_price_tier(conn, vendor_ingredient_id, row, source_id)

        # Insert packaging info
        upsert_packaging_size(conn, vendor_ingredient_id, size_kg, size_description)

        # Insert order rule
        upsert_order_rule(conn, vendor_ingredient_id, size_kg, scraped_at)

        # Insert inventory status
        upsert_inventory_simple(conn, vendor_ingredient_id, stock_status, source_id)

    # Mark variants not in this batch as inactive (variant-level staleness)
    mark_missing_variants_for_product(conn, vendor_id, variant_id, seen_skus, scraped_at)


# =============================================================================
# Progress Tracking
# =============================================================================

class ProgressTracker:
    """Track and display progress with ETA."""

    def __init__(self, total: int):
        self.total = total
        self.completed = 0
        self.failed = 0
        self.skipped = 0
        self.start_time = time.time()

    def update(self, success: bool = True, item_name: str = "", status: str = None):
        """Update progress and print status."""
        self.completed += 1
        if status and status.startswith("SKIPPED"):
            self.skipped += 1
        elif not success:
            self.failed += 1

        elapsed = time.time() - self.start_time
        rate = self.completed / elapsed if elapsed > 0 else 0
        remaining = self.total - self.completed
        eta_seconds = remaining / rate if rate > 0 else 0
        eta = str(timedelta(seconds=int(eta_seconds)))

        pct = (self.completed / self.total) * 100
        if status is None:
            status = "OK" if success else "ERROR"
        timestamp = datetime.now().strftime("%H:%M:%S")

        # Truncate item name for display
        display_name = item_name[:40] if len(item_name) > 40 else item_name

        print(f"[{timestamp}] [{self.completed}/{self.total}] ({pct:5.1f}%) "
              f"{display_name:<40} [{status}] "
              f"| {rate:.1f}/s | ETA: {eta}", flush=True)

    def summary(self):
        """Print final summary."""
        elapsed = time.time() - self.start_time
        elapsed_str = str(timedelta(seconds=int(elapsed)))
        successful = self.completed - self.failed - self.skipped
        print(f"\n{'='*60}")
        print(f"Completed: {successful}/{self.total} "
              f"({self.skipped} skipped, {self.failed} errors) in {elapsed_str}")
        print(f"{'='*60}", flush=True)


# =============================================================================
# Checkpoint Functions
# =============================================================================

def save_checkpoint(processed_slugs: List[str], all_data: List[Dict],
                    all_products: List[Dict], output_file: str = None) -> None:
    """Save scraping progress to checkpoint file."""
    checkpoint = {
        'processed_slugs': processed_slugs,
        'all_products': all_products,
        'data_count': len(all_data),
        'output_file': output_file,
        'timestamp': datetime.now().isoformat()
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)

    if all_data and output_file:
        df = pd.DataFrame(all_data)
        df.to_csv(output_file, index=False)


def load_checkpoint() -> Optional[Dict]:
    """Load checkpoint if it exists."""
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading checkpoint: {e}")
        return None


def clear_checkpoint() -> None:
    """Remove checkpoint file after successful completion."""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("Cleared checkpoint file")


# =============================================================================
# HTTP Fetch with Exponential Backoff
# =============================================================================

def fetch_with_backoff(url: str, session: requests.Session,
                       method: str = 'GET', data: dict = None) -> Optional[str]:
    """Fetch URL with exponential backoff retry logic. Returns HTML text."""
    for attempt in range(MAX_RETRIES):
        try:
            if method.upper() == 'POST':
                response = session.post(url, headers=HEADERS, data=data, timeout=30)
            else:
                response = session.get(url, headers=HEADERS, timeout=30)

            if response.status_code == 429:
                delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                print(f"    Rate limited, backoff {delay}s...", flush=True)
                time.sleep(delay)
                continue

            response.raise_for_status()
            return response.text

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"    Failed after {MAX_RETRIES} attempts: {e}", flush=True)
                return None
            delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
            time.sleep(delay)

    return None


def fetch_with_backoff_ajax(url: str, session: requests.Session,
                            data: dict = None) -> Optional[str]:
    """Fetch URL with AJAX headers and exponential backoff. Returns HTML text."""
    for attempt in range(MAX_RETRIES):
        try:
            response = session.post(url, headers=AJAX_HEADERS, data=data, timeout=30)

            if response.status_code == 429:
                delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                print(f"    Rate limited, backoff {delay}s...", flush=True)
                time.sleep(delay)
                continue

            response.raise_for_status()
            return response.text

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"    Failed after {MAX_RETRIES} attempts: {e}", flush=True)
                return None
            delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
            time.sleep(delay)

    return None


# =============================================================================
# Product Discovery (via Pagination)
# =============================================================================

def discover_products(session: requests.Session, max_products: int = None) -> List[Dict]:
    """
    Discover all products using infinite scroll pagination.
    Returns list of dicts with basic product info (name, slug, category, base_price).
    """
    all_products = []
    offset = 0

    print("\n" + "=" * 60, flush=True)
    print("PHASE 1: Discovering products via pagination", flush=True)
    print("=" * 60, flush=True)

    while True:
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] Fetching page offset={offset}...", end=" ", flush=True)

        # Form data for pagination POST
        form_data = {
            'offset': offset,
        }

        html = fetch_with_backoff(PRODUCTS_AJAX_URL, session, method='POST', data=form_data)

        if not html or html.strip() == '':
            print("empty - done!", flush=True)
            break

        soup = BeautifulSoup(html, 'html.parser')

        # Parse product cards from the HTML response
        # Looking for product listing structure based on site analysis
        product_cards = soup.find_all('li') or soup.find_all('div', class_=re.compile(r'product|item'))

        if not product_cards:
            # Try alternative selectors
            product_links = soup.find_all('a', href=re.compile(r'^https://trafapharma\.com/[a-z0-9-]+$'))
            if not product_links:
                print("no products found - done!", flush=True)
                break

        batch_products = []

        # Parse each product from the response
        for card in product_cards:
            try:
                # Find product link
                link = card.find('a', href=True)
                if not link:
                    continue

                href = link.get('href', '')
                if not href or 'trafapharma.com' not in href:
                    continue

                # Extract slug from URL
                slug_match = re.search(r'trafapharma\.com/([a-z0-9-_.]+)$', href, re.IGNORECASE)
                if not slug_match:
                    continue

                slug = slug_match.group(1)

                # Skip non-product pages
                if slug in ['products', 'category', 'cart', 'users', 'members', 'pages',
                            'contactus', 'aboutus', 'faq', 'sitemap']:
                    continue

                # Find product name
                name_el = card.find(['h2', 'h3', 'h4', 'a', 'span'], string=True)
                name = name_el.get_text(strip=True) if name_el else slug.replace('-', ' ').title()

                # Find price
                price_el = card.find(string=re.compile(r'\$\s*[\d,]+\.?\d*'))
                base_price = None
                if price_el:
                    base_price = parse_price(str(price_el))

                # Find category
                category_el = card.find(string=re.compile(r'Category|:'))
                category = None
                if category_el:
                    parent = category_el.find_parent()
                    if parent:
                        cat_text = parent.get_text(strip=True)
                        cat_match = re.search(r'Category\s*:\s*(.+)', cat_text)
                        if cat_match:
                            category = cat_match.group(1).strip()

                # Extract product ID from wishlist link
                wishlist_link = card.find('a', href=re.compile(r'add_to_wishlist'))
                product_id = None
                if wishlist_link:
                    product_id = extract_product_id_from_url(wishlist_link.get('href', ''))

                batch_products.append({
                    'slug': slug,
                    'name': name,
                    'url': href,
                    'category': category,
                    'base_price': base_price,
                    'product_id': product_id,
                })

            except Exception as e:
                continue

        if not batch_products:
            print("no valid products - done!", flush=True)
            break

        # Deduplicate within batch
        existing_slugs = {p['slug'] for p in all_products}
        new_products = [p for p in batch_products if p['slug'] not in existing_slugs]

        all_products.extend(new_products)
        print(f"found {len(new_products)} new products (total: {len(all_products)})", flush=True)

        if max_products and len(all_products) >= max_products:
            all_products = all_products[:max_products]
            print(f"Reached max products limit ({max_products})", flush=True)
            break

        offset += 1
        time.sleep(REQUEST_DELAY)

    print(f"\nDiscovered {len(all_products)} products", flush=True)
    return all_products


def discover_products_from_main_page(session: requests.Session, max_products: int = None) -> List[Dict]:
    """
    Discover products from the products listing page.
    Uses the products page and AJAX pagination to find all products.
    Products are identified by having an "Add to Cart" link nearby.

    Pagination uses offset parameter that represents number of products to skip.
    Each page returns PRODUCTS_PER_PAGE (12) products.
    Total products on site: ~663
    """
    all_products = []
    offset = 0
    max_offset = 700  # Safety limit (> 663 total products)

    print("\n" + "=" * 60, flush=True)
    print("PHASE 1: Discovering products from listing page", flush=True)
    print("=" * 60, flush=True)

    # Known non-product URL patterns to skip
    skip_slugs = {
        'products', 'category', 'cart', 'users', 'members', 'pages',
        'contactus', 'aboutus', 'faq', 'sitemap', 'featured-products',
        'new-products', 'trending-products', 'how-to-buy', 'return-policy',
        'privacy-policy', 'terms-conditions', 'testimonials', 'stripe_make_payment',
        # Category slugs
        'amino-acid', 'animal-ingredients', 'bee-ingredients', 'carotenoids',
        'clays', 'colors', 'compounding-chemicals', 'cosmeceutical-ingredients',
        'dairy-ingredients', 'digital-scales', 'empty-capsules', 'enzymes',
        'excipients-non-medicinal', 'fatty-acids', 'food-chemicals',
        'fortified-plant-extract', 'fruit-powders', 'gift-certificates',
        'glandular-powder', 'greens', 'gums', 'herbal-powders', 'liquid-extracts',
        'marine-ingredients', 'minerals', 'mushroom-powder-extracts', 'non-dietary',
        'nutraceutical-ingredients', 'oil-powders', 'oils', 'organic-herbal-powders',
        'peptide', 'phytoceutical-ingredients', 'poultry-ingredients', 'prebiotics',
        'preservatives', 'probiotics', 'proteins', 'ratio-powder-extracts',
        'softgels', 'sports', 'standardized-plant-extracts', 'sweeteners',
        'vitamins', 'whole-herbs/seeds', 'yeast-ingredients',
    }

    seen_slugs = set()

    while offset < max_offset:
        timestamp = datetime.now().strftime("%H:%M:%S")
        page_num = offset // PRODUCTS_PER_PAGE
        print(f"[{timestamp}] Fetching page {page_num} (offset={offset})...", end=" ", flush=True)

        # Form data matching the site's #myform structure
        form_data = {
            'sortbyprice': '',
            'sortbyname': '',
            'category_id': '',
            'keyword2': '',
            'amount': '',
            'per_page': '',
            'offset': str(offset)
        }

        if offset == 0:
            # First page - use GET request
            url = f"{BASE_URL}/products"
            html = fetch_with_backoff(url, session, method='GET')
        else:
            # Subsequent pages - use POST with AJAX headers
            html = fetch_with_backoff_ajax(PRODUCTS_AJAX_URL, session, data=form_data)

        if not html:
            print("error fetching", flush=True)
            break

        soup = BeautifulSoup(html, 'html.parser')

        batch_products = []

        # Find "Add to Cart" links - these indicate actual products
        # The cart links contain an image with "Cart" alt text
        cart_links = []

        # Method 1: Find links containing images with Cart alt text
        for img in soup.find_all('img', alt=re.compile(r'Cart', re.IGNORECASE)):
            parent_link = img.find_parent('a')
            if parent_link and parent_link.get('href'):
                cart_links.append(parent_link)

        # Method 2: Find links with "Add to Cart" text
        for link in soup.find_all('a', string=re.compile(r'Add to Cart', re.IGNORECASE)):
            if link not in cart_links:
                cart_links.append(link)

        for cart_link in cart_links:
            # The cart link href points to the product page
            href = cart_link.get('href', '')
            if not href or 'trafapharma.com' not in href:
                continue

            slug_match = re.search(r'trafapharma\.com/([a-z0-9-_.%]+)$', href, re.IGNORECASE)
            if not slug_match:
                continue

            slug = slug_match.group(1)

            # Skip non-product pages and already seen
            if slug in skip_slugs or slug in seen_slugs:
                continue

            seen_slugs.add(slug)

            # Find the product name - look in parent elements for link with product name
            parent = cart_link.find_parent(['li', 'div', 'article'])
            name = None
            product_id = None
            category = None
            base_price = None

            if parent:
                # Find product name link
                name_link = parent.find('a', href=href)
                if name_link:
                    name = name_link.get_text(strip=True)
                else:
                    # Try other links
                    for link in parent.find_all('a', href=True):
                        link_href = link.get('href', '')
                        if slug in link_href and link.get_text(strip=True):
                            name = link.get_text(strip=True)
                            break

                # Find category
                cat_el = parent.find(string=re.compile(r'Category\s*:', re.IGNORECASE))
                if cat_el:
                    cat_parent = cat_el.find_parent()
                    if cat_parent:
                        category = cat_parent.get_text(strip=True).replace('Category :', '').strip()

                # Find price
                price_el = parent.find(string=re.compile(r'\$\s*[\d,]+\.?\d*'))
                if price_el:
                    base_price = parse_price(str(price_el))

                # Find product ID from wishlist link
                wishlist_link = parent.find('a', href=re.compile(r'add_to_wishlist/(\d+)'))
                if wishlist_link:
                    product_id = extract_product_id_from_url(wishlist_link.get('href', ''))

            if not name:
                name = slug.replace('-', ' ').replace('.', ' ').title()

            batch_products.append({
                'slug': slug,
                'name': name,
                'url': href,
                'category': category,
                'base_price': base_price,
                'product_id': product_id,
            })

        # Check if we got any new products
        if not batch_products:
            print("no new products - done!", flush=True)
            break

        all_products.extend(batch_products)
        print(f"found {len(batch_products)} products (total: {len(all_products)})", flush=True)

        if max_products and len(all_products) >= max_products:
            all_products = all_products[:max_products]
            break

        # Move to next page (increment by products per page)
        offset += PRODUCTS_PER_PAGE
        time.sleep(REQUEST_DELAY)

    print(f"\nDiscovered {len(all_products)} products", flush=True)
    return all_products


# =============================================================================
# Product Detail Scraping
# =============================================================================

def scrape_product_details(slug: str, session: requests.Session) -> List[Dict]:
    """
    Scrape a single product page for all size variants and their prices.
    Returns list of dicts, one per size variant.
    """
    url = f"{BASE_URL}/{slug}"
    rows = []

    # Fetch the product page
    html = fetch_with_backoff(url, session, method='GET')
    if not html:
        return []

    soup = BeautifulSoup(html, 'html.parser')

    # Extract product info
    product_name = None

    # Method 1: Get from page title (most reliable)
    title_tag = soup.find('title')
    if title_tag:
        title = title_tag.get_text(strip=True)
        # Remove site name suffix if present
        if '|' in title:
            product_name = title.split('|')[0].strip()
        elif ' - Trafa' in title:
            product_name = title.split(' - Trafa')[0].strip()
        else:
            product_name = title

    # Method 2: Look for breadcrumb - last item is product name
    if not product_name or len(product_name) < 3:
        breadcrumbs = soup.find_all('li')
        for li in reversed(breadcrumbs):
            text = li.get_text(strip=True)
            # Skip navigation items
            if text and 'Home' not in text and 'Categories' not in text and len(text) > 3:
                # Check if this looks like a product name (not a category)
                if not any(cat in text.lower() for cat in ['powder', 'extract', 'vitamin', 'mineral']):
                    continue
                product_name = text
                break

    # Method 3: Look for heading elements in main content
    if not product_name or len(product_name) < 3:
        for tag in soup.find_all(['h1', 'h2', 'h3']):
            text = tag.get_text(strip=True)
            # Skip common non-product headings
            skip_texts = ['quick links', 'shopping guide', 'my account', 'contact us',
                          'follow us', 'about trafa', 'payment', 'subscribe', 'filters',
                          'product lists', 'featured', 'new ingredients', 'trending']
            if text and not any(skip in text.lower() for skip in skip_texts):
                if len(text) > 3 and len(text) < 150:
                    product_name = text
                    break

    # Method 4: Extract from URL slug as final fallback
    if not product_name or len(product_name) < 3:
        product_name = slug.replace('-', ' ').replace('.', ' ').title()

    # Find product code
    product_code = None
    code_el = soup.find(string=re.compile(r'Product code|Product Code', re.IGNORECASE))
    if code_el:
        parent = code_el.find_parent()
        if parent:
            # Look for the next sibling or text that contains the code
            next_text = parent.find_next_sibling()
            if next_text:
                product_code = next_text.get_text(strip=True)
            else:
                # Try to extract from same element
                full_text = parent.get_text(strip=True)
                code_match = re.search(r'(?:Product code|Code)\s*:?\s*([A-Z0-9]+)', full_text, re.IGNORECASE)
                if code_match:
                    product_code = code_match.group(1)

    # Find category
    category = None
    cat_el = soup.find(string=re.compile(r'^Category\s*:?', re.IGNORECASE))
    if cat_el:
        parent = cat_el.find_parent()
        if parent:
            next_el = parent.find_next_sibling()
            if next_el:
                category = next_el.get_text(strip=True)

    # Find product ID from wishlist/inquiry links
    product_id = None
    wishlist_link = soup.find('a', href=re.compile(r'add_to_wishlist|enquiry_now'))
    if wishlist_link:
        product_id = extract_product_id_from_url(wishlist_link.get('href', ''))

    # Find size dropdown
    size_select = soup.find('select', {'id': re.compile(r'size|prod_size', re.IGNORECASE)}) or \
                  soup.find('select', attrs={'name': re.compile(r'size', re.IGNORECASE)})

    if not size_select:
        # Look for combobox by content
        size_select = soup.find('select')

    if not size_select:
        # No size dropdown - single product with one price
        price_el = soup.find(string=re.compile(r'\$\s*[\d,]+\.?\d*'))
        price = parse_price(str(price_el)) if price_el else None

        rows.append({
            'product_id': product_id,
            'product_code': product_code,
            'product_name': product_name,
            'ingredient_name': extract_ingredient_name(product_name),
            'category': category,
            'size_id': None,
            'size_name': 'Default',
            'size_kg': None,
            'price': price,
            'price_per_kg': None,
            'stock_status': 'unknown',
            'order_rule_type': TRAFA_BUSINESS_MODEL['order_rule_type'],
            'shipping_responsibility': TRAFA_BUSINESS_MODEL['shipping_responsibility'],
            'url': url,
        })
        return rows

    # Extract size options
    size_options = size_select.find_all('option')

    for option in size_options:
        size_id = option.get('value', '')
        size_name = option.get_text(strip=True)

        # Skip placeholder options
        if not size_id or size_name.lower() in ['select size', 'select', '']:
            continue

        size_kg = parse_size_to_kg(size_name)

        # POST to get price for this size
        form_data = {
            'prod_size': size_id,
        }

        size_html = fetch_with_backoff(url, session, method='POST', data=form_data)

        price = None
        if size_html:
            size_soup = BeautifulSoup(size_html, 'html.parser')
            # Find price in the response - look in #sec_id or price element
            sec_id = size_soup.find(id='sec_id')
            if sec_id:
                price_el = sec_id.find(string=re.compile(r'\$\s*[\d,]+\.?\d*'))
                if price_el:
                    price = parse_price(str(price_el))
            else:
                # Fallback: look for price anywhere
                price_el = size_soup.find(string=re.compile(r'\$\s*[\d,]+\.?\d*'))
                if price_el:
                    price = parse_price(str(price_el))

        price_per_kg = calculate_price_per_kg(price, size_kg)

        rows.append({
            'product_id': product_id,
            'product_code': product_code,
            'product_name': product_name,
            'ingredient_name': extract_ingredient_name(product_name),
            'category': category,
            'size_id': size_id,
            'size_name': size_name,
            'size_kg': size_kg,
            'price': price,
            'price_per_kg': price_per_kg,
            'stock_status': 'unknown',
            'order_rule_type': TRAFA_BUSINESS_MODEL['order_rule_type'],
            'shipping_responsibility': TRAFA_BUSINESS_MODEL['shipping_responsibility'],
            'url': url,
        })

        time.sleep(REQUEST_DELAY * 0.5)  # Shorter delay between size requests

    return rows


# =============================================================================
# Main
# =============================================================================

def save_to_csv(data: List[Dict], output_dir: str = ".") -> str:
    """Save scraped data to a timestamped CSV file."""
    if not data:
        print("No data to save")
        return ""

    df = pd.DataFrame(data)

    # Reorder columns
    priority_cols = [
        'product_id', 'product_code', 'product_name', 'ingredient_name', 'category',
        'size_id', 'size_name', 'size_kg',
        'price', 'price_per_kg',
        'stock_status', 'order_rule_type', 'shipping_responsibility',
        'url', 'scraped_at'
    ]
    other_cols = [c for c in df.columns if c not in priority_cols]
    ordered_cols = [c for c in priority_cols if c in df.columns] + other_cols
    df = df[ordered_cols]

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"trafapharma_products_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False)
    print(f"\nSaved {len(data)} rows to: {filepath}")

    return filepath


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='TrafaPharma.com Product Scraper'
    )
    parser.add_argument('--resume', action='store_true',
                        help='Resume from checkpoint if available')
    parser.add_argument('--max-products', type=int, default=None,
                        help='Maximum products to scrape (for testing)')
    parser.add_argument('--discovery-only', action='store_true',
                        help='Only discover products, do not scrape details')
    args = parser.parse_args()

    print("=" * 60, flush=True)
    print("TrafaPharma.com Product Scraper", flush=True)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print("=" * 60, flush=True)

    # Record scrape start time for staleness tracking
    scrape_start_time = datetime.now().isoformat()

    # Create session for connection pooling
    session = requests.Session()

    # Check for checkpoint
    checkpoint = load_checkpoint()
    processed_slugs = []
    all_data = []
    all_products = []
    output_file = None

    if checkpoint and args.resume:
        print(f"\nFound checkpoint from {checkpoint['timestamp']}", flush=True)
        print(f"  Processed: {len(checkpoint['processed_slugs'])} products", flush=True)
        print(f"  Remaining: {len(checkpoint['all_products']) - len(checkpoint['processed_slugs'])} products", flush=True)

        resume = input("\nResume from checkpoint? [Y/n]: ").strip().lower()
        if resume != 'n':
            processed_slugs = checkpoint['processed_slugs']
            all_products = checkpoint['all_products']
            output_file = checkpoint.get('output_file')
            if output_file and os.path.exists(output_file):
                df = pd.read_csv(output_file)
                all_data = df.to_dict('records')
                print(f"  Loaded {len(all_data)} existing data rows", flush=True)
        else:
            clear_checkpoint()
            checkpoint = None
    elif checkpoint and not args.resume:
        print(f"\nNote: Checkpoint exists from {checkpoint['timestamp']}", flush=True)
        print("  Use --resume to continue, or it will start fresh", flush=True)
        clear = input("Clear checkpoint and start fresh? [y/N]: ").strip().lower()
        if clear == 'y':
            clear_checkpoint()
        checkpoint = None

    # Discover products if not resuming
    if not all_products:
        all_products = discover_products_from_main_page(session, max_products=args.max_products)

        if not all_products:
            print("No products discovered. Exiting.", flush=True)
            sys.exit(1)

    if args.discovery_only:
        # Just save discovered products and exit
        df = pd.DataFrame(all_products)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        filename = f"trafapharma_discovered_{timestamp}.csv"
        df.to_csv(filename, index=False)
        print(f"\nSaved {len(all_products)} discovered products to: {filename}")
        sys.exit(0)

    # Generate output filename
    if not output_file:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        output_file = f"trafapharma_products_{timestamp}.csv"

    # Initialize database
    db_path = DATABASE_FILE
    print(f"\nInitializing database: {db_path}")
    db_wrapper = DatabaseConnection(db_path)
    db_wrapper.connect()
    print("Database initialized")

    # Filter out already processed
    if processed_slugs:
        remaining_products = [p for p in all_products if p['slug'] not in processed_slugs]
        print(f"\nResuming: {len(remaining_products)} products remaining", flush=True)
    else:
        remaining_products = all_products

    # Scrape product details
    print("\n" + "=" * 60, flush=True)
    print("PHASE 2: Scraping product details", flush=True)
    print("=" * 60, flush=True)

    progress = ProgressTracker(len(remaining_products))

    for i, product in enumerate(remaining_products, 1):
        slug = product['slug']
        try:
            rows = scrape_product_details(slug, session)
            if rows:
                all_data.extend(rows)
                db_wrapper.execute_with_retry(save_to_relational_tables, rows)
                processed_slugs.append(slug)
                progress.update(success=True, item_name=product.get('name', slug))

                # Print detailed variant table
                print(f"    -> {len(rows)} size variants", flush=True)
                details = format_product_details(rows)
                if details:
                    print(details, flush=True)
                print(flush=True)
            else:
                progress.update(success=False, item_name=product.get('name', slug),
                              status="SKIPPED-NO_DATA")
                print(flush=True)
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            progress.update(success=False, item_name=product.get('name', slug), status="ERROR")

        # Save checkpoint periodically
        if len(processed_slugs) % CHECKPOINT_INTERVAL == 0:
            print(f"\n>>> Checkpoint saved: {len(processed_slugs)} products <<<\n", flush=True)
            db_wrapper.commit()
            save_checkpoint(processed_slugs, all_data, all_products, output_file)

        # Rate limiting
        if i < len(remaining_products):
            time.sleep(REQUEST_DELAY)

    progress.summary()

    # Save final results
    print("\n" + "=" * 60, flush=True)
    print("PHASE 3: Saving results", flush=True)
    print("=" * 60, flush=True)

    if all_data:
        # Add scraped_at timestamp
        timestamp_str = datetime.now().isoformat()
        for row in all_data:
            if 'scraped_at' not in row:
                row['scraped_at'] = timestamp_str

        filepath = save_to_csv(all_data)

        # Mark stale variants (only on full scrapes, not --max-products)
        if not args.max_products:
            # Get TrafaPharma vendor_id
            cursor = db_wrapper.cursor()
            cursor.execute("SELECT vendor_id FROM vendors WHERE name = %s", ('TrafaPharma',))
            vendor_row = cursor.fetchone()
            if vendor_row:
                vendor_id = vendor_row[0]
                stale_count = mark_stale_variants(db_wrapper.conn, vendor_id, scrape_start_time)
                if stale_count > 0:
                    print(f"  Staleness check: {stale_count} variants marked inactive")

        # Final database commit and close
        db_wrapper.commit()
        db_wrapper.close()

        clear_checkpoint()

        print("\n" + "=" * 60, flush=True)
        print("SCRAPING COMPLETE", flush=True)
        print("=" * 60, flush=True)
        print(f"Total products scraped: {len(processed_slugs)}", flush=True)
        print(f"Total size variants extracted: {len(all_data)}", flush=True)
        print(f"Output file: {filepath}", flush=True)
        print(f"Database file: {db_path}", flush=True)

        # Preview
        print("\nData preview:", flush=True)
        df = pd.DataFrame(all_data)
        preview_cols = ['product_name', 'size_name', 'price', 'price_per_kg']
        available_cols = [c for c in preview_cols if c in df.columns]
        print(df[available_cols].head(10).to_string(), flush=True)

        # Stats
        print("\n" + "-" * 40, flush=True)
        print("Statistics:", flush=True)
        print(f"  Unique products: {df['product_name'].nunique()}", flush=True)
        print(f"  Total size variants: {len(df)}", flush=True)
        priced = df['price'].notna().sum()
        inquire = len(df) - priced
        print(f"  With price: {priced}", flush=True)
        print(f"  Inquire only: {inquire}", flush=True)
        if priced > 0:
            try:
                prices = df['price'].dropna()
                print(f"  Price range: ${prices.min():.2f} - ${prices.max():.2f}", flush=True)
            except:
                pass
            if 'price_per_kg' in df.columns:
                try:
                    ppk = df['price_per_kg'].dropna()
                    if len(ppk) > 0:
                        print(f"  Price/kg range: ${ppk.min():.2f} - ${ppk.max():.2f}", flush=True)
                except:
                    pass
    else:
        print("\nNo data was extracted.", flush=True)
        db_wrapper.close()


if __name__ == "__main__":
    main()
