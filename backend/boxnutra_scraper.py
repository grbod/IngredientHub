#!/usr/bin/env python3
"""
BoxNutra.com Product Scraper

Scrapes all products from BoxNutra.com including variants, pricing, and availability.
Output is saved to a timestamped CSV file with checkpoint support.

Based on bulksupplements_scraper.py with adaptations:
- HTML scraping for availability (JSON API returns null)
- Filters non-ingredient products (shipping insurance, gift cards, deposits)
- Direct grams field from JSON (no parsing needed)
"""

import os
import sys
import re
import json
import time
import sqlite3
import argparse
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Optional PostgreSQL support
try:
    import psycopg2
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False


# =============================================================================
# Configuration
# =============================================================================

BASE_URL = "https://www.boxnutra.com"

# Rate limiting
REQUEST_DELAY = 0.5  # Seconds between requests

# Retry configuration (exponential backoff)
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 30

# Checkpoint configuration
CHECKPOINT_INTERVAL = 10
CHECKPOINT_FILE = "output/.boxnutra_checkpoint.json"

# Request headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json,*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
}

# BoxNutra Business Model Constants
BOXNUTRA_BUSINESS_MODEL = {
    'order_rule_type': 'fixed_pack',
    'shipping_responsibility': 'vendor',  # Free shipping $49+
}


# =============================================================================
# Statistics & Reporting Types
# =============================================================================

class AlertType(Enum):
    """Types of alerts that can be raised during scraping."""
    NEW_PRODUCT = "new_product"
    REACTIVATED = "reactivated"
    PRICE_DECREASE_MAJOR = "price_decrease_major"
    PRICE_INCREASE_MAJOR = "price_increase_major"
    STOCK_OUT = "stock_out"
    STALE_VARIANT = "stale_variant"
    PARSE_FAILURE = "parse_failure"
    MISSING_REQUIRED = "missing_required"
    DB_ERROR = "db_error"
    HTTP_ERROR = "http_error"


class AlertSeverity(Enum):
    """Severity levels for alerts."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


# Map alert types to their severity
ALERT_SEVERITY = {
    AlertType.NEW_PRODUCT: AlertSeverity.INFO,
    AlertType.REACTIVATED: AlertSeverity.INFO,
    AlertType.PRICE_DECREASE_MAJOR: AlertSeverity.CRITICAL,
    AlertType.PRICE_INCREASE_MAJOR: AlertSeverity.WARNING,
    AlertType.STOCK_OUT: AlertSeverity.WARNING,
    AlertType.STALE_VARIANT: AlertSeverity.WARNING,
    AlertType.PARSE_FAILURE: AlertSeverity.WARNING,
    AlertType.MISSING_REQUIRED: AlertSeverity.WARNING,
    AlertType.DB_ERROR: AlertSeverity.CRITICAL,
    AlertType.HTTP_ERROR: AlertSeverity.CRITICAL,
}


@dataclass
class Alert:
    """Individual alert record."""
    alert_type: AlertType
    severity: AlertSeverity
    sku: Optional[str] = None
    product_name: Optional[str] = None
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    change_percent: Optional[float] = None
    message: str = ""
    vendor_ingredient_id: Optional[int] = None


@dataclass
class UpsertResult:
    """Result from upserting a vendor ingredient."""
    vendor_ingredient_id: int
    is_new: bool
    was_stale: bool = False  # True if reactivated from stale
    changed_fields: Dict[str, Tuple] = field(default_factory=dict)  # field → (old, new)


# Database configuration
DATABASE_FILE = "ingredients.db"  # SQLite fallback
USE_POSTGRES = True  # Set to False to force SQLite


def get_postgres_url() -> Optional[str]:
    """Get PostgreSQL connection URL from environment."""
    return os.environ.get('DATABASE_URL')


# =============================================================================
# Database Connection Wrapper
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

    @property
    def connection(self):
        """Get the underlying database connection."""
        return self._conn

    def connect(self):
        """Establish database connection."""
        self.postgres_url = get_postgres_url()
        if USE_POSTGRES and HAS_POSTGRES and self.postgres_url:
            try:
                self._conn = psycopg2.connect(self.postgres_url)
                self._is_postgres = True
                print("  Connected to PostgreSQL", flush=True)
            except Exception as e:
                print(f"  PostgreSQL connection failed: {e}", flush=True)
                print("  Falling back to SQLite...", flush=True)
                self._conn = sqlite3.connect(self.db_path)
                self._conn.row_factory = sqlite3.Row
                self._is_postgres = False
        else:
            if not HAS_POSTGRES:
                print("  (psycopg2 not installed, using SQLite)", flush=True)
            elif not self.postgres_url:
                print("  (DATABASE_URL not set, using SQLite)", flush=True)
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            self._is_postgres = False
            print(f"  Connected to SQLite: {self.db_path}", flush=True)
        return self._conn

    def reconnect(self):
        """Reconnect to database after connection loss."""
        print("  🔄 Reconnecting to database...", flush=True)
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
            print("  ✓ Database reconnected (PostgreSQL)", flush=True)
        else:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            print(f"  ✓ Database reconnected (SQLite: {self.db_path})", flush=True)
        return self._conn

    def close(self):
        """Close the database connection."""
        if self._conn:
            try:
                self._conn.close()
            except:
                pass
            self._conn = None

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
        """
        Execute a database function with automatic reconnection on failure.

        Args:
            func: Function to execute (should take conn as first argument)
            *args: Additional arguments to pass to func
            max_retries: Maximum number of reconnection attempts
            **kwargs: Keyword arguments to pass to func

        Returns:
            Result of func
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                return func(self._conn, *args, **kwargs)
            except Exception as e:
                last_error = e
                if self.is_connection_error(e):
                    if attempt < max_retries - 1:
                        print(f"  ⚠ Database error: {e}", flush=True)
                        self.reconnect()
                        time.sleep(1)  # Brief pause before retry
                    else:
                        raise
                else:
                    # Non-connection error, don't retry
                    raise
        raise last_error


def is_postgres(conn) -> bool:
    """Check if connection is PostgreSQL."""
    return HAS_POSTGRES and hasattr(conn, 'info')


def db_placeholder(conn) -> str:
    """Return the correct placeholder for the database type."""
    return '%s' if is_postgres(conn) else '?'


# Non-ingredient products to skip
SKIP_PRODUCTS = [
    'shipping insurance',
    'shipping protection',
    'gift card',
    'extra fee',
    'deposit',
    'bottle caps',
    'bottles case',
    'white bottles',
]

# Only scrape products from BoxNutra (skip third-party marketplace vendors)
ALLOWED_VENDORS = ['boxnutra']

# Track skipped products for logging
skipped_products = []


def should_skip_product(title: str, vendor: str, url: str) -> tuple[bool, str]:
    """
    Check if product should be skipped.
    Returns (should_skip, reason) tuple.
    """
    title_lower = title.lower()
    vendor_lower = vendor.lower() if vendor else ''

    # Check non-ingredient products
    for skip_term in SKIP_PRODUCTS:
        if skip_term in title_lower:
            return True, f"non-ingredient ({skip_term})"

    # Check third-party vendors
    if vendor_lower not in ALLOWED_VENDORS:
        return True, f"third-party vendor ({vendor})"

    return False, ""


def log_skipped_product(title: str, vendor: str, url: str, reason: str) -> None:
    """Log a skipped product for review."""
    skipped_products.append({
        'title': title,
        'vendor': vendor,
        'url': url,
        'reason': reason,
        'timestamp': datetime.now().isoformat()
    })


def extract_availability_from_html(html: str) -> Dict[int, bool]:
    """
    Extract variant availability from HTML's embedded Shopify product data.

    BoxNutra's JSON API returns null for 'available' field, but the HTML
    contains the data in embedded JavaScript objects with the structure:
    {"id":12345,...,"available":true,...}

    Returns dict mapping variant_id to availability (True = in stock).
    """
    availability = {}

    try:
        # Find all variant-like JSON objects with both id and available fields
        # Pattern matches: {"id":12345,...,"available":true,...}
        variant_pattern = r'\{"id":(\d+),[^}]*?"available":(true|false)[^}]*?\}'
        matches = re.findall(variant_pattern, html)

        for vid, avail in matches:
            variant_id = int(vid)
            is_available = avail == 'true'
            availability[variant_id] = is_available

    except Exception:
        pass

    return availability


# =============================================================================
# Parsing Functions
# =============================================================================

def calculate_price_per_kg(price: float, grams: int) -> float:
    """Calculate price per kg from price and grams."""
    if not grams or grams <= 0:
        return 0
    return (price / grams) * 1000


def convert_stock_status(available: bool) -> str:
    """Convert boolean available to string status."""
    if available is None:
        return 'unknown'
    return 'in_stock' if available else 'out_of_stock'


def format_product_details(rows: List[Dict], verbose: bool = True) -> str:
    """Format product details as a table for console output."""
    if not rows or not verbose:
        return ""

    lines = []
    lines.append(f"    {'Packaging':<30} {'Size':>8} {'Price':>10} {'$/kg':>10} {'Stock':<10}")
    lines.append(f"    {'-'*30} {'-'*8} {'-'*10} {'-'*10} {'-'*10}")

    sorted_rows = sorted(rows, key=lambda r: r.get('pack_size_g', 0) or 0)

    for row in sorted_rows:
        packaging = row.get('packaging', 'N/A')
        if len(packaging) > 30:
            packaging = packaging[:28] + '..'

        packaging_kg = row.get('packaging_kg')
        size_str = f"{packaging_kg}kg" if packaging_kg else '-'

        price = row.get('price', 0)
        try:
            price_val = float(price) if price else 0
        except:
            price_val = 0

        price_per_kg = row.get('price_per_kg', 0) or 0
        stock_status = row.get('stock_status', 'unknown')

        lines.append(f"    {packaging:<30} {size_str:>8} {f'${price_val:,.2f}':>10} {f'${price_per_kg:,.2f}':>10} {stock_status:<10}")

    return '\n'.join(lines)


# =============================================================================
# Progress Tracking
# =============================================================================

class ProgressTracker:
    """Track scraping progress with rate calculation."""

    def __init__(self, total: int):
        self.total = total
        self.processed = 0
        self.start_time = time.time()

    def update(self, count: int = 1):
        self.processed += count

    def get_rate(self) -> float:
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            return self.processed / elapsed
        return 0

    def get_eta(self) -> str:
        rate = self.get_rate()
        if rate > 0:
            remaining = self.total - self.processed
            seconds = remaining / rate
            if seconds < 60:
                return f"{int(seconds)}s"
            elif seconds < 3600:
                return f"{int(seconds/60)}:{int(seconds%60):02d}"
            else:
                hours = int(seconds / 3600)
                minutes = int((seconds % 3600) / 60)
                return f"{hours}:{minutes:02d}:00"
        return "calculating..."

    def format_progress(self, handle: str, status: str) -> str:
        timestamp = datetime.now().strftime("%H:%M:%S")
        pct = (self.processed / self.total * 100) if self.total > 0 else 0
        rate = self.get_rate()
        eta = self.get_eta()
        return f"[{timestamp}] [{self.processed}/{self.total}] ({pct:5.1f}%) {handle:<45} [{status}] | {rate:.1f}/s | ETA: {eta}"


# =============================================================================
# Checkpoint Functions
# =============================================================================

def save_checkpoint(processed_handles: List[str], all_handles: List[str], data: List[Dict]) -> None:
    """Save checkpoint for resume capability."""
    checkpoint = {
        'processed_handles': processed_handles,
        'all_handles': all_handles,
        'data': data,
        'timestamp': datetime.now().isoformat()
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f)


def load_checkpoint() -> Optional[Dict]:
    """Load checkpoint if it exists."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return None
    return None


def clear_checkpoint() -> None:
    """Remove checkpoint file after successful completion."""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("Cleared checkpoint file")


# =============================================================================
# HTTP Fetch with Exponential Backoff
# =============================================================================

def fetch_with_backoff(url: str, session: requests.Session, log_slow: bool = True) -> Optional[Dict]:
    """Fetch JSON URL with exponential backoff retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            start_time = time.time()
            response = session.get(url, headers=HEADERS, timeout=30)
            elapsed = time.time() - start_time

            # Log slow requests (> 5 seconds)
            if log_slow and elapsed > 5:
                print(f"    [SLOW] JSON fetch took {elapsed:.1f}s", flush=True)

            if response.status_code == 429:
                delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                print(f"    [RATE-LIMITED] 429 response, backoff {delay}s (attempt {attempt+1}/{MAX_RETRIES})", flush=True)
                time.sleep(delay)
                continue

            if response.status_code >= 400:
                print(f"    [HTTP-ERROR] Status {response.status_code} (attempt {attempt+1}/{MAX_RETRIES})", flush=True)

            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
            print(f"    [TIMEOUT] Request timed out after 30s, retry in {delay}s (attempt {attempt+1}/{MAX_RETRIES})", flush=True)
            time.sleep(delay)

        except requests.exceptions.ConnectionError as e:
            delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
            print(f"    [CONN-ERROR] {str(e)[:50]}, retry in {delay}s (attempt {attempt+1}/{MAX_RETRIES})", flush=True)
            time.sleep(delay)

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"    [FAILED] {str(e)[:80]}", flush=True)
                return None
            delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
            print(f"    [ERROR] {str(e)[:50]}, retry in {delay}s (attempt {attempt+1}/{MAX_RETRIES})", flush=True)
            time.sleep(delay)

    return None


# =============================================================================
# Product Discovery
# =============================================================================

def discover_products(session: requests.Session) -> List[str]:
    """Discover all product handles using /products.json pagination."""
    all_handles = []
    page_num = 1
    limit = 250

    print("Discovering products...", flush=True)

    while True:
        url = f"{BASE_URL}/products.json?page={page_num}&limit={limit}"
        print(f"  Page {page_num}: ", end='', flush=True)

        try:
            data = fetch_with_backoff(url, session)

            if not data:
                print("failed to fetch", flush=True)
                break

            products = data.get('products', [])

            if not products:
                print("no more products", flush=True)
                break

            page_handles = [p.get('handle') for p in products if p.get('handle')]
            all_handles.extend(page_handles)

            print(f"found {len(products)} products (total: {len(all_handles)})", flush=True)

            page_num += 1
            time.sleep(REQUEST_DELAY)

        except Exception as e:
            print(f"error: {e}", flush=True)
            break

    # Remove duplicates while preserving order
    seen = set()
    unique_handles = []
    for h in all_handles:
        if h not in seen:
            seen.add(h)
            unique_handles.append(h)

    print(f"\nDiscovered {len(unique_handles)} unique products", flush=True)
    return unique_handles


# =============================================================================
# Product Scraping
# =============================================================================

def parse_product(product_data: Dict, availability: Dict[int, bool] = None) -> List[Dict]:
    """Parse a product's JSON data into rows (one per variant).

    Args:
        product_data: Raw product JSON from Shopify API
        availability: Optional dict mapping variant_id to availability from HTML
    """
    rows = []
    timestamp = datetime.now().isoformat()

    try:
        product = product_data.get('product', {})

        product_id = product.get('id')
        title = product.get('title', 'Unknown')
        handle = product.get('handle', '')
        vendor = product.get('vendor', 'BoxNutra')
        product_url = f"{BASE_URL}/products/{handle}"

        # Skip non-ingredient products and third-party vendors
        should_skip, skip_reason = should_skip_product(title, vendor, product_url)
        if should_skip:
            log_skipped_product(title, vendor, product_url, skip_reason)
            return []

        variants = product.get('variants', [])

        for variant in variants:
            option1 = variant.get('option1', '')  # Size in BoxNutra
            sku = variant.get('sku', '')
            variant_id = variant.get('id')

            # Direct fields from JSON
            grams = variant.get('grams', 0) or 0

            # Get availability: prefer HTML data, fall back to JSON (usually null)
            if availability and variant_id in availability:
                is_available = availability[variant_id]
            else:
                is_available = variant.get('available')

            # Calculate derived values
            packaging_kg = round(grams / 1000, 4) if grams > 0 else None

            try:
                price_val = float(variant.get('price', 0))
            except:
                price_val = 0

            price_per_kg = calculate_price_per_kg(price_val, grams)

            row = {
                'product_id': product_id,
                'product_title': title,
                'vendor': vendor,
                'variant_id': variant_id,
                'variant_sku': sku,
                'packaging': option1,
                'packaging_kg': packaging_kg,
                'pack_size_g': grams,
                'price': variant.get('price', ''),
                'compare_at_price': variant.get('compare_at_price', ''),
                'price_per_kg': round(price_per_kg, 2) if price_per_kg else None,
                'available': is_available,
                'stock_status': convert_stock_status(is_available),
                'order_rule_type': BOXNUTRA_BUSINESS_MODEL['order_rule_type'],
                'shipping_responsibility': BOXNUTRA_BUSINESS_MODEL['shipping_responsibility'],
                'url': product_url,
                'scraped_at': timestamp,
            }
            rows.append(row)

        return rows

    except Exception:
        return []


def scrape_product(handle: str, session: requests.Session) -> List[Dict]:
    """Scrape a single product - JSON + HTML for availability."""
    json_url = f"{BASE_URL}/products/{handle}.json"
    html_url = f"{BASE_URL}/products/{handle}"

    # Fetch JSON for product data
    product_data = fetch_with_backoff(json_url, session)
    if not product_data:
        return []

    # Fetch HTML for availability (JSON API returns null for 'available')
    availability = {}
    try:
        start_time = time.time()
        html_response = session.get(html_url, headers=HEADERS, timeout=30)
        elapsed = time.time() - start_time

        # Log slow HTML fetches
        if elapsed > 5:
            print(f"    [SLOW] HTML fetch took {elapsed:.1f}s", flush=True)

        if html_response.status_code == 200:
            availability = extract_availability_from_html(html_response.text)
        elif html_response.status_code == 429:
            print(f"    [RATE-LIMITED] HTML fetch got 429", flush=True)
        elif html_response.status_code >= 400:
            print(f"    [HTTP-ERROR] HTML fetch got {html_response.status_code}", flush=True)

    except requests.exceptions.Timeout:
        print(f"    [TIMEOUT] HTML fetch timed out after 30s", flush=True)
    except requests.exceptions.ConnectionError as e:
        print(f"    [CONN-ERROR] HTML fetch failed: {str(e)[:50]}", flush=True)
    except Exception as e:
        print(f"    [ERROR] HTML fetch failed: {str(e)[:50]}", flush=True)

    return parse_product(product_data, availability)


# =============================================================================
# CSV Output
# =============================================================================

def save_to_csv(data: List[Dict], output_dir: str = "output") -> str:
    """Save scraped data to a timestamped CSV file."""
    if not data:
        print("No data to save")
        return ""

    df = pd.DataFrame(data)

    # Reorder columns
    priority_cols = [
        'product_id', 'product_title', 'vendor',
        'variant_id', 'variant_sku',
        'packaging', 'packaging_kg', 'pack_size_g',
        'price', 'compare_at_price', 'price_per_kg',
        'available', 'stock_status',
        'order_rule_type', 'shipping_responsibility',
        'url', 'scraped_at'
    ]
    other_cols = [c for c in df.columns if c not in priority_cols]
    ordered_cols = [c for c in priority_cols if c in df.columns] + other_cols
    df = df[ordered_cols]

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"boxnutra_products_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False)
    print(f"\nSaved {len(df)} rows to {filepath}")

    return filepath


# =============================================================================
# Database Functions
# =============================================================================

def init_boxnutra_tables(conn) -> None:
    """Ensure BoxNutra vendor exists in database."""
    cursor = conn.cursor()

    if is_postgres(conn):
        # Ensure BoxNutra vendor exists in Vendors table
        try:
            cursor.execute('''
                INSERT INTO vendors (name, pricing_model, status)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO NOTHING
            ''', ('BoxNutra', 'per_package', 'active'))
            conn.commit()
        except Exception:
            conn.rollback()

    else:
        # SQLite: Create Vendors table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Vendors (
                vendor_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                pricing_model TEXT,
                status TEXT DEFAULT 'active'
            )
        ''')

        cursor.execute('''
            INSERT OR IGNORE INTO Vendors (name, pricing_model, status)
            VALUES (?, ?, ?)
        ''', ('BoxNutra', 'per_package', 'active'))

    conn.commit()
    print("  BoxNutra vendor initialized", flush=True)


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
                             sku: str, raw_name: str, source_id: int) -> UpsertResult:
    """Insert or update vendor ingredient, return UpsertResult with tracking info."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    now = datetime.now().isoformat()

    # Check if exists and get current status for reactivation detection
    cursor.execute(
        f'''SELECT vendor_ingredient_id, status, stale_since FROM vendoringredients
           WHERE vendor_id = {ph} AND variant_id = {ph} AND sku = {ph}''',
        (vendor_id, variant_id, sku)
    )
    row = cursor.fetchone()

    if row:
        vendor_ingredient_id = row[0]
        old_status = row[1] if row[1] else 'active'
        stale_since = row[2]

        # Check if reactivating from stale
        was_stale = old_status == 'stale'

        # Update - reactivate if stale, clear stale_since
        cursor.execute(
            f'''UPDATE vendoringredients SET raw_product_name = {ph},
               shipping_responsibility = {ph}, current_source_id = {ph},
               last_seen_at = {ph}, status = 'active', stale_since = NULL
               WHERE vendor_ingredient_id = {ph}''',
            (raw_name, BOXNUTRA_BUSINESS_MODEL['shipping_responsibility'],
             source_id, now, vendor_ingredient_id)
        )

        return UpsertResult(
            vendor_ingredient_id=vendor_ingredient_id,
            is_new=False,
            was_stale=was_stale,
            changed_fields={'stale_since': (stale_since, None)} if was_stale else {}
        )

    # Insert new record
    if is_postgres(conn):
        cursor.execute(
            f'''INSERT INTO vendoringredients
               (vendor_id, variant_id, sku, raw_product_name, shipping_responsibility,
                current_source_id, last_seen_at, status)
               VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, 'active')
               RETURNING vendor_ingredient_id''',
            (vendor_id, variant_id, sku, raw_name,
             BOXNUTRA_BUSINESS_MODEL['shipping_responsibility'], source_id, now)
        )
        vendor_ingredient_id = cursor.fetchone()[0]
    else:
        cursor.execute(
            f'''INSERT INTO vendoringredients
               (vendor_id, variant_id, sku, raw_product_name, shipping_responsibility,
                current_source_id, last_seen_at, status)
               VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, 'active')''',
            (vendor_id, variant_id, sku, raw_name,
             BOXNUTRA_BUSINESS_MODEL['shipping_responsibility'], source_id, now)
        )
        vendor_ingredient_id = cursor.lastrowid

    return UpsertResult(
        vendor_ingredient_id=vendor_ingredient_id,
        is_new=True,
        was_stale=False
    )


def get_existing_price(conn, vendor_ingredient_id: int) -> Optional[float]:
    """Get the most recent price for a vendor ingredient (for comparison)."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(
        f'''SELECT price FROM pricetiers
           WHERE vendor_ingredient_id = {ph}
           ORDER BY effective_date DESC LIMIT 1''',
        (vendor_ingredient_id,)
    )
    row = cursor.fetchone()
    return float(row[0]) if row and row[0] else None


def delete_old_price_tiers(conn, vendor_ingredient_id: int) -> None:
    """Delete existing price tiers for a vendor ingredient."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(f'DELETE FROM pricetiers WHERE vendor_ingredient_id = {ph}', (vendor_ingredient_id,))


def insert_price_tier(conn, vendor_ingredient_id: int, row_data: dict, source_id: int) -> None:
    """Insert price tier record for BoxNutra (per_package pricing)."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)

    # Get g unit_id
    cursor.execute(f'SELECT unit_id FROM units WHERE name = {ph}', ('g',))
    unit_row = cursor.fetchone()
    unit_id = unit_row[0] if unit_row else None

    # Get per_package pricing model id
    cursor.execute(f'SELECT model_id FROM pricingmodels WHERE name = {ph}', ('per_package',))
    model_row = cursor.fetchone()
    pricing_model_id = model_row[0] if model_row else 2

    # Parse price
    try:
        price = float(row_data.get('price', 0) or 0)
    except (ValueError, TypeError):
        price = 0

    # Parse compare_at_price
    original_price = None
    compare_at = row_data.get('compare_at_price')
    if compare_at:
        try:
            original_price = float(compare_at)
        except (ValueError, TypeError):
            pass

    # Calculate discount percent
    discount_percent = 0
    if original_price and original_price > price:
        discount_percent = ((original_price - price) / original_price) * 100

    cursor.execute(
        f'''INSERT INTO pricetiers
           (vendor_ingredient_id, pricing_model_id, unit_id, source_id, min_quantity,
            price, original_price, discount_percent, price_per_kg, effective_date, includes_shipping)
           VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})''',
        (vendor_ingredient_id, pricing_model_id, unit_id, source_id,
         row_data.get('pack_size_g', 0),
         price,
         original_price,
         discount_percent,
         row_data.get('price_per_kg'),
         row_data.get('scraped_at', datetime.now().isoformat()),
         1)
    )


def upsert_packaging_size(conn, vendor_ingredient_id: int, pack_size_g: float, description: str) -> None:
    """Insert or update packaging size."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)

    # Get g unit_id
    cursor.execute(f'SELECT unit_id FROM units WHERE name = {ph}', ('g',))
    unit_row = cursor.fetchone()
    unit_id = unit_row[0] if unit_row else None

    # Delete existing and insert new
    cursor.execute(f'DELETE FROM packagingsizes WHERE vendor_ingredient_id = {ph}', (vendor_ingredient_id,))
    cursor.execute(
        f'''INSERT INTO packagingsizes (vendor_ingredient_id, unit_id, description, quantity)
           VALUES ({ph}, {ph}, {ph}, {ph})''',
        (vendor_ingredient_id, unit_id, description, pack_size_g)
    )


def upsert_order_rule(conn, vendor_ingredient_id: int, pack_size_g: float, scraped_at: str) -> None:
    """Insert or update order rule for BoxNutra fixed_pack."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)

    # Get rule type id for fixed_pack
    cursor.execute(f'SELECT type_id FROM orderruletypes WHERE name = {ph}', ('fixed_pack',))
    type_row = cursor.fetchone()
    rule_type_id = type_row[0] if type_row else 2

    # Get g unit_id
    cursor.execute(f'SELECT unit_id FROM units WHERE name = {ph}', ('g',))
    unit_row = cursor.fetchone()
    unit_id = unit_row[0] if unit_row else None

    # Delete existing and insert new
    cursor.execute(f'DELETE FROM orderrules WHERE vendor_ingredient_id = {ph}', (vendor_ingredient_id,))
    cursor.execute(
        f'''INSERT INTO orderrules
           (vendor_ingredient_id, rule_type_id, unit_id, base_quantity, min_quantity, effective_date)
           VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})''',
        (vendor_ingredient_id, rule_type_id, unit_id, pack_size_g, pack_size_g, scraped_at)
    )


def get_existing_stock_status(conn, vendor_ingredient_id: int) -> Optional[str]:
    """Get the existing stock status for a vendor ingredient (for comparison)."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(
        f'''SELECT stock_status FROM vendorinventory
           WHERE vendor_ingredient_id = {ph}''',
        (vendor_ingredient_id,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


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


def mark_stale_variants(conn, vendor_id: int, scrape_start_time: str,
                        stats: Optional['StatsTracker'] = None) -> List[Dict]:
    """Mark variants not seen in this scrape as stale (soft-delete).

    Call this after a FULL scrape (not --max-products) to detect products
    that have been removed from the vendor's site.

    Returns list of stale variant info for reporting.
    """
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    now = datetime.now().isoformat()

    # First SELECT variants that will become stale (for reporting)
    cursor.execute(
        f'''SELECT vendor_ingredient_id, sku, raw_product_name, last_seen_at
           FROM vendoringredients
           WHERE vendor_id = {ph}
           AND status = 'active'
           AND (last_seen_at IS NULL OR last_seen_at < {ph})''',
        (vendor_id, scrape_start_time)
    )
    stale_rows = cursor.fetchall()

    stale_variants = []
    for row in stale_rows:
        stale_variants.append({
            'vendor_ingredient_id': row[0],
            'sku': row[1],
            'product_name': row[2],
            'last_seen_at': str(row[3]) if row[3] else None
        })

    if not stale_variants:
        return []

    # Update to stale with stale_since timestamp
    cursor.execute(
        f'''UPDATE vendoringredients
           SET status = 'stale', stale_since = {ph}
           WHERE vendor_id = {ph}
           AND status = 'active'
           AND (last_seen_at IS NULL OR last_seen_at < {ph})''',
        (now, vendor_id, scrape_start_time)
    )

    # Record in stats if provided
    if stats:
        for v in stale_variants:
            stats.record_stale(
                sku=v['sku'],
                name=v['product_name'],
                last_seen_at=v['last_seen_at'],
                vendor_ingredient_id=v['vendor_ingredient_id']
            )

    print(f"  Marked {len(stale_variants)} variants as stale (soft-deleted)")
    return stale_variants


def mark_missing_variants_for_product(conn, vendor_id: int, variant_id: int,
                                       seen_skus: List[str], scrape_time: str) -> int:
    """Mark variants of this product that weren't in current scrape as stale."""
    if not seen_skus:
        return 0

    cursor = conn.cursor()
    ph = db_placeholder(conn)
    now = datetime.now().isoformat()

    # Mark variants for this product NOT in seen_skus as stale
    placeholders = ','.join([ph] * len(seen_skus))
    cursor.execute(
        f'''UPDATE vendoringredients
           SET status = 'stale', stale_since = {ph}
           WHERE vendor_id = {ph}
           AND variant_id = {ph}
           AND sku NOT IN ({placeholders})
           AND status = 'active' ''',
        (now, vendor_id, variant_id, *seen_skus)
    )

    return cursor.rowcount


def save_to_relational_tables(conn, rows: List[Dict],
                               stats: Optional['StatsTracker'] = None) -> None:
    """Save processed product rows to the relational tables with change tracking."""
    if not rows:
        return

    cursor = conn.cursor()
    ph = db_placeholder(conn)

    # Get vendor_id for BoxNutra
    cursor.execute(f'SELECT vendor_id FROM vendors WHERE name = {ph}', ('BoxNutra',))
    vendor_row = cursor.fetchone()
    if not vendor_row:
        print("  Warning: BoxNutra vendor not found, skipping relational tables")
        return
    vendor_id = vendor_row[0]

    # All rows for same product share same base info
    first_row = rows[0]
    product_title = first_row.get('product_title', '')
    url = first_row.get('url', '')
    scraped_at = first_row.get('scraped_at', datetime.now().isoformat())

    # Create source record
    source_id = insert_scrape_source(conn, vendor_id, url, scraped_at)

    # Create category (BoxNutra doesn't provide categories, use None)
    category_id = None

    # Create ingredient using product title
    ingredient_id = get_or_create_ingredient(conn, product_title, category_id)

    # Create manufacturer (BoxNutra is both vendor and manufacturer)
    manufacturer_id = get_or_create_manufacturer(conn, 'BoxNutra')

    # Create variant
    variant_id = get_or_create_variant(conn, ingredient_id, manufacturer_id, product_title)

    # Process each variant row (different pack sizes)
    seen_skus = []
    for row in rows:
        sku = row.get('variant_sku', '')
        seen_skus.append(sku)
        pack_size_g = row.get('pack_size_g', 0)
        pack_description = row.get('packaging', '')
        stock_status = row.get('stock_status', 'unknown')

        # Parse new price
        try:
            new_price = float(row.get('price', 0) or 0)
        except (ValueError, TypeError):
            new_price = 0

        # Create/update vendor ingredient (returns UpsertResult with tracking info)
        upsert_result = upsert_vendor_ingredient(
            conn, vendor_id, variant_id, sku, product_title, source_id
        )
        vendor_ingredient_id = upsert_result.vendor_ingredient_id

        # Track new product or reactivation
        if stats:
            if upsert_result.is_new:
                stats.record_new_product(sku, product_title, vendor_ingredient_id)
            elif upsert_result.was_stale:
                stale_since = upsert_result.changed_fields.get('stale_since', (None, None))[0]
                stats.record_reactivated(sku, product_title, str(stale_since) if stale_since else None, vendor_ingredient_id)

        # Get existing price BEFORE deleting (for change tracking)
        old_price = get_existing_price(conn, vendor_ingredient_id)

        # Delete old price tier and insert new
        delete_old_price_tiers(conn, vendor_ingredient_id)
        insert_price_tier(conn, vendor_ingredient_id, row, source_id)

        # Track price changes (>30% threshold)
        if stats and old_price is not None and new_price > 0 and new_price != old_price:
            stats.record_price_change(sku, product_title, old_price, new_price, vendor_ingredient_id)

        # Get existing stock status BEFORE upserting (for change tracking)
        old_stock_status = get_existing_stock_status(conn, vendor_ingredient_id)

        # Insert packaging info
        upsert_packaging_size(conn, vendor_ingredient_id, pack_size_g, pack_description)

        # Insert order rule
        upsert_order_rule(conn, vendor_ingredient_id, pack_size_g, scraped_at)

        # Insert inventory status
        upsert_inventory_simple(conn, vendor_ingredient_id, stock_status, source_id)

        # Track stock status changes (in_stock → out_of_stock only)
        if stats and old_stock_status is not None:
            was_in_stock = old_stock_status == 'in_stock'
            is_in_stock = stock_status == 'in_stock'
            if was_in_stock and not is_in_stock:
                stats.record_stock_change(sku, product_title, was_in_stock, is_in_stock, vendor_ingredient_id)

        # Track updated vs unchanged
        if stats and not upsert_result.is_new and not upsert_result.was_stale:
            # Check if anything changed (price or stock status)
            price_changed = old_price is not None and new_price > 0 and old_price != new_price
            stock_changed = old_stock_status is not None and old_stock_status != stock_status
            if price_changed or stock_changed:
                stats.record_updated()
            else:
                stats.record_unchanged()

    # Mark variants not in this batch as stale (variant-level staleness)
    mark_missing_variants_for_product(conn, vendor_id, variant_id, seen_skus, scraped_at)


def save_to_database(db_conn: DatabaseConnection, data: List[Dict],
                     stats: Optional['StatsTracker'] = None) -> int:
    """
    Save BoxNutra data to relational database tables.
    Returns number of rows saved.
    """
    def _save(conn, rows):
        saved = 0

        # Save to relational tables (grouped by product)
        products = {}
        for row in rows:
            pid = row.get('product_id')
            if pid not in products:
                products[pid] = []
            products[pid].append(row)

        for pid, product_rows in products.items():
            save_to_relational_tables(conn, product_rows, stats)
            saved += len(product_rows)

        conn.commit()
        return saved

    return db_conn.execute_with_retry(_save, data)


def save_skipped_log(output_dir: str = "output") -> str:
    """Save skipped products log for review."""
    if not skipped_products:
        return ""

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"boxnutra_skipped_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)

    # Group by reason for summary
    by_reason = {}
    for item in skipped_products:
        reason = item['reason']
        if reason not in by_reason:
            by_reason[reason] = []
        by_reason[reason].append(item)

    # Create log structure
    log_data = {
        'generated_at': datetime.now().isoformat(),
        'total_skipped': len(skipped_products),
        'summary': {reason: len(items) for reason, items in by_reason.items()},
        'skipped_products': skipped_products
    }

    with open(filepath, 'w') as f:
        json.dump(log_data, f, indent=2)

    print(f"Skipped {len(skipped_products)} products -> {filepath}")

    # Print summary
    print("  Skip reasons:")
    for reason, count in log_data['summary'].items():
        print(f"    - {reason}: {count}")

    return filepath


# =============================================================================
# Statistics Tracker
# =============================================================================

from datetime import timedelta

class StatsTracker:
    """
    Track scraping statistics and alerts for reporting.
    Collects metrics during scrape, then persists to DB and prints report at end.
    """

    def __init__(self, vendor_id: int, is_full_scrape: bool = True, max_products_limit: Optional[int] = None):
        self.vendor_id = vendor_id
        self.is_full_scrape = is_full_scrape
        self.max_products_limit = max_products_limit
        self.started_at = datetime.now()
        self.completed_at: Optional[datetime] = None

        # Counters
        self.products_discovered = 0
        self.products_processed = 0
        self.products_skipped = 0
        self.products_failed = 0
        self.variants_new = 0
        self.variants_updated = 0
        self.variants_unchanged = 0
        self.variants_stale = 0
        self.variants_reactivated = 0

        # Alerts (in-memory during scrape)
        self.alerts: List[Alert] = []

        # Run ID (set after persisting to ScrapeRuns)
        self.run_id: Optional[int] = None

    def record_new_product(self, sku: str, name: str, vendor_ingredient_id: Optional[int] = None):
        """Record a new product being added to the database."""
        self.variants_new += 1
        self.alerts.append(Alert(
            alert_type=AlertType.NEW_PRODUCT,
            severity=ALERT_SEVERITY[AlertType.NEW_PRODUCT],
            sku=sku,
            product_name=name,
            message=f"New product: {name}",
            vendor_ingredient_id=vendor_ingredient_id
        ))

    def record_reactivated(self, sku: str, name: str, stale_since: Optional[str] = None,
                           vendor_ingredient_id: Optional[int] = None):
        """Record a stale product being reactivated."""
        self.variants_reactivated += 1
        msg = f"Reactivated: {name}"
        if stale_since:
            msg += f" (was stale since {stale_since})"
        self.alerts.append(Alert(
            alert_type=AlertType.REACTIVATED,
            severity=ALERT_SEVERITY[AlertType.REACTIVATED],
            sku=sku,
            product_name=name,
            old_value=stale_since,
            message=msg,
            vendor_ingredient_id=vendor_ingredient_id
        ))

    def record_price_change(self, sku: str, name: str, old_price: float, new_price: float,
                            vendor_ingredient_id: Optional[int] = None):
        """Record a price change if it exceeds 30% threshold."""
        if old_price <= 0:
            return

        change_pct = ((new_price - old_price) / old_price) * 100

        if change_pct <= -30:
            # Major price decrease
            self.alerts.append(Alert(
                alert_type=AlertType.PRICE_DECREASE_MAJOR,
                severity=ALERT_SEVERITY[AlertType.PRICE_DECREASE_MAJOR],
                sku=sku,
                product_name=name,
                old_value=f"${old_price:.2f}",
                new_value=f"${new_price:.2f}",
                change_percent=change_pct,
                message=f"Price dropped {change_pct:.1f}%: ${old_price:.2f} → ${new_price:.2f}",
                vendor_ingredient_id=vendor_ingredient_id
            ))
        elif change_pct >= 30:
            # Major price increase
            self.alerts.append(Alert(
                alert_type=AlertType.PRICE_INCREASE_MAJOR,
                severity=ALERT_SEVERITY[AlertType.PRICE_INCREASE_MAJOR],
                sku=sku,
                product_name=name,
                old_value=f"${old_price:.2f}",
                new_value=f"${new_price:.2f}",
                change_percent=change_pct,
                message=f"Price increased {change_pct:.1f}%: ${old_price:.2f} → ${new_price:.2f}",
                vendor_ingredient_id=vendor_ingredient_id
            ))

    def record_stock_change(self, sku: str, name: str, was_in_stock: bool, is_in_stock: bool,
                            vendor_ingredient_id: Optional[int] = None):
        """Record stock status change (only in_stock → out_of_stock)."""
        if was_in_stock and not is_in_stock:
            self.alerts.append(Alert(
                alert_type=AlertType.STOCK_OUT,
                severity=ALERT_SEVERITY[AlertType.STOCK_OUT],
                sku=sku,
                product_name=name,
                old_value="in_stock",
                new_value="out_of_stock",
                message=f"Stock out: {name}",
                vendor_ingredient_id=vendor_ingredient_id
            ))

    def record_unchanged(self):
        """Record an unchanged variant."""
        self.variants_unchanged += 1

    def record_updated(self):
        """Record an updated variant."""
        self.variants_updated += 1

    def record_stale(self, sku: str, name: str, last_seen_at: Optional[str] = None,
                     vendor_ingredient_id: Optional[int] = None):
        """Record a variant being marked as stale (soft-deleted)."""
        self.variants_stale += 1
        self.alerts.append(Alert(
            alert_type=AlertType.STALE_VARIANT,
            severity=ALERT_SEVERITY[AlertType.STALE_VARIANT],
            sku=sku,
            product_name=name,
            old_value=last_seen_at,
            message=f"Stale: {name} (last seen: {last_seen_at or 'unknown'})",
            vendor_ingredient_id=vendor_ingredient_id
        ))

    def record_parse_failure(self, sku: Optional[str], name: Optional[str], field: str, raw_value: str):
        """Record a parse failure for a field."""
        self.alerts.append(Alert(
            alert_type=AlertType.PARSE_FAILURE,
            severity=ALERT_SEVERITY[AlertType.PARSE_FAILURE],
            sku=sku,
            product_name=name,
            old_value=raw_value,
            message=f"Parse failure for '{field}': {raw_value[:50]}"
        ))

    def record_missing_required(self, sku: Optional[str], name: Optional[str], field: str):
        """Record a missing required field."""
        self.alerts.append(Alert(
            alert_type=AlertType.MISSING_REQUIRED,
            severity=ALERT_SEVERITY[AlertType.MISSING_REQUIRED],
            sku=sku,
            product_name=name,
            message=f"Missing required field: {field}"
        ))

    def record_failure(self, slug: str, error_type: str, error_msg: str):
        """Record a scraping failure (HTTP or DB error)."""
        self.products_failed += 1
        alert_type = AlertType.HTTP_ERROR if error_type == "HTTP" else AlertType.DB_ERROR
        self.alerts.append(Alert(
            alert_type=alert_type,
            severity=ALERT_SEVERITY[alert_type],
            sku=slug,
            message=f"[{error_type}] {slug}: {error_msg}"
        ))

    def get_alert_counts(self) -> Dict[str, int]:
        """Get counts of each alert type."""
        counts: Dict[str, int] = {}
        for alert in self.alerts:
            key = alert.alert_type.value
            counts[key] = counts.get(key, 0) + 1
        return counts

    def get_alerts_by_type(self, alert_type: AlertType) -> List[Alert]:
        """Get all alerts of a specific type."""
        return [a for a in self.alerts if a.alert_type == alert_type]

    def to_checkpoint_dict(self) -> Dict:
        """Serialize stats for checkpoint."""
        return {
            'vendor_id': self.vendor_id,
            'is_full_scrape': self.is_full_scrape,
            'max_products_limit': self.max_products_limit,
            'started_at': self.started_at.isoformat(),
            'products_discovered': self.products_discovered,
            'products_processed': self.products_processed,
            'products_skipped': self.products_skipped,
            'products_failed': self.products_failed,
            'variants_new': self.variants_new,
            'variants_updated': self.variants_updated,
            'variants_unchanged': self.variants_unchanged,
            'variants_stale': self.variants_stale,
            'variants_reactivated': self.variants_reactivated,
            # Don't serialize alerts to checkpoint - they can be large
        }

    @classmethod
    def from_checkpoint_dict(cls, data: Dict) -> 'StatsTracker':
        """Deserialize stats from checkpoint."""
        stats = cls(
            vendor_id=data['vendor_id'],
            is_full_scrape=data.get('is_full_scrape', True),
            max_products_limit=data.get('max_products_limit')
        )
        stats.started_at = datetime.fromisoformat(data['started_at'])
        stats.products_discovered = data.get('products_discovered', 0)
        stats.products_processed = data.get('products_processed', 0)
        stats.products_skipped = data.get('products_skipped', 0)
        stats.products_failed = data.get('products_failed', 0)
        stats.variants_new = data.get('variants_new', 0)
        stats.variants_updated = data.get('variants_updated', 0)
        stats.variants_unchanged = data.get('variants_unchanged', 0)
        stats.variants_stale = data.get('variants_stale', 0)
        stats.variants_reactivated = data.get('variants_reactivated', 0)
        return stats

    def print_report(self):
        """Print the final scrape statistics report to console."""
        self.completed_at = datetime.now()
        duration = self.completed_at - self.started_at
        duration_str = str(timedelta(seconds=int(duration.total_seconds())))

        print("\n" + "=" * 70)
        print("SCRAPE STATISTICS REPORT")
        print("=" * 70)
        print(f"\nRun Duration: {duration_str}")
        print(f"Full Scrape: {'Yes' if self.is_full_scrape else 'No'}")
        if self.max_products_limit:
            print(f"Max Products Limit: {self.max_products_limit}")

        print("\n--- PRODUCTS ---")
        print(f"  Discovered:    {self.products_discovered:>6}")
        print(f"  Processed:     {self.products_processed:>6}")
        print(f"  Skipped:       {self.products_skipped:>6}")
        print(f"  Failed:        {self.products_failed:>6}")

        print("\n--- VARIANTS ---")
        print(f"  New:           {self.variants_new:>6}")
        print(f"  Updated:       {self.variants_updated:>6}")
        print(f"  Unchanged:     {self.variants_unchanged:>6}")
        print(f"  Stale:         {self.variants_stale:>6}")
        print(f"  Reactivated:   {self.variants_reactivated:>6}")

        # Alert counts
        alert_counts = self.get_alert_counts()
        if alert_counts:
            print("\n--- ALERTS ---")
            for alert_type, count in sorted(alert_counts.items()):
                print(f"  {alert_type:<25} {count:>6}")

        # Major price changes
        price_decreases = self.get_alerts_by_type(AlertType.PRICE_DECREASE_MAJOR)
        price_increases = self.get_alerts_by_type(AlertType.PRICE_INCREASE_MAJOR)
        if price_decreases or price_increases:
            print("\n--- MAJOR PRICE CHANGES (>30%) ---")
            for alert in price_decreases[:10]:
                name = (alert.product_name or alert.sku or "Unknown")[:35]
                print(f"  ▼ {name:<35} {alert.change_percent:>+6.1f}%: {alert.old_value} → {alert.new_value}")
            for alert in price_increases[:10]:
                name = (alert.product_name or alert.sku or "Unknown")[:35]
                print(f"  ▲ {name:<35} {alert.change_percent:>+6.1f}%: {alert.old_value} → {alert.new_value}")
            total_price = len(price_decreases) + len(price_increases)
            if total_price > 20:
                print(f"  ... ({total_price} total)")

        # Stock outs
        stock_outs = self.get_alerts_by_type(AlertType.STOCK_OUT)
        if stock_outs:
            print("\n--- STOCK OUTS ---")
            for alert in stock_outs[:10]:
                sku = alert.sku or "N/A"
                name = (alert.product_name or "Unknown")[:40]
                print(f"  {sku:<12} {name:<40} in_stock → out_of_stock")
            if len(stock_outs) > 10:
                print(f"  ... ({len(stock_outs)} total)")

        # Stale variants
        stale = self.get_alerts_by_type(AlertType.STALE_VARIANT)
        if stale:
            print("\n--- STALE VARIANTS (Soft-deleted) ---")
            for alert in stale[:10]:
                sku = alert.sku or "N/A"
                name = (alert.product_name or "Unknown")[:40]
                last_seen = alert.old_value or "unknown"
                print(f"  {sku:<12} {name:<40} Last seen: {last_seen}")
            if len(stale) > 10:
                print(f"  ... ({len(stale)} total)")

        # Reactivated
        reactivated = self.get_alerts_by_type(AlertType.REACTIVATED)
        if reactivated:
            print("\n--- REACTIVATED (Returned to site) ---")
            for alert in reactivated[:10]:
                sku = alert.sku or "N/A"
                name = (alert.product_name or "Unknown")[:40]
                stale_since = alert.old_value or "unknown"
                print(f"  {sku:<12} {name:<40} Was stale since: {stale_since}")
            if len(reactivated) > 10:
                print(f"  ... ({len(reactivated)} total)")

        # Failures
        failures = self.get_alerts_by_type(AlertType.HTTP_ERROR) + self.get_alerts_by_type(AlertType.DB_ERROR)
        if failures:
            print("\n--- FAILURES ---")
            for alert in failures[:10]:
                print(f"  {alert.message}")
            if len(failures) > 10:
                print(f"  ... ({len(failures)} total)")

        print("\n" + "=" * 70)


# =============================================================================
# Scrape Run Persistence
# =============================================================================

def save_scrape_run(conn, stats: 'StatsTracker') -> Optional[int]:
    """Save scrape run summary to ScrapeRuns table. Returns run_id."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)

    # Check if ScrapeRuns table exists
    try:
        if is_postgres(conn):
            cursor.execute(
                f'''INSERT INTO scraperuns
                   (vendor_id, started_at, completed_at, status,
                    products_discovered, products_processed, products_skipped, products_failed,
                    variants_new, variants_updated, variants_unchanged, variants_stale, variants_reactivated,
                    price_alerts, stock_alerts, data_quality_alerts,
                    is_full_scrape, max_products_limit)
                   VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
                   RETURNING run_id''',
                (stats.vendor_id, stats.started_at.isoformat(),
                 datetime.now().isoformat(), 'completed',
                 stats.products_discovered, stats.products_processed,
                 stats.products_skipped, stats.products_failed,
                 stats.variants_new, stats.variants_updated,
                 stats.variants_unchanged, stats.variants_stale, stats.variants_reactivated,
                 len(stats.get_alerts_by_type(AlertType.PRICE_DECREASE_MAJOR)) +
                 len(stats.get_alerts_by_type(AlertType.PRICE_INCREASE_MAJOR)),
                 len(stats.get_alerts_by_type(AlertType.STOCK_OUT)),
                 len(stats.get_alerts_by_type(AlertType.PARSE_FAILURE)) +
                 len(stats.get_alerts_by_type(AlertType.MISSING_REQUIRED)),
                 stats.is_full_scrape, stats.max_products_limit)
            )
            run_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                f'''INSERT INTO scraperuns
                   (vendor_id, started_at, completed_at, status,
                    products_discovered, products_processed, products_skipped, products_failed,
                    variants_new, variants_updated, variants_unchanged, variants_stale, variants_reactivated,
                    price_alerts, stock_alerts, data_quality_alerts,
                    is_full_scrape, max_products_limit)
                   VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})''',
                (stats.vendor_id, stats.started_at.isoformat(),
                 datetime.now().isoformat(), 'completed',
                 stats.products_discovered, stats.products_processed,
                 stats.products_skipped, stats.products_failed,
                 stats.variants_new, stats.variants_updated,
                 stats.variants_unchanged, stats.variants_stale, stats.variants_reactivated,
                 len(stats.get_alerts_by_type(AlertType.PRICE_DECREASE_MAJOR)) +
                 len(stats.get_alerts_by_type(AlertType.PRICE_INCREASE_MAJOR)),
                 len(stats.get_alerts_by_type(AlertType.STOCK_OUT)),
                 len(stats.get_alerts_by_type(AlertType.PARSE_FAILURE)) +
                 len(stats.get_alerts_by_type(AlertType.MISSING_REQUIRED)),
                 stats.is_full_scrape, stats.max_products_limit)
            )
            run_id = cursor.lastrowid

        stats.run_id = run_id
        return run_id
    except Exception as e:
        # Table may not exist yet - that's OK, just skip persistence
        print(f"  Note: Could not save scrape run (table may not exist): {e}")
        return None


def save_alerts(conn, stats: 'StatsTracker') -> int:
    """Save warning and critical alerts to ScrapeAlerts table. Returns count saved."""
    if not stats.run_id:
        return 0

    cursor = conn.cursor()
    ph = db_placeholder(conn)
    saved = 0

    try:
        for alert in stats.alerts:
            # Only persist warning and critical alerts (not info)
            if alert.severity == AlertSeverity.INFO:
                continue

            cursor.execute(
                f'''INSERT INTO scrapealerts
                   (run_id, vendor_ingredient_id, alert_type, severity,
                    sku, product_name, old_value, new_value, change_percent, message)
                   VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})''',
                (stats.run_id, alert.vendor_ingredient_id,
                 alert.alert_type.value, alert.severity.value,
                 alert.sku, alert.product_name,
                 alert.old_value, alert.new_value,
                 alert.change_percent, alert.message)
            )
            saved += 1

        return saved
    except Exception as e:
        print(f"  Note: Could not save alerts (table may not exist): {e}")
        return 0


def cleanup_old_alerts(conn, days: int = 30) -> int:
    """Delete alerts older than specified days. Returns count deleted."""
    cursor = conn.cursor()

    try:
        if is_postgres(conn):
            cursor.execute(
                f"DELETE FROM scrapealerts WHERE created_at < NOW() - INTERVAL '{days} days'"
            )
        else:
            cursor.execute(
                f"DELETE FROM scrapealerts WHERE created_at < datetime('now', '-{days} days')"
            )
        deleted = cursor.rowcount
        if deleted > 0:
            print(f"  Cleaned up {deleted} alerts older than {days} days")
        return deleted
    except Exception:
        # Table may not exist
        return 0


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Scrape BoxNutra.com products')
    parser.add_argument('--max-products', type=int, help='Maximum products to scrape')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    parser.add_argument('--output-dir', default='output', help='Output directory for CSV')
    parser.add_argument('--no-db', action='store_true', help='Skip database save (CSV only)')
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    print("=" * 60)
    print("BoxNutra.com Product Scraper")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Track scrape start time for staleness detection
    scrape_start_time = datetime.now().isoformat()

    # Initialize database connection
    db_conn = None
    stats = None
    if not args.no_db:
        print("\nInitializing database...", flush=True)
        db_conn = DatabaseConnection()
        try:
            db_conn.connect()
            init_boxnutra_tables(db_conn.connection)

            # Initialize StatsTracker
            stats = StatsTracker(
                vendor_id=25,  # BoxNutra
                is_full_scrape=(args.max_products is None),
                max_products_limit=args.max_products
            )

            # Cleanup old alerts (30 day retention)
            try:
                cleanup_old_alerts(db_conn.connection)
            except Exception:
                pass
        except Exception as e:
            print(f"  ⚠ Database initialization failed: {e}", flush=True)
            print("  Continuing with CSV-only mode...", flush=True)
            db_conn = None
            stats = None

    session = requests.Session()
    all_data = []
    processed_handles = []

    # Check for checkpoint
    checkpoint = load_checkpoint()
    if checkpoint and args.resume:
        print(f"\nFound checkpoint from {checkpoint.get('timestamp', 'unknown')}")
        print(f"  Processed: {len(checkpoint.get('processed_handles', []))} products")

        all_handles = checkpoint.get('all_handles', [])
        processed_handles = checkpoint.get('processed_handles', [])
        all_data = checkpoint.get('data', [])

        remaining = [h for h in all_handles if h not in processed_handles]
        print(f"  Remaining: {len(remaining)} products")
    elif checkpoint:
        print(f"\nNote: Checkpoint exists from {checkpoint.get('timestamp', 'unknown')}")
        print("  Use --resume to continue, or it will start fresh")
        try:
            clear = input("Clear checkpoint and start fresh? [y/N]: ").strip().lower()
            if clear == 'y':
                clear_checkpoint()
                all_handles = discover_products(session)
            else:
                print("Use --resume flag to continue from checkpoint")
                return
        except EOFError:
            all_handles = discover_products(session)
    else:
        all_handles = discover_products(session)

    if not all_handles:
        print("No products found!")
        return

    # Set products_discovered in stats
    if stats:
        stats.products_discovered = len(all_handles)

    # Apply max products limit
    if args.max_products:
        all_handles = all_handles[:args.max_products]
        print(f"\nLimited to {args.max_products} products")

    # Filter out already processed
    remaining_handles = [h for h in all_handles if h not in processed_handles]

    if not remaining_handles:
        print("\nAll products already processed!")
        if all_data:
            save_to_csv(all_data, args.output_dir)
        clear_checkpoint()
        return

    print(f"\nScraping {len(remaining_handles)} products...\n")
    tracker = ProgressTracker(len(remaining_handles))

    for i, handle in enumerate(remaining_handles):
        try:
            product_start = time.time()
            rows = scrape_product(handle, session)
            product_elapsed = time.time() - product_start
            tracker.update()

            # Log if total product scrape took > 10 seconds
            if product_elapsed > 10:
                print(f"    [SLOW-TOTAL] Product took {product_elapsed:.1f}s total", flush=True)

            if rows:
                all_data.extend(rows)
                status = "OK"
                print(tracker.format_progress(handle, status))
                print(f"    -> {len(rows)} variants")
                print(format_product_details(rows))
                if stats:
                    stats.products_processed += 1
            else:
                status = "EMPTY"
                print(tracker.format_progress(handle, status))
                if stats:
                    stats.products_skipped += 1

            processed_handles.append(handle)

            # Save checkpoint periodically
            if (i + 1) % CHECKPOINT_INTERVAL == 0:
                save_checkpoint(processed_handles, all_handles, all_data)
                print(f"\n>>> Checkpoint saved: {len(processed_handles)} products <<<\n")

            time.sleep(REQUEST_DELAY)

        except KeyboardInterrupt:
            print("\n\nInterrupted! Saving checkpoint...")
            save_checkpoint(processed_handles, all_handles, all_data)
            print(f"Checkpoint saved. Use --resume to continue.")
            return

        except Exception as e:
            print(f"Error scraping {handle}: {e}")
            continue

    # Save final results
    if all_data:
        save_to_csv(all_data, args.output_dir)

        # Save to relational database
        if db_conn:
            try:
                print("\nSaving to database...", flush=True)
                saved = save_to_database(db_conn, all_data, stats)
                print(f"  Saved {saved} rows to relational tables", flush=True)
            except Exception as e:
                print(f"  Database save failed: {e}", flush=True)
                if stats:
                    stats.record_failure("database", "DB", str(e))

        # Mark stale variants (only for full scrapes, not --max-products)
        if db_conn and not args.max_products:
            try:
                print("\nChecking for stale products...", flush=True)
                stale_variants = db_conn.execute_with_retry(
                    mark_stale_variants, 25, scrape_start_time, stats  # vendor_id=25 for BoxNutra
                )
                db_conn.connection.commit()
            except Exception as e:
                print(f"  ✗ Staleness check failed: {e}", flush=True)

    # Save skipped products log for review
    save_skipped_log(args.output_dir)

    # Save scrape run and alerts, then print report
    if db_conn and stats:
        try:
            save_scrape_run(db_conn.connection, stats)
            save_alerts(db_conn.connection, stats)
            db_conn.connection.commit()
        except Exception as e:
            print(f"  Note: Could not persist run data: {e}")

        stats.print_report()

    # Close database connection
    if db_conn:
        db_conn.close()

    clear_checkpoint()

    print("\n" + "=" * 60)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total products scraped: {len(processed_handles)}")
    print(f"Total variants saved: {len(all_data)}")
    print(f"Total products skipped: {len(skipped_products)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
