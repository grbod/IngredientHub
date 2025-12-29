#!/usr/bin/env python3
"""
IngredientsOnline.com Pricing Scraper (GraphQL API Version)

Scrapes pricing and inventory data using the GraphQL API.
No browser required - pure HTTP requests.

Credentials are read from environment variables IO_EMAIL and IO_PASSWORD.
Output is saved to a timestamped CSV file.
"""

import os
import sys
import json
import time
import random
import re
import argparse
from datetime import datetime
from typing import List, Dict, Optional, Set, Union
from urllib.parse import urlparse

import pandas as pd
import requests

# Database support - PostgreSQL (Supabase) or SQLite fallback
try:
    import psycopg2
    import psycopg2.extras
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False
    import sqlite3

# Playwright for fallback inventory scraping
_playwright_browser = None
_playwright_page = None
_playwright_context = None
_playwright_authenticated = False
_playwright_email = None
_playwright_password = None


# =============================================================================
# Configuration
# =============================================================================

GRAPHQL_URL = "https://pwaktx64p8stvio.ingredientsonline.com/graphql"
BASE_URL = "https://www.ingredientsonline.com"

# Pagination settings
DEFAULT_PAGE_SIZE = 50  # Products per GraphQL query
REQUEST_DELAY = 0.5     # Seconds between requests (be polite to the API)

# Retry configuration
MAX_RETRIES = 5
RETRY_DELAY = 2
MAX_RETRY_DELAY = 32    # Maximum delay for exponential backoff

# Token refresh settings
TOKEN_REFRESH_INTERVAL = 2700  # Refresh token after 45 minutes (before 1hr expiry)

# Checkpointing settings
CHECKPOINT_FILE = "output/scraper_checkpoint.json"
CHECKPOINT_INTERVAL = 25  # Save checkpoint every N products

# Database settings
DATABASE_FILE = "ingredients.db"  # SQLite fallback
USE_POSTGRES = True  # Set to False to force SQLite

# IO Business Model Constants (same for all IngredientsOnline products)
IO_BUSINESS_MODEL = {
    'order_rule_type': 'fixed_multiple',
    'order_rule_base_qty': 25,
    'order_rule_unit': 'kg',
    'packaging_size': 25,
    'packaging_unit': 'kg',
    'packaging_description': '25kg Fiber Drum',
    'shipping_responsibility': 'buyer',
    'shipping_terms': 'EXW'
}


# =============================================================================
# Database Connection Wrapper with Auto-Reconnect
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

    @property
    def conn(self):
        """Get the underlying connection (for direct access when needed)."""
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

def parse_manufacturer(product_name: str) -> str:
    """Extract manufacturer from 'Product Name by Manufacturer' format."""
    if ' by ' in product_name:
        return product_name.rsplit(' by ', 1)[1].strip()
    return ''


def parse_ingredient_name(product_name: str) -> str:
    """Remove manufacturer suffix from product name."""
    if ' by ' in product_name:
        return product_name.rsplit(' by ', 1)[0].strip()
    return product_name


def parse_category_from_url(url: str) -> str:
    """Extract category from URL path (first segment after domain)."""
    # https://www.ingredientsonline.com/botanicals/product-slug/ → "botanicals"
    path = urlparse(url).path
    parts = [p for p in path.split('/') if p]
    return parts[0] if parts else ''


def parse_packaging_kg(packaging: str) -> Optional[float]:
    """
    Parse packaging string to weight in kg.

    Examples:
        "25 kg Drum" → 25.0
        "50 lb Bag" → 22.68
        "100g Bottle" → 0.1
        "1gal Jug" → 3.785
        "(1,665 pieces) Carton" → None (not weight-based)
    """
    if not packaging:
        return None

    # Skip piece-count packaging like "(1,665 pieces) Carton"
    if 'pieces' in packaging.lower():
        return None

    # Match patterns like "25 kg", "50lb", "100g", "1gal", "200L"
    match = re.search(r'([\d.]+)\s*(kg|lb|g|oz|gal|l)\b', packaging, re.IGNORECASE)
    if not match:
        return None

    value = float(match.group(1))
    unit = match.group(2).lower()

    # Conversion factors to kg
    conversions = {
        'kg': 1.0,
        'g': 0.001,
        'lb': 0.453592,
        'oz': 0.0283495,
        'gal': 3.785,      # Approximate for water-based liquids
        'l': 1.0,          # Approximate 1 kg per liter
    }

    return round(value * conversions.get(unit, 1.0), 4)


def extract_variant_code(variant_sku: str) -> Optional[str]:
    """
    Extract variant/packaging code from variant SKU.

    SKU format: [product_id]-[variant_code]-[attribute_id]-[manufacturer_id]
    Example: "59410-100-10312-11455" → "100"
    """
    if not variant_sku:
        return None
    parts = variant_sku.split('-')
    return parts[1] if len(parts) >= 2 else None


def format_product_details(rows: List[Dict], verbose: bool = True) -> str:
    """
    Format product details as a table for console output.

    Shows each variant with all price tiers and inventory.
    """
    if not rows or not verbose:
        return ""

    lines = []

    # Table header
    lines.append(f"    {'Packaging':<16} {'Tier':>8} {'$/kg':>10} {'Inventory':<30}")
    lines.append(f"    {'-'*16} {'-'*8} {'-'*10} {'-'*30}")

    # Group rows by variant
    variants = {}
    for row in rows:
        variant_sku = row.get('variant_sku', '')
        if variant_sku not in variants:
            variants[variant_sku] = []
        variants[variant_sku].append(row)

    for variant_sku, variant_rows in variants.items():
        first_row = variant_rows[0]
        packaging = first_row.get('packaging', 'N/A')
        if len(packaging) > 16:
            packaging = packaging[:14] + '..'

        # Collect inventory info for this variant
        inv_parts = []
        for key in ['inv_chino_qty', 'inv_nj_qty', 'inv_sw_qty', 'inv_edison_qty']:
            if key in first_row and first_row[key]:
                loc = key.replace('inv_', '').replace('_qty', '')
                qty = first_row[key]
                inv_parts.append(f"{loc}:{qty}")
        inv_str = ', '.join(inv_parts) if inv_parts else '-'

        # Sort tiers by quantity
        sorted_rows = sorted(variant_rows, key=lambda r: r.get('tier_quantity', 0))

        for i, row in enumerate(sorted_rows):
            tier_qty = row.get('tier_quantity', 0)
            price = row.get('price', 0)
            price_type = row.get('price_type', 'tiered')

            # Only show packaging and inventory on first row of variant
            if i == 0:
                pkg_display = packaging
                inv_display = inv_str
            else:
                pkg_display = ''
                inv_display = ''

            # Format tier
            if price_type == 'flat_rate':
                tier_str = 'flat'
            else:
                tier_str = f"{tier_qty}+"

            lines.append(f"    {pkg_display:<16} {tier_str:>8} {f'${price:,.2f}':>10} {inv_display:<30}")

    return '\n'.join(lines)


# =============================================================================
# Database Functions
# =============================================================================

# Database connection type
DbConnection = Union['psycopg2.connection', 'sqlite3.Connection'] if HAS_POSTGRES else 'sqlite3.Connection'


def get_postgres_url() -> Optional[str]:
    """Get PostgreSQL connection URL from environment."""
    load_env_file()
    return os.environ.get('SUPABASE_DB_URL')


def init_database(db_path: str = None) -> DbConnection:
    """
    Initialize database with schema and seed data.
    Uses PostgreSQL (Supabase) if available, falls back to SQLite.
    """
    # Try PostgreSQL first
    postgres_url = get_postgres_url()
    if USE_POSTGRES and HAS_POSTGRES and postgres_url:
        return init_postgres_database(postgres_url)
    else:
        # Fallback to SQLite
        if not HAS_POSTGRES:
            print("  (psycopg2 not installed, using SQLite)")
        elif not postgres_url:
            print("  (SUPABASE_DB_URL not set, using SQLite)")
        return init_sqlite_database(db_path or DATABASE_FILE)


def init_postgres_database(db_url: str):
    """Initialize PostgreSQL database with schema."""
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    # Reference Tables (PostgreSQL syntax)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Units (
            unit_id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            type TEXT NOT NULL,
            conversion_factor REAL NOT NULL,
            base_unit TEXT NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Categories (
            category_id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            description TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Locations (
            location_id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            state TEXT,
            is_active INTEGER DEFAULT 1
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Manufacturers (
            manufacturer_id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            status TEXT DEFAULT 'active'
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS OrderRuleTypes (
            type_id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            description TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PricingModels (
            model_id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            description TEXT
        )
    ''')

    # Core Tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Vendors (
            vendor_id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            pricing_model TEXT,
            status TEXT DEFAULT 'active'
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Ingredients (
            ingredient_id SERIAL PRIMARY KEY,
            category_id INTEGER REFERENCES Categories(category_id),
            name TEXT NOT NULL,
            status TEXT DEFAULT 'active'
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS IngredientVariants (
            variant_id SERIAL PRIMARY KEY,
            ingredient_id INTEGER NOT NULL REFERENCES Ingredients(ingredient_id),
            manufacturer_id INTEGER REFERENCES Manufacturers(manufacturer_id),
            variant_name TEXT NOT NULL,
            status TEXT DEFAULT 'active'
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ScrapeSources (
            source_id SERIAL PRIMARY KEY,
            vendor_id INTEGER NOT NULL REFERENCES Vendors(vendor_id),
            product_url TEXT NOT NULL,
            scraped_at TEXT NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS VendorIngredients (
            vendor_ingredient_id SERIAL PRIMARY KEY,
            vendor_id INTEGER NOT NULL REFERENCES Vendors(vendor_id),
            variant_id INTEGER NOT NULL REFERENCES IngredientVariants(variant_id),
            sku TEXT,
            raw_product_name TEXT,
            shipping_responsibility TEXT,
            shipping_terms TEXT,
            current_source_id INTEGER REFERENCES ScrapeSources(source_id),
            status TEXT DEFAULT 'active',
            UNIQUE(vendor_id, variant_id, sku)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PriceTiers (
            price_tier_id SERIAL PRIMARY KEY,
            vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id),
            pricing_model_id INTEGER NOT NULL REFERENCES PricingModels(model_id),
            unit_id INTEGER REFERENCES Units(unit_id),
            source_id INTEGER REFERENCES ScrapeSources(source_id),
            min_quantity REAL DEFAULT 0,
            price REAL NOT NULL,
            original_price REAL,
            discount_percent REAL,
            price_per_kg REAL,
            effective_date TEXT NOT NULL,
            includes_shipping INTEGER DEFAULT 0
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS OrderRules (
            rule_id SERIAL PRIMARY KEY,
            vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id),
            rule_type_id INTEGER NOT NULL REFERENCES OrderRuleTypes(type_id),
            unit_id INTEGER REFERENCES Units(unit_id),
            base_quantity REAL,
            min_quantity REAL,
            effective_date TEXT NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PackagingSizes (
            package_id SERIAL PRIMARY KEY,
            vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id),
            unit_id INTEGER REFERENCES Units(unit_id),
            description TEXT,
            quantity REAL NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS InventoryLocations (
            inventory_location_id SERIAL PRIMARY KEY,
            vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id),
            location_id INTEGER NOT NULL REFERENCES Locations(location_id),
            is_primary INTEGER DEFAULT 0,
            UNIQUE(vendor_ingredient_id, location_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS InventoryLevels (
            level_id SERIAL PRIMARY KEY,
            inventory_location_id INTEGER NOT NULL REFERENCES InventoryLocations(inventory_location_id),
            unit_id INTEGER REFERENCES Units(unit_id),
            source_id INTEGER REFERENCES ScrapeSources(source_id),
            quantity_available REAL NOT NULL DEFAULT 0,
            lead_time_days INTEGER,
            expected_arrival TEXT,
            stock_status TEXT DEFAULT 'unknown',
            last_updated TEXT
        )
    ''')

    # Seed data (PostgreSQL ON CONFLICT syntax)
    for name, type_, factor, base in [('kg', 'weight', 1.0, 'kg'), ('g', 'weight', 0.001, 'kg'), ('lb', 'weight', 0.45359237, 'kg')]:
        cursor.execute(
            'INSERT INTO Units (name, type, conversion_factor, base_unit) VALUES (%s, %s, %s, %s) ON CONFLICT (name) DO NOTHING',
            (name, type_, factor, base)
        )

    for name, desc in [('fixed_multiple', 'Must order in exact multiples'), ('fixed_pack', 'Must order specific pack sizes'), ('range', 'Any quantity within min-max')]:
        cursor.execute(
            'INSERT INTO OrderRuleTypes (name, description) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING',
            (name, desc)
        )

    for name, desc in [('per_unit', 'Price per kg/lb'), ('per_package', 'Fixed price per package'), ('tiered_unit', 'Volume discount per unit'), ('tiered_package', 'Volume discount per package')]:
        cursor.execute(
            'INSERT INTO PricingModels (name, description) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING',
            (name, desc)
        )

    cursor.execute(
        'INSERT INTO Vendors (name, pricing_model, status) VALUES (%s, %s, %s) ON CONFLICT (name) DO NOTHING',
        ('IngredientsOnline', 'per_unit', 'active')
    )

    for name, state in [('Chino', 'CA'), ('Edison', 'NJ'), ('Southwest', None)]:
        cursor.execute(
            'INSERT INTO Locations (name, state) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING',
            (name, state)
        )

    conn.commit()
    print("  PostgreSQL database initialized (Supabase)")
    return conn


def init_sqlite_database(db_path: str):
    """Initialize SQLite database with schema (fallback)."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Reference Tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Units (
            unit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            type TEXT NOT NULL,
            conversion_factor REAL NOT NULL,
            base_unit TEXT NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Categories (
            category_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Locations (
            location_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            state TEXT,
            is_active INTEGER DEFAULT 1
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Manufacturers (
            manufacturer_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            status TEXT DEFAULT 'active'
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS OrderRuleTypes (
            type_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PricingModels (
            model_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT
        )
    ''')

    # Core Tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Vendors (
            vendor_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            pricing_model TEXT,
            status TEXT DEFAULT 'active'
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Ingredients (
            ingredient_id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER REFERENCES Categories(category_id),
            name TEXT NOT NULL,
            status TEXT DEFAULT 'active'
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS IngredientVariants (
            variant_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ingredient_id INTEGER NOT NULL REFERENCES Ingredients(ingredient_id),
            manufacturer_id INTEGER REFERENCES Manufacturers(manufacturer_id),
            variant_name TEXT NOT NULL,
            status TEXT DEFAULT 'active'
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ScrapeSources (
            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_id INTEGER NOT NULL REFERENCES Vendors(vendor_id),
            product_url TEXT NOT NULL,
            scraped_at TEXT NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS VendorIngredients (
            vendor_ingredient_id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_id INTEGER NOT NULL REFERENCES Vendors(vendor_id),
            variant_id INTEGER NOT NULL REFERENCES IngredientVariants(variant_id),
            sku TEXT,
            raw_product_name TEXT,
            shipping_responsibility TEXT,
            shipping_terms TEXT,
            current_source_id INTEGER REFERENCES ScrapeSources(source_id),
            status TEXT DEFAULT 'active',
            UNIQUE(vendor_id, variant_id, sku)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PriceTiers (
            price_tier_id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id),
            pricing_model_id INTEGER NOT NULL REFERENCES PricingModels(model_id),
            unit_id INTEGER REFERENCES Units(unit_id),
            source_id INTEGER REFERENCES ScrapeSources(source_id),
            min_quantity REAL DEFAULT 0,
            price REAL NOT NULL,
            original_price REAL,
            discount_percent REAL,
            price_per_kg REAL,
            effective_date TEXT NOT NULL,
            includes_shipping INTEGER DEFAULT 0
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS OrderRules (
            rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id),
            rule_type_id INTEGER NOT NULL REFERENCES OrderRuleTypes(type_id),
            unit_id INTEGER REFERENCES Units(unit_id),
            base_quantity REAL,
            min_quantity REAL,
            effective_date TEXT NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PackagingSizes (
            package_id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id),
            unit_id INTEGER REFERENCES Units(unit_id),
            description TEXT,
            quantity REAL NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS InventoryLocations (
            inventory_location_id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id),
            location_id INTEGER NOT NULL REFERENCES Locations(location_id),
            is_primary INTEGER DEFAULT 0,
            UNIQUE(vendor_ingredient_id, location_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS InventoryLevels (
            level_id INTEGER PRIMARY KEY AUTOINCREMENT,
            inventory_location_id INTEGER NOT NULL REFERENCES InventoryLocations(inventory_location_id),
            unit_id INTEGER REFERENCES Units(unit_id),
            source_id INTEGER REFERENCES ScrapeSources(source_id),
            quantity_available REAL NOT NULL DEFAULT 0,
            lead_time_days INTEGER,
            expected_arrival TEXT,
            stock_status TEXT DEFAULT 'unknown',
            last_updated TEXT
        )
    ''')

    # Seed data (SQLite INSERT OR IGNORE syntax)
    cursor.executemany(
        'INSERT OR IGNORE INTO Units (name, type, conversion_factor, base_unit) VALUES (?, ?, ?, ?)',
        [('kg', 'weight', 1.0, 'kg'), ('g', 'weight', 0.001, 'kg'), ('lb', 'weight', 0.45359237, 'kg')]
    )

    cursor.executemany(
        'INSERT OR IGNORE INTO OrderRuleTypes (name, description) VALUES (?, ?)',
        [('fixed_multiple', 'Must order in exact multiples'), ('fixed_pack', 'Must order specific pack sizes'), ('range', 'Any quantity within min-max')]
    )

    cursor.executemany(
        'INSERT OR IGNORE INTO PricingModels (name, description) VALUES (?, ?)',
        [('per_unit', 'Price per kg/lb'), ('per_package', 'Fixed price per package'), ('tiered_unit', 'Volume discount per unit'), ('tiered_package', 'Volume discount per package')]
    )

    cursor.execute(
        'INSERT OR IGNORE INTO Vendors (name, pricing_model, status) VALUES (?, ?, ?)',
        ('IngredientsOnline', 'per_unit', 'active')
    )

    cursor.executemany(
        'INSERT OR IGNORE INTO Locations (name, state) VALUES (?, ?)',
        [('Chino', 'CA'), ('Edison', 'NJ'), ('Southwest', None)]
    )

    conn.commit()
    print(f"  SQLite database initialized: {db_path}")
    return conn


def is_postgres(conn) -> bool:
    """Check if connection is PostgreSQL."""
    return HAS_POSTGRES and hasattr(conn, 'info')


def db_placeholder(conn) -> str:
    """Return the correct placeholder for the database type."""
    return '%s' if is_postgres(conn) else '?'


def get_or_create_category(conn, name: str) -> int:
    """Get existing category_id or create new one."""
    if not name:
        return None
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(f'SELECT category_id FROM Categories WHERE name = {ph}', (name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    if is_postgres(conn):
        cursor.execute(f'INSERT INTO Categories (name) VALUES ({ph}) RETURNING category_id', (name,))
        return cursor.fetchone()[0]
    else:
        cursor.execute(f'INSERT INTO Categories (name) VALUES ({ph})', (name,))
        return cursor.lastrowid


def get_or_create_manufacturer(conn, name: str) -> int:
    """Get existing manufacturer_id or create new one."""
    if not name:
        return None
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(f'SELECT manufacturer_id FROM Manufacturers WHERE name = {ph}', (name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    if is_postgres(conn):
        cursor.execute(f'INSERT INTO Manufacturers (name) VALUES ({ph}) RETURNING manufacturer_id', (name,))
        return cursor.fetchone()[0]
    else:
        cursor.execute(f'INSERT INTO Manufacturers (name) VALUES ({ph})', (name,))
        return cursor.lastrowid


def get_or_create_ingredient(conn, name: str, category_id: int) -> int:
    """Get existing ingredient_id or create new one."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(f'SELECT ingredient_id FROM Ingredients WHERE name = {ph}', (name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    if is_postgres(conn):
        cursor.execute(f'INSERT INTO Ingredients (name, category_id) VALUES ({ph}, {ph}) RETURNING ingredient_id', (name, category_id))
        return cursor.fetchone()[0]
    else:
        cursor.execute(f'INSERT INTO Ingredients (name, category_id) VALUES ({ph}, {ph})', (name, category_id))
        return cursor.lastrowid


def get_or_create_variant(conn, ingredient_id: int,
                          manufacturer_id: int, variant_name: str) -> int:
    """Get existing variant_id or create new one."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    # Note: PostgreSQL uses 'IS NOT DISTINCT FROM' for NULL-safe comparison, SQLite uses 'IS'
    if is_postgres(conn):
        cursor.execute(
            f'SELECT variant_id FROM IngredientVariants WHERE ingredient_id = {ph} AND manufacturer_id IS NOT DISTINCT FROM {ph} AND variant_name = {ph}',
            (ingredient_id, manufacturer_id, variant_name)
        )
    else:
        cursor.execute(
            f'SELECT variant_id FROM IngredientVariants WHERE ingredient_id = {ph} AND manufacturer_id IS {ph} AND variant_name = {ph}',
            (ingredient_id, manufacturer_id, variant_name)
        )
    row = cursor.fetchone()
    if row:
        return row[0]
    if is_postgres(conn):
        cursor.execute(
            f'INSERT INTO IngredientVariants (ingredient_id, manufacturer_id, variant_name) VALUES ({ph}, {ph}, {ph}) RETURNING variant_id',
            (ingredient_id, manufacturer_id, variant_name)
        )
        return cursor.fetchone()[0]
    else:
        cursor.execute(
            f'INSERT INTO IngredientVariants (ingredient_id, manufacturer_id, variant_name) VALUES ({ph}, {ph}, {ph})',
            (ingredient_id, manufacturer_id, variant_name)
        )
        return cursor.lastrowid


def insert_scrape_source(conn, vendor_id: int, url: str, scraped_at: str) -> int:
    """Insert scrape source record, return source_id."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    if is_postgres(conn):
        cursor.execute(
            f'INSERT INTO ScrapeSources (vendor_id, product_url, scraped_at) VALUES ({ph}, {ph}, {ph}) RETURNING source_id',
            (vendor_id, url, scraped_at)
        )
        return cursor.fetchone()[0]
    else:
        cursor.execute(
            f'INSERT INTO ScrapeSources (vendor_id, product_url, scraped_at) VALUES ({ph}, {ph}, {ph})',
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
        f'''SELECT vendor_ingredient_id FROM VendorIngredients
           WHERE vendor_id = {ph} AND variant_id = {ph} AND sku = {ph}''',
        (vendor_id, variant_id, sku)
    )
    row = cursor.fetchone()
    if row:
        vendor_ingredient_id = row[0]
        cursor.execute(
            f'''UPDATE VendorIngredients SET raw_product_name = {ph},
               shipping_responsibility = {ph}, shipping_terms = {ph}, current_source_id = {ph},
               last_seen_at = {ph}, status = 'active'
               WHERE vendor_ingredient_id = {ph}''',
            (raw_name, IO_BUSINESS_MODEL['shipping_responsibility'],
             IO_BUSINESS_MODEL['shipping_terms'], source_id, now, vendor_ingredient_id)
        )
        return vendor_ingredient_id
    if is_postgres(conn):
        cursor.execute(
            f'''INSERT INTO VendorIngredients
               (vendor_id, variant_id, sku, raw_product_name, shipping_responsibility,
                shipping_terms, current_source_id, last_seen_at, status)
               VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, 'active')
               RETURNING vendor_ingredient_id''',
            (vendor_id, variant_id, sku, raw_name,
             IO_BUSINESS_MODEL['shipping_responsibility'], IO_BUSINESS_MODEL['shipping_terms'],
             source_id, now)
        )
        return cursor.fetchone()[0]
    else:
        cursor.execute(
            f'''INSERT INTO VendorIngredients
               (vendor_id, variant_id, sku, raw_product_name, shipping_responsibility,
                shipping_terms, current_source_id, last_seen_at, status)
               VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, 'active')''',
            (vendor_id, variant_id, sku, raw_name,
             IO_BUSINESS_MODEL['shipping_responsibility'], IO_BUSINESS_MODEL['shipping_terms'],
             source_id, now)
        )
        return cursor.lastrowid


def delete_old_price_tiers(conn, vendor_ingredient_id: int) -> None:
    """Delete existing price tiers for a vendor ingredient (simple upsert approach)."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(f'DELETE FROM PriceTiers WHERE vendor_ingredient_id = {ph}', (vendor_ingredient_id,))


def insert_price_tier(conn, vendor_ingredient_id: int,
                      tier_data: dict, source_id: int, pricing_model_id: int) -> None:
    """Insert price tier record."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    # Get kg unit_id
    cursor.execute(f'SELECT unit_id FROM Units WHERE name = {ph}', ('kg',))
    unit_row = cursor.fetchone()
    unit_id = unit_row[0] if unit_row else None

    cursor.execute(
        f'''INSERT INTO PriceTiers
           (vendor_ingredient_id, pricing_model_id, unit_id, source_id, min_quantity,
            price, original_price, discount_percent, price_per_kg, effective_date, includes_shipping)
           VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})''',
        (vendor_ingredient_id, pricing_model_id, unit_id, source_id,
         tier_data.get('tier_quantity', 0),
         tier_data.get('price', 0),
         tier_data.get('original_price'),
         tier_data.get('discount_percent', 0),
         tier_data.get('price_per_kg', tier_data.get('price', 0)),
         tier_data.get('scraped_at', datetime.now().isoformat()),
         0)  # includes_shipping = 0 for IO (buyer pays)
    )


def upsert_order_rule(conn, vendor_ingredient_id: int, scraped_at: str) -> None:
    """Insert or update order rule for IO fixed_multiple."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    # Get rule type id for fixed_multiple
    cursor.execute(f'SELECT type_id FROM OrderRuleTypes WHERE name = {ph}', ('fixed_multiple',))
    type_row = cursor.fetchone()
    rule_type_id = type_row[0] if type_row else 1

    # Get kg unit_id
    cursor.execute(f'SELECT unit_id FROM Units WHERE name = {ph}', ('kg',))
    unit_row = cursor.fetchone()
    unit_id = unit_row[0] if unit_row else None

    # Delete existing and insert new
    cursor.execute(f'DELETE FROM OrderRules WHERE vendor_ingredient_id = {ph}', (vendor_ingredient_id,))
    cursor.execute(
        f'''INSERT INTO OrderRules
           (vendor_ingredient_id, rule_type_id, unit_id, base_quantity, min_quantity, effective_date)
           VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})''',
        (vendor_ingredient_id, rule_type_id, unit_id,
         IO_BUSINESS_MODEL['order_rule_base_qty'], IO_BUSINESS_MODEL['order_rule_base_qty'], scraped_at)
    )


def upsert_packaging_size(conn, vendor_ingredient_id: int) -> None:
    """Insert or update packaging size for IO 25kg drum."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    # Get kg unit_id
    cursor.execute(f'SELECT unit_id FROM Units WHERE name = {ph}', ('kg',))
    unit_row = cursor.fetchone()
    unit_id = unit_row[0] if unit_row else None

    # Delete existing and insert new
    cursor.execute(f'DELETE FROM PackagingSizes WHERE vendor_ingredient_id = {ph}', (vendor_ingredient_id,))
    cursor.execute(
        f'''INSERT INTO PackagingSizes (vendor_ingredient_id, unit_id, description, quantity)
           VALUES ({ph}, {ph}, {ph}, {ph})''',
        (vendor_ingredient_id, unit_id, IO_BUSINESS_MODEL['packaging_description'], IO_BUSINESS_MODEL['packaging_size'])
    )


def get_location_id(conn, source_name: str) -> Optional[int]:
    """Map warehouse source name to location_id."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    # Map known source names to location names
    location_map = {
        'Chino, CA': 'Chino',
        'Edison, NJ': 'Edison',
        'chino': 'Chino',
        'edison': 'Edison',
        'nj': 'Edison',  # API returns 'nj' for Edison, NJ
        'southwest': 'Southwest',
        'sw': 'Southwest',  # API returns 'sw' for Southwest
    }
    location_name = location_map.get(source_name)
    if not location_name:
        # Try direct match
        for key, val in location_map.items():
            if key.lower() in source_name.lower():
                location_name = val
                break
    if not location_name:
        return None
    cursor.execute(f'SELECT location_id FROM Locations WHERE name = {ph}', (location_name,))
    row = cursor.fetchone()
    return row[0] if row else None


def upsert_inventory(conn, vendor_ingredient_id: int, location_id: int,
                     qty: float, leadtime_weeks: str, eta: str, source_id: int) -> None:
    """Insert or update inventory level."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    # Get kg unit_id
    cursor.execute(f'SELECT unit_id FROM Units WHERE name = {ph}', ('kg',))
    unit_row = cursor.fetchone()
    unit_id = unit_row[0] if unit_row else None

    # Get or create inventory location
    cursor.execute(
        f'SELECT inventory_location_id FROM InventoryLocations WHERE vendor_ingredient_id = {ph} AND location_id = {ph}',
        (vendor_ingredient_id, location_id)
    )
    row = cursor.fetchone()
    if row:
        inv_loc_id = row[0]
    else:
        if is_postgres(conn):
            cursor.execute(
                f'INSERT INTO InventoryLocations (vendor_ingredient_id, location_id) VALUES ({ph}, {ph}) RETURNING inventory_location_id',
                (vendor_ingredient_id, location_id)
            )
            inv_loc_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                f'INSERT INTO InventoryLocations (vendor_ingredient_id, location_id) VALUES ({ph}, {ph})',
                (vendor_ingredient_id, location_id)
            )
            inv_loc_id = cursor.lastrowid

    # Convert leadtime from weeks to days
    leadtime_days = None
    if leadtime_weeks:
        try:
            leadtime_days = int(float(leadtime_weeks) * 7)
        except (ValueError, TypeError):
            pass

    # Determine stock status
    try:
        qty_val = float(qty) if qty else 0
        stock_status = 'in_stock' if qty_val > 0 else 'out_of_stock'
    except:
        qty_val = 0
        stock_status = 'unknown'

    # Delete old and insert new
    cursor.execute(f'DELETE FROM InventoryLevels WHERE inventory_location_id = {ph}', (inv_loc_id,))
    cursor.execute(
        f'''INSERT INTO InventoryLevels
           (inventory_location_id, unit_id, source_id, quantity_available, lead_time_days,
            expected_arrival, stock_status, last_updated)
           VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})''',
        (inv_loc_id, unit_id, source_id, qty_val, leadtime_days, eta, stock_status, datetime.now().isoformat())
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
        f'''UPDATE VendorIngredients
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
        f'''UPDATE VendorIngredients
           SET status = 'inactive'
           WHERE vendor_id = {ph}
           AND variant_id = {ph}
           AND sku NOT IN ({placeholders})
           AND status = 'active' ''',
        (vendor_id, variant_id, *seen_skus)
    )

    return cursor.rowcount


def save_to_database(conn, rows: List[Dict]) -> None:
    """Save processed product rows to the database."""
    if not rows:
        return

    cursor = conn.cursor()
    ph = db_placeholder(conn)

    # Get vendor_id for IngredientsOnline
    cursor.execute(f'SELECT vendor_id FROM Vendors WHERE name = {ph}', ('IngredientsOnline',))
    vendor_row = cursor.fetchone()
    vendor_id = vendor_row[0] if vendor_row else 1

    # Get pricing model IDs
    cursor.execute(f'SELECT model_id FROM PricingModels WHERE name = {ph}', ('tiered_unit',))
    tiered_model = cursor.fetchone()
    tiered_model_id = tiered_model[0] if tiered_model else 3

    cursor.execute(f'SELECT model_id FROM PricingModels WHERE name = {ph}', ('per_unit',))
    flat_model = cursor.fetchone()
    flat_model_id = flat_model[0] if flat_model else 1

    # All rows for same product share same base info
    first_row = rows[0]
    product_name = first_row.get('product_name', '')
    url = first_row.get('url', '')
    scraped_at = first_row.get('scraped_at', datetime.now().isoformat())
    ingredient_name = first_row.get('ingredient_name', product_name)
    manufacturer = first_row.get('manufacturer', '')
    category = first_row.get('category', '')

    # Create source record
    source_id = insert_scrape_source(conn, vendor_id, url, scraped_at)

    # Create category, manufacturer, ingredient, variant
    category_id = get_or_create_category(conn, category)
    manufacturer_id = get_or_create_manufacturer(conn, manufacturer)
    ingredient_id = get_or_create_ingredient(conn, ingredient_name, category_id)
    variant_id = get_or_create_variant(conn, ingredient_id, manufacturer_id, ingredient_name)

    # Group rows by SKU (variants)
    sku_groups = {}
    for row in rows:
        sku = row.get('variant_sku', row.get('product_sku', ''))
        if sku not in sku_groups:
            sku_groups[sku] = []
        sku_groups[sku].append(row)

    # Track seen SKUs for variant-level staleness
    seen_skus = list(sku_groups.keys())

    for sku, sku_rows in sku_groups.items():
        # Create/update vendor ingredient
        vendor_ingredient_id = upsert_vendor_ingredient(conn, vendor_id, variant_id, sku, product_name, source_id)

        # Delete old price tiers and insert new ones
        delete_old_price_tiers(conn, vendor_ingredient_id)
        for row in sku_rows:
            price_type = row.get('price_type', 'tiered')
            pricing_model_id = tiered_model_id if price_type == 'tiered' else flat_model_id
            insert_price_tier(conn, vendor_ingredient_id, row, source_id, pricing_model_id)

        # Insert order rule and packaging
        upsert_order_rule(conn, vendor_ingredient_id, scraped_at)
        upsert_packaging_size(conn, vendor_ingredient_id)

        # Insert inventory from first row (all rows share same inventory)
        first_sku_row = sku_rows[0]
        for key, value in first_sku_row.items():
            if key.startswith('inv_') and key.endswith('_qty'):
                # Extract warehouse name: inv_{warehouse}_qty
                warehouse = key[4:-4]  # Remove 'inv_' prefix and '_qty' suffix
                leadtime_key = f'inv_{warehouse}_leadtime'
                eta_key = f'inv_{warehouse}_eta'
                leadtime = first_sku_row.get(leadtime_key, '')
                eta = first_sku_row.get(eta_key, '')

                # Map warehouse to location
                location_id = get_location_id(conn, warehouse)
                if location_id:
                    upsert_inventory(conn, vendor_ingredient_id, location_id, value, leadtime, eta, source_id)

    # Mark variants not in this batch as inactive (variant-level staleness)
    mark_missing_variants_for_product(conn, vendor_id, variant_id, seen_skus, scraped_at)


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


def get_credentials() -> tuple[str, str]:
    """Get credentials from .env file or environment variables."""
    load_env_file()

    email = os.environ.get("IO_EMAIL")
    password = os.environ.get("IO_PASSWORD")

    if not email or not password:
        print("Error: Missing credentials.")
        print("Please create a .env file with:")
        print("  IO_EMAIL=your-email@example.com")
        print("  IO_PASSWORD=your-password")
        sys.exit(1)

    return email, password


# =============================================================================
# Helper Functions
# =============================================================================

def format_duration(seconds: float) -> str:
    """Format duration in human-readable form."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def format_progress(current: int, total: int, start_time: float) -> str:
    """Format progress with percentage and ETA."""
    pct = (current / total) * 100
    elapsed = time.time() - start_time

    if current > 0 and elapsed > 0:
        rate = current / elapsed
        remaining = (total - current) / rate
        return f"[{current}/{total}] ({pct:.1f}%) ETA: {format_duration(remaining)}"
    return f"[{current}/{total}] ({pct:.1f}%)"


# =============================================================================
# Checkpointing Functions
# =============================================================================

def save_checkpoint(processed_skus: Set[str], output_file: str,
                   products_processed: int, start_time: float) -> None:
    """Save checkpoint to allow resuming after crash."""
    checkpoint = {
        'processed_skus': list(processed_skus),
        'output_file': output_file,
        'products_processed': products_processed,
        'start_time': start_time,
        'checkpoint_time': datetime.now().isoformat()
    }
    # Write to temp file first, then rename (atomic operation)
    temp_file = CHECKPOINT_FILE + '.tmp'
    with open(temp_file, 'w') as f:
        json.dump(checkpoint, f)
    os.replace(temp_file, CHECKPOINT_FILE)


def load_checkpoint() -> Optional[Dict]:
    """Load checkpoint if exists."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            print("Warning: Checkpoint file corrupted, starting fresh")
            return None
    return None


def clear_checkpoint() -> None:
    """Remove checkpoint file after successful completion."""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("Checkpoint file cleared")


# =============================================================================
# Failed Products Logging
# =============================================================================

def save_failed_products(failed: List[Dict], output_dir: str = "output") -> str:
    """Save failed products to JSON file for later review/retry."""
    if not failed:
        return ""

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"failed_products_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, 'w') as f:
        json.dump(failed, f, indent=2)

    print(f"Saved {len(failed)} failed products to: {filepath}")
    return filepath


# =============================================================================
# GraphQL API Functions
# =============================================================================

def get_auth_token(email: str, password: str) -> str:
    """
    Authenticate via GraphQL and get JWT token.
    """
    query = '''
    mutation {
      generateCustomerToken(email: "%s", password: "%s") {
        token
      }
    }
    ''' % (email, password)

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                GRAPHQL_URL,
                json={'query': query},
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            if 'errors' in data:
                error_msg = data['errors'][0].get('message', 'Unknown error')
                print(f"Authentication error: {error_msg}")
                sys.exit(1)

            token = data['data']['generateCustomerToken']['token']
            return token

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                # Exponential backoff with jitter
                delay = min(RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                delay = delay * (0.5 + random.random())  # 50-150% of base delay
                print(f"Auth attempt {attempt + 1} failed: {e}, retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                print(f"Authentication failed after {MAX_RETRIES} attempts: {e}")
                sys.exit(1)


class AuthenticatedSession:
    """Manages authentication token with automatic refresh."""

    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.token: Optional[str] = None
        self.token_acquired_at: float = 0

    def get_token(self) -> str:
        """Get current token, refreshing if needed."""
        if self.token is None or self._should_refresh():
            self.refresh_token()
        return self.token

    def _should_refresh(self) -> bool:
        """Check if token should be proactively refreshed."""
        return time.time() - self.token_acquired_at > TOKEN_REFRESH_INTERVAL

    def refresh_token(self) -> str:
        """Get a new authentication token."""
        if self.token is not None:
            print("  Refreshing authentication token...")
        self.token = get_auth_token(self.email, self.password)
        self.token_acquired_at = time.time()
        return self.token


def graphql_request(query: str, token: str, variables: Dict = None,
                   auth_refresh_callback=None) -> Dict:
    """
    Make an authenticated GraphQL request with exponential backoff and token refresh.
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    payload = {'query': query}
    if variables:
        payload['variables'] = variables

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                GRAPHQL_URL,
                json=payload,
                headers=headers,
                timeout=60
            )

            # Check for auth errors (401/403)
            if response.status_code in (401, 403):
                if auth_refresh_callback and attempt < MAX_RETRIES - 1:
                    print("  Token expired, refreshing...")
                    token = auth_refresh_callback()
                    headers['Authorization'] = f'Bearer {token}'
                    continue
                raise Exception(f"Authentication failed: {response.status_code}")

            response.raise_for_status()
            data = response.json()

            # Check for GraphQL auth errors in response
            if 'errors' in data:
                error_msg = data['errors'][0].get('message', '').lower()
                if 'not authorized' in error_msg or 'token' in error_msg:
                    if auth_refresh_callback and attempt < MAX_RETRIES - 1:
                        print("  Token expired (GraphQL error), refreshing...")
                        token = auth_refresh_callback()
                        headers['Authorization'] = f'Bearer {token}'
                        continue

            return data

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                # Exponential backoff with jitter
                delay = min(RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                delay = delay * (0.5 + random.random())  # 50-150% of base delay
                print(f"  Request failed: {e}, retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                raise


def get_total_product_count(token: str, in_stock_only: bool = True) -> int:
    """
    Get total number of products available.
    """
    if in_stock_only:
        query = '''
        {
          products(filter: {in_stock: {eq: "1"}}, pageSize: 1) {
            total_count
          }
        }
        '''
    else:
        query = '''
        {
          products(filter: {}, pageSize: 1) {
            total_count
          }
        }
        '''
    data = graphql_request(query, token)
    return data['data']['products']['total_count']


def fetch_products_page(token: str, page: int, page_size: int, in_stock_only: bool = True) -> List[Dict]:
    """
    Fetch a page of products with pricing data, sorted alphabetically by name.
    """
    stock_filter = 'in_stock: {eq: "1"}' if in_stock_only else ''
    query = '''
    {
      products(filter: {%s}, pageSize: %d, currentPage: %d, sort: {name: ASC}) {''' % (stock_filter, page_size, page)

    query += '''
        items {
          __typename
          name
          sku
          url_key
          url_rewrites {
            url
          }
          price_range {
            minimum_price {
              regular_price { value currency }
              final_price { value currency }
              discount { percent_off amount_off }
            }
          }
          ... on ConfigurableProduct {
            variants {
              product {
                sku
                name
                price_range {
                  minimum_price {
                    regular_price { value currency }
                    final_price { value currency }
                    discount { percent_off amount_off }
                  }
                }
                price_tiers {
                  quantity
                  final_price { value currency }
                  discount { percent_off }
                }
              }
              attributes {
                code
                label
              }
            }
          }
          ... on SimpleProduct {
            price_range {
              minimum_price {
                regular_price { value currency }
                final_price { value currency }
                discount { percent_off amount_off }
              }
            }
            price_tiers {
              quantity
              final_price { value currency }
              discount { percent_off }
            }
          }
        }
        total_count
      }
    }
    '''

    data = graphql_request(query, token)
    return data['data']['products']['items']


_playwright_context = None

def init_playwright_browser(email: str = None, password: str = None) -> bool:
    """
    Initialize Playwright browser and authenticate for inventory fallback.
    Uses stealth options from original browser-based scraper.
    Returns True if authentication successful.

    Credentials are stored for automatic reconnection if browser is closed.
    """
    global _playwright_browser, _playwright_page, _playwright_context, _playwright_authenticated
    global _playwright_email, _playwright_password

    # Store credentials for reconnection
    if email:
        _playwright_email = email
    if password:
        _playwright_password = password

    # Use stored credentials if not provided
    email = email or _playwright_email
    password = password or _playwright_password

    if not email or not password:
        print("  No credentials available for Playwright", flush=True)
        return False

    if _playwright_authenticated and _playwright_page:
        # Verify browser is still open
        try:
            _playwright_page.url  # This will throw if browser is closed
            return True
        except:
            print("  Playwright browser was closed, reinitializing...", flush=True)
            _playwright_authenticated = False

    try:
        from playwright.sync_api import sync_playwright

        print("  Initializing Playwright for inventory fallback...", flush=True)
        playwright = sync_playwright().start()

        # Launch with headed mode (headless triggers bot detection on this site)
        _playwright_browser = playwright.chromium.launch(
            headless=False,  # Headed mode required - site detects headless
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-infobars',
            ]
        )

        # Create context with realistic settings
        _playwright_context = _playwright_browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='America/New_York',
        )

        # Pre-set cookie consent to skip banner
        _playwright_context.add_cookies([{
            "name": "__hs_notify_banner_dismiss",
            "value": "true",
            "domain": ".ingredientsonline.com",
            "path": "/"
        }])

        _playwright_page = _playwright_context.new_page()
        _playwright_page.set_default_timeout(60000)

        # Inject stealth JavaScript
        _playwright_page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            window.chrome = { runtime: {} };
        """)

        # Navigate to login page (correct URL from original scraper)
        LOGIN_URL = f"{BASE_URL}/login"
        print(f"  Navigating to {LOGIN_URL}", flush=True)
        _playwright_page.goto(LOGIN_URL + "/", wait_until="domcontentloaded", timeout=60000)
        time.sleep(2)

        # Fill email using getByLabel (preferred Playwright method)
        email_filled = False
        try:
            email_input = _playwright_page.get_by_label("Email", exact=False)
            if email_input.count() > 0:
                email_input.click()
                time.sleep(0.3)
                email_input.fill(email)
                email_filled = True
                print("  Filled email field", flush=True)
        except:
            pass

        if not email_filled:
            # Fallback selectors
            for selector in ['input[type="email"]', 'input[id="email"]', 'input[placeholder*="email" i]']:
                try:
                    loc = _playwright_page.locator(selector)
                    if loc.count() > 0:
                        loc.click()
                        time.sleep(0.3)
                        loc.fill(email)
                        email_filled = True
                        break
                except:
                    continue

        if not email_filled:
            print("  Could not find email field", flush=True)
            return False

        time.sleep(0.5)

        # Fill password using getByLabel
        password_filled = False
        try:
            password_input = _playwright_page.get_by_label("Password", exact=False)
            if password_input.count() > 0:
                password_input.click()
                time.sleep(0.3)
                password_input.fill(password)
                password_filled = True
                print("  Filled password field", flush=True)
        except:
            pass

        if not password_filled:
            for selector in ['input[type="password"]', 'input[id="pass"]']:
                try:
                    loc = _playwright_page.locator(selector)
                    if loc.count() > 0:
                        loc.click()
                        time.sleep(0.3)
                        loc.fill(password)
                        password_filled = True
                        break
                except:
                    continue

        if not password_filled:
            print("  Could not find password field", flush=True)
            return False

        time.sleep(0.5)

        # Click submit button (button text is "Login" on this page)
        submit_clicked = False
        submit_selectors = [
            'button[type="submit"]',
            'button:has-text("Login")',
            'button:has-text("Sign In")',
        ]
        for selector in submit_selectors:
            try:
                loc = _playwright_page.locator(selector).first
                if loc.is_visible():
                    loc.click()
                    submit_clicked = True
                    print(f"  Clicked submit button ({selector})", flush=True)
                    break
            except Exception as e:
                continue

        if not submit_clicked:
            print("  Warning: Could not find submit button", flush=True)
            return False

        # Wait for login to complete
        _playwright_page.wait_for_load_state('domcontentloaded')
        time.sleep(5)  # Give time for session cookies to be set

        # Verify login by checking catalog page (like original scraper)
        print("  Verifying login on catalog page...", flush=True)
        _playwright_page.goto(f"{BASE_URL}/products/?in_stock[filter]=1,1&size=10",
                              wait_until='domcontentloaded', timeout=30000)
        time.sleep(3)

        content = _playwright_page.content()
        if 'log in to see pricing' in content.lower() or 'login to see pricing' in content.lower():
            print("  ✗ Not logged in - seeing 'Log in to see pricing'", flush=True)
            _playwright_authenticated = False
            return False

        # Also verify we can see prices
        if '$' in content:
            _playwright_authenticated = True
            print("  Playwright authenticated successfully", flush=True)
            return True
        else:
            print("  Playwright authentication may have failed (no prices visible)", flush=True)
            print(f"  Current URL: {_playwright_page.url}", flush=True)
            return False

    except Exception as e:
        print(f"  Playwright init error: {e}", flush=True)
        return False


def scrape_inventory_from_html(product_url: str, retry_on_close: bool = True) -> List[Dict]:
    """
    Fallback: Scrape inventory data from product page HTML using Playwright.
    Returns list of inventory dicts with source_name, quantity, leadtime, next_stocking.

    If browser is closed, attempts to reinitialize it automatically.
    """
    global _playwright_page, _playwright_authenticated

    if not _playwright_authenticated or not _playwright_page:
        # Try to reinitialize if we have stored credentials
        if retry_on_close and _playwright_email and _playwright_password:
            print("  Attempting to reinitialize Playwright...", flush=True)
            if init_playwright_browser():
                return scrape_inventory_from_html(product_url, retry_on_close=False)
        return []

    try:
        # Navigate and wait for page to load
        _playwright_page.goto(product_url, timeout=30000, wait_until='domcontentloaded')

        # Wait for inventory table to appear (it's loaded dynamically)
        try:
            _playwright_page.wait_for_selector('.inventory-table', timeout=10000)
        except:
            # Try waiting for WAREHOUSE text as fallback
            try:
                _playwright_page.wait_for_selector('text=WAREHOUSE', timeout=5000)
            except:
                time.sleep(3)

        # Get page content
        content = _playwright_page.content()

        inventory_list = []

        # Parse inventory table structure:
        # <table class="inventory-table">
        #   <tr><td><span>Chino, CA</span></td><td>125</td><td>6 weeks</td></tr>
        #
        # Pattern 1: Look for radio button values with quantity in next cells
        # Pattern 2: Look for location names followed by table cells with numbers

        warehouse_patterns = [
            (r'Chino,?\s*CA', 'chino'),
            (r'Edison,?\s*NJ', 'nj'),
            (r'Southwest', 'sw'),
        ]

        for pattern, source_code in warehouse_patterns:
            # Try Pattern 1: location followed by table-item cells
            # e.g., <span>Chino, CA</span></label></td><td class="table-item">125</td><td class="table-item">6
            match = re.search(
                rf'{pattern}.*?</(?:span|label|td)>.*?(?:class="table-item"[^>]*>|<td[^>]*>)\s*(\d+)\s*</td>.*?(?:class="table-item"[^>]*>|<td[^>]*>)\s*([\d\-]+\s*weeks?|\d+|N/?A)?',
                content, re.IGNORECASE | re.DOTALL
            )
            if match:
                qty = int(match.group(1)) if match.group(1) else 0
                leadtime_raw = match.group(2) if match.group(2) else ''

                # Parse leadtime (e.g., "6 weeks" -> 6)
                leadtime_match = re.search(r'(\d+)', leadtime_raw)
                leadtime = int(leadtime_match.group(1)) if leadtime_match else 0

                inventory_list.append({
                    'source_code': source_code,
                    'source_name': source_code,
                    'quantity': qty,
                    'leadtime': leadtime,
                    'next_stocking': '',
                    'backorder': 0
                })
                continue

            # Try Pattern 2: simpler pattern for location + number
            match = re.search(
                rf'{pattern}[^<]*</.*?(\d+)[^<]*</td>',
                content, re.IGNORECASE | re.DOTALL
            )
            if match:
                qty = int(match.group(1)) if match.group(1) else 0
                inventory_list.append({
                    'source_code': source_code,
                    'source_name': source_code,
                    'quantity': qty,
                    'leadtime': 0,
                    'next_stocking': '',
                    'backorder': 0
                })

        return inventory_list

    except Exception as e:
        error_str = str(e).lower()
        browser_closed_errors = [
            'target page, context or browser has been closed',
            'browser has been closed',
            'context has been closed',
            'page has been closed',
            'target closed',
        ]

        if any(err in error_str for err in browser_closed_errors):
            print(f"    HTML scrape error: {e}", flush=True)
            # Mark as not authenticated so next call will try to reinitialize
            _playwright_authenticated = False

            # Try to reinitialize and retry once
            if retry_on_close and _playwright_email and _playwright_password:
                print("    🔄 Browser was closed, attempting to reconnect...", flush=True)
                if init_playwright_browser():
                    return scrape_inventory_from_html(product_url, retry_on_close=False)

        print(f"    HTML scrape error: {e}", flush=True)
        return []


def close_playwright():
    """Close Playwright browser if open."""
    global _playwright_browser, _playwright_page, _playwright_authenticated

    if _playwright_browser:
        try:
            _playwright_browser.close()
        except:
            pass
    _playwright_browser = None
    _playwright_page = None
    _playwright_authenticated = False


def get_inventory(sku: str, product_url: str = None) -> List[Dict]:
    """
    Fetch inventory data from GraphQL API.
    Falls back to HTML scraping if API fails.
    Returns list of warehouse inventory details.
    """
    query = """
    query getInventory($sku: String) {
      inventory(sku: $sku) {
        inventorydetail {
          backorder
          leadtime
          next_stocking
          quantity
          sku
          source_code
          source_name
        }
      }
    }
    """

    api_failed = False
    inventory_details = []

    try:
        response = requests.post(
            GRAPHQL_URL,
            json={"query": query, "variables": {"sku": sku}},
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()

        # Check for API errors or null inventory
        if 'errors' in data or data.get("data", {}).get("inventory") is None:
            api_failed = True
        else:
            inventory_details = data.get("data", {}).get("inventory", {}).get("inventorydetail", [])
            # If API returned empty list, don't fallback (might just be no inventory)
            if inventory_details:
                return inventory_details

    except Exception as e:
        api_failed = True

    # Fallback to HTML scraping if API failed and we have a product URL
    if api_failed and product_url and _playwright_authenticated:
        print(f"    API failed, trying HTML fallback...", flush=True)
        inventory_details = scrape_inventory_from_html(product_url)
        if inventory_details:
            print(f"    HTML fallback got {len(inventory_details)} warehouse(s)", flush=True)

    return inventory_details


# =============================================================================
# Data Processing
# =============================================================================

def get_product_url(product: Dict) -> str:
    """
    Get the correct product URL from url_rewrites field.
    This provides the canonical URL path for each product.
    """
    url_rewrites = product.get('url_rewrites', [])

    if url_rewrites and len(url_rewrites) > 0:
        # Use the first url_rewrite - this is the canonical URL
        url_path = url_rewrites[0].get('url', '')
        if url_path:
            # Ensure it starts with / and ends with /
            if not url_path.startswith('/'):
                url_path = '/' + url_path
            if not url_path.endswith('/'):
                url_path = url_path + '/'
            return f"{BASE_URL}{url_path}"

    # Fallback to just url_key if no rewrites
    url_key = product.get('url_key', '')
    if url_key:
        return f"{BASE_URL}/{url_key}/"

    return ''


def process_product(product: Dict) -> List[Dict]:
    """
    Process a single product and return rows for CSV.
    One row per price tier per variant.
    Inventory is tracked per-variant, not aggregated.
    """
    rows = []
    timestamp = datetime.now().isoformat()

    product_name = product.get('name', 'Unknown')
    product_sku = product.get('sku', 'Unknown')
    product_url = get_product_url(product)
    product_type = product.get('__typename', 'Unknown')

    # Parse new fields from existing data
    ingredient_name = parse_ingredient_name(product_name)
    manufacturer = parse_manufacturer(product_name)
    category = parse_category_from_url(product_url)

    # Fetch inventory for this product (with HTML fallback if API fails)
    inventory_data = get_inventory(product_sku, product_url)

    # Build inventory by VARIANT SKU, then by warehouse
    # Structure: {variant_sku: {warehouse: {quantity, leadtime, next_stocking}}}
    inventory_by_variant = {}
    for inv in inventory_data:
        inv_sku = inv.get('sku', '')
        source = inv.get('source_name') or inv.get('source_code') or 'Unknown'
        if not source or not inv_sku:
            continue

        qty = inv.get('quantity', 0)
        leadtime = inv.get('leadtime', '')
        next_stock = inv.get('next_stocking', '')

        try:
            qty_float = float(qty) if qty else 0
        except:
            qty_float = 0

        # Initialize variant dict if needed
        if inv_sku not in inventory_by_variant:
            inventory_by_variant[inv_sku] = {}

        # Store inventory for this variant at this warehouse
        inventory_by_variant[inv_sku][source] = {
            'quantity': qty,
            'quantity_float': qty_float,
            'leadtime_weeks': leadtime,
            'next_stocking': next_stock
        }

    # Base row data with parsed fields and IO business model constants
    base_row = {
        'product_name': product_name,
        'ingredient_name': ingredient_name,
        'manufacturer': manufacturer,
        'category': category,
        'product_sku': product_sku,
        'url': product_url,
        'scraped_at': timestamp,
        # IO Business Model fields (hardcoded)
        'order_rule_type': IO_BUSINESS_MODEL['order_rule_type'],
        'order_rule_base_qty': IO_BUSINESS_MODEL['order_rule_base_qty'],
        'order_rule_unit': IO_BUSINESS_MODEL['order_rule_unit'],
        'shipping_responsibility': IO_BUSINESS_MODEL['shipping_responsibility'],
        'shipping_terms': IO_BUSINESS_MODEL['shipping_terms'],
    }

    def add_variant_inventory(row: Dict, variant_sku: str):
        """Add per-variant inventory columns to a row."""
        variant_inv = inventory_by_variant.get(variant_sku, {})
        for warehouse, inv_info in variant_inv.items():
            safe_name = warehouse.replace(' ', '_').replace(',', '')
            row[f'inv_{safe_name}_qty'] = inv_info['quantity']
            row[f'inv_{safe_name}_leadtime'] = inv_info['leadtime_weeks']
            row[f'inv_{safe_name}_eta'] = inv_info['next_stocking']

    # Handle ConfigurableProduct (has variants)
    if product_type == 'ConfigurableProduct':
        variants = product.get('variants', [])
        if not variants:
            return rows

        for variant in variants:
            variant_product = variant.get('product', {})
            variant_sku = variant_product.get('sku', 'Unknown')
            variant_name = variant_product.get('name', product_name)
            price_tiers = variant_product.get('price_tiers', [])

            # Extract packaging from variant attributes
            variant_attrs = variant.get('attributes', [])
            packaging = variant_attrs[0].get('label', '') if variant_attrs else ''
            packaging_kg = parse_packaging_kg(packaging)
            variant_code = extract_variant_code(variant_sku)

            if price_tiers:
                # Use tiered pricing
                for tier in price_tiers:
                    price_val = tier.get('final_price', {}).get('value', 0)
                    row = base_row.copy()
                    row.update({
                        'variant_sku': variant_sku,
                        'variant_name': variant_name,
                        'variant_code': variant_code,
                        'packaging': packaging,
                        'packaging_kg': packaging_kg,
                        'tier_quantity': tier.get('quantity', 0),
                        'price': price_val,
                        'price_per_kg': price_val,  # IO already quotes in $/kg
                        'currency': tier.get('final_price', {}).get('currency', 'USD'),
                        'discount_percent': tier.get('discount', {}).get('percent_off', 0),
                        'price_type': 'tiered',
                    })
                    add_variant_inventory(row, variant_sku)
                    rows.append(row)
            else:
                # Fallback to price_range for flat-rate sale pricing
                price_range = variant_product.get('price_range', {})
                min_price = price_range.get('minimum_price', {})
                final_price = min_price.get('final_price', {}).get('value', 0)

                if final_price > 0:
                    row = base_row.copy()
                    row.update({
                        'variant_sku': variant_sku,
                        'variant_name': variant_name,
                        'variant_code': variant_code,
                        'packaging': packaging,
                        'packaging_kg': packaging_kg,
                        'tier_quantity': 1,
                        'price': final_price,
                        'price_per_kg': final_price,  # IO already quotes in $/kg
                        'original_price': min_price.get('regular_price', {}).get('value', 0),
                        'currency': min_price.get('final_price', {}).get('currency', 'USD'),
                        'discount_percent': min_price.get('discount', {}).get('percent_off', 0),
                        'price_type': 'flat_rate',
                    })
                    add_variant_inventory(row, variant_sku)
                    rows.append(row)

    # Handle SimpleProduct (no variants)
    elif product_type == 'SimpleProduct':
        price_tiers = product.get('price_tiers', [])
        variant_code = extract_variant_code(product_sku)

        # SimpleProduct doesn't have variant attributes, default to 25kg Drum
        packaging = '25 kg Drum'
        packaging_kg = 25.0

        if price_tiers:
            # Use tiered pricing
            for tier in price_tiers:
                price_val = tier.get('final_price', {}).get('value', 0)
                row = base_row.copy()
                row.update({
                    'variant_sku': product_sku,
                    'variant_name': product_name,
                    'variant_code': variant_code,
                    'packaging': packaging,
                    'packaging_kg': packaging_kg,
                    'tier_quantity': tier.get('quantity', 0),
                    'price': price_val,
                    'price_per_kg': price_val,  # IO already quotes in $/kg
                    'currency': tier.get('final_price', {}).get('currency', 'USD'),
                    'discount_percent': tier.get('discount', {}).get('percent_off', 0),
                    'price_type': 'tiered',
                })
                add_variant_inventory(row, product_sku)
                rows.append(row)
        else:
            # Fallback to price_range for flat-rate sale pricing
            price_range = product.get('price_range', {})
            min_price = price_range.get('minimum_price', {})
            final_price = min_price.get('final_price', {}).get('value', 0)

            if final_price > 0:
                row = base_row.copy()
                row.update({
                    'variant_sku': product_sku,
                    'variant_name': product_name,
                    'variant_code': variant_code,
                    'packaging': packaging,
                    'packaging_kg': packaging_kg,
                    'tier_quantity': 1,
                    'price': final_price,
                    'price_per_kg': final_price,  # IO already quotes in $/kg
                    'original_price': min_price.get('regular_price', {}).get('value', 0),
                    'currency': min_price.get('final_price', {}).get('currency', 'USD'),
                    'discount_percent': min_price.get('discount', {}).get('percent_off', 0),
                    'price_type': 'flat_rate',
                })
                add_variant_inventory(row, product_sku)
                rows.append(row)

    return rows


def save_to_csv(data: List[Dict], output_dir: str = "output", output_file: str = None) -> str:
    """
    Save scraped data to a CSV file.
    If output_file is provided, uses that filename. Otherwise generates timestamped name.
    Returns the filepath of the created file.
    """
    if not data:
        print("No data to save")
        return ""

    df = pd.DataFrame(data)

    # Reorder columns per scraper-specifications.md
    priority_cols = [
        'product_name', 'ingredient_name', 'manufacturer', 'category',
        'product_sku', 'variant_sku', 'variant_code', 'variant_name',
        'packaging', 'packaging_kg',
        'tier_quantity', 'price', 'price_per_kg',
        'original_price', 'discount_percent', 'price_type',
        'order_rule_type', 'order_rule_base_qty', 'order_rule_unit',
        'shipping_responsibility', 'shipping_terms',
        'url', 'scraped_at', 'currency'
    ]
    other_cols = [c for c in df.columns if c not in priority_cols]
    ordered_cols = [c for c in priority_cols if c in df.columns] + other_cols
    df = df[ordered_cols]

    if output_file:
        filepath = output_file if os.path.isabs(output_file) else os.path.join(output_dir, output_file)
    else:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        filename = f"pricing_data_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False)
    print(f"\nSaved {len(data)} rows to: {filepath}")

    return filepath


# =============================================================================
# Main Scraper
# =============================================================================

def main():
    """Main entry point for the scraper."""
    parser = argparse.ArgumentParser(
        description='IngredientsOnline.com Pricing Scraper (GraphQL API)'
    )
    parser.add_argument('--page-size', type=int, default=DEFAULT_PAGE_SIZE,
                        help=f'Products per page (default: {DEFAULT_PAGE_SIZE})')
    parser.add_argument('--max-products', type=int, default=None,
                        help='Maximum products to scrape (for testing)')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from checkpoint if exists')
    parser.add_argument('--checkpoint-interval', type=int, default=CHECKPOINT_INTERVAL,
                        help=f'Products between checkpoints (default: {CHECKPOINT_INTERVAL})')
    parser.add_argument('--no-playwright', action='store_true',
                        help='Disable Playwright fallback (faster startup, API-only)')
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs("output", exist_ok=True)

    print("=" * 60)
    print("IngredientsOnline.com Pricing Scraper (GraphQL API)")
    print("=" * 60)

    # Track scrape start time for staleness detection
    scrape_start_time = datetime.now().isoformat()

    # Check for checkpoint resume
    checkpoint = None
    processed_skus: Set[str] = set()
    output_file = None

    if args.resume:
        checkpoint = load_checkpoint()
        if checkpoint:
            processed_skus = set(checkpoint.get('processed_skus', []))
            output_file = checkpoint.get('output_file')
            print(f"\n✓ Resuming from checkpoint: {len(processed_skus)} products already processed")
            print(f"  Output file: {output_file}")
        else:
            print("\nNo checkpoint found, starting fresh")

    # Get credentials and create authenticated session
    email, password = get_credentials()

    print("\nAuthenticating...")
    session = AuthenticatedSession(email, password)
    token = session.get_token()
    print("✓ Authentication successful")

    # Initialize Playwright for inventory fallback (optional)
    if not args.no_playwright:
        print("\nInitializing inventory fallback (Playwright)...")
        if init_playwright_browser(email, password):
            print("✓ Playwright ready for inventory fallback")
        else:
            print("⚠ Playwright fallback not available (API-only mode)")
    else:
        print("\n⚡ Playwright disabled (--no-playwright), using API-only mode")

    # Get total product count
    print("\nFetching product count...")
    total_count = get_total_product_count(token)
    print(f"Found {total_count} total products")

    # Apply max limit if specified
    target_count = total_count
    if args.max_products:
        target_count = min(total_count, args.max_products)
        print(f"Limited to {target_count} products (--max-products)")

    # Calculate pagination
    page_size = args.page_size
    total_pages = (total_count + page_size - 1) // page_size

    # Determine output file (new or resume)
    if output_file is None:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        output_file = f"pricing_data_{timestamp}.csv"

    # Initialize database with auto-reconnect wrapper
    db_path = DATABASE_FILE
    print(f"\nInitializing database: {db_path}")
    db_wrapper = DatabaseConnection(db_path)
    db_wrapper.connect()
    print("✓ Database initialized")

    print(f"\nScraping {target_count} products ({page_size}/page, {total_pages} pages)")
    print(f"Checkpoint interval: every {args.checkpoint_interval} products")
    print(f"Output file: {output_file}")
    print("-" * 40)

    # Scrape all products
    all_data = []
    failed_products = []
    products_processed = 0
    products_in_session = 0  # Products processed in this session (for checkpointing)
    start_time = time.time()

    for page in range(1, total_pages + 1):
        print(f"\n[Page {page}/{total_pages}] Fetching products...", flush=True)

        try:
            # Get fresh token if needed before each page
            token = session.get_token()
            products = fetch_products_page(token, page, page_size)

            for product in products:
                product_sku = product.get('sku', 'Unknown')

                # Skip if already processed (resume mode)
                if product_sku in processed_skus:
                    continue

                if args.max_products and products_processed >= args.max_products:
                    break

                products_processed += 1
                products_in_session += 1

                # Display progress with ETA
                progress = format_progress(products_processed, target_count, start_time)
                product_name = product.get('name', 'Unknown')[:45]
                print(f"  {progress} {product_name}...", flush=True)

                try:
                    rows = process_product(product)
                    if rows:
                        all_data.extend(rows)
                        # Save to database with auto-reconnect
                        db_wrapper.execute_with_retry(save_to_database, rows)

                        # Count unique variants
                        unique_variants = len(set(r.get('variant_sku', '') for r in rows))
                        tier_count = len(rows)
                        price_type = rows[0].get('price_type', 'tiered')

                        if unique_variants == 1:
                            if price_type == 'flat_rate':
                                print(f"    → 1 variant, flat rate ${rows[0].get('price', 0)}/kg", flush=True)
                            else:
                                print(f"    → 1 variant, {tier_count} price tiers", flush=True)
                        else:
                            print(f"    → {unique_variants} variants, {tier_count} total rows", flush=True)

                        # Print detailed breakdown
                        details = format_product_details(rows, verbose=True)
                        if details:
                            print(details, flush=True)
                        print(flush=True)  # Blank line between products
                    else:
                        print(f"    → No pricing data\n", flush=True)

                    # Mark as processed
                    processed_skus.add(product_sku)

                except Exception as e:
                    # Track failed product
                    failed_products.append({
                        'sku': product_sku,
                        'name': product.get('name', 'Unknown'),
                        'error': str(e),
                        'timestamp': datetime.now().isoformat(),
                        'page': page
                    })
                    print(f"    ✗ Failed: {e}", flush=True)

                # Checkpoint periodically
                if products_in_session > 0 and products_in_session % args.checkpoint_interval == 0:
                    # Save data collected so far
                    if all_data:
                        save_to_csv(all_data, output_file=output_file)
                    # Commit database with auto-reconnect
                    db_wrapper.commit()
                    save_checkpoint(processed_skus, output_file, products_processed, start_time)
                    print(f"    📍 Checkpoint saved ({products_processed} products)", flush=True)

                # Small delay between inventory fetches
                time.sleep(REQUEST_DELAY)

            if args.max_products and products_processed >= args.max_products:
                break

        except Exception as e:
            print(f"  Error on page {page}: {e}")
            continue

        # Delay between pages
        time.sleep(REQUEST_DELAY)

    # Calculate elapsed time
    elapsed = time.time() - start_time
    rate = products_processed / elapsed if elapsed > 0 else 0

    # Save results
    print("\n" + "-" * 40)
    print("Saving results...")

    if all_data:
        filepath = save_to_csv(all_data, output_file=output_file)

        # Final database commit
        db_wrapper.commit()

        # Mark stale variants (only for full scrapes, not --max-products)
        if not args.max_products:
            print("\nChecking for stale products...")
            stale_count = db_wrapper.execute_with_retry(
                mark_stale_variants, 1, scrape_start_time  # vendor_id=1 for IngredientsOnline
            )
            db_wrapper.commit()

        db_wrapper.close()

        print("\n" + "=" * 60)
        print("SCRAPING COMPLETE")
        print("=" * 60)
        print(f"Products scraped: {products_processed}")
        print(f"Price tiers extracted: {len(all_data)}")
        print(f"Time elapsed: {elapsed:.1f}s ({rate:.1f} products/sec)")
        print(f"Output file: {filepath}")
        print(f"Database file: {db_path}")

        if failed_products:
            print(f"Failed products: {len(failed_products)}")
            save_failed_products(failed_products)

        # Clear checkpoint on successful completion
        clear_checkpoint()

        # Close Playwright browser
        close_playwright()

        # Preview
        print("\nData preview:")
        df = pd.DataFrame(all_data)
        preview_cols = ['product_name', 'ingredient_name', 'manufacturer', 'tier_quantity', 'price']
        available = [c for c in preview_cols if c in df.columns]
        print(df[available].head(10).to_string())
    else:
        print("\nNo data was extracted.")
        # Still close database and Playwright
        db_wrapper.close()
        close_playwright()
        if failed_products:
            print(f"Failed products: {len(failed_products)}")
            save_failed_products(failed_products)


if __name__ == "__main__":
    main()
