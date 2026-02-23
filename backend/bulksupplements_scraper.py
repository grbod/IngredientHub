#!/usr/bin/env python3
"""
BulkSupplements.com Product Scraper

Scrapes all products from BulkSupplements.com including variants, pricing, and availability.
Output is saved to a timestamped CSV file with checkpoint support.
"""

import os
import sys
import json
import time
import re
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum

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

BASE_URL = "https://www.bulksupplements.com"

# Rate limiting
REQUEST_DELAY = 0.5  # Seconds between requests

# Retry configuration (exponential backoff)
MAX_RETRIES = 7
INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 60

# Checkpoint configuration
CHECKPOINT_INTERVAL = 25
CHECKPOINT_FILE = "output/.bulksupplements_checkpoint.json"

# Request headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/json,*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
}

# Database settings (shared with IO scraper)
DATABASE_FILE = "ingredients.db"  # SQLite fallback
USE_POSTGRES = True  # Set to False to force SQLite

# BS Business Model Constants (same for all BulkSupplements products)
BS_BUSINESS_MODEL = {
    'order_rule_type': 'fixed_pack',
    'shipping_responsibility': 'vendor',  # BS includes free shipping
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
                        print(f"  Database error: {e}", flush=True)
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

def parse_pack_size_g(option2: str) -> float:
    """Parse option2 string to grams.
    Examples:
    - "100 Grams (3.5 oz)" → 100
    - "1 Kilogram (2.2 lbs)" → 1000
    - "25 Kilograms (55 lbs)" → 25000
    """
    if not option2:
        return 0

    option2_lower = option2.lower()

    # Extract number at the beginning
    match = re.match(r'(\d+(?:\.\d+)?)', option2)
    if not match:
        return 0

    number = float(match.group(1))

    if 'kilogram' in option2_lower:
        return number * 1000
    elif 'gram' in option2_lower:
        return number

    return 0


def calculate_price_per_kg(price: float, pack_size_g: float) -> float:
    """Calculate price per kg from price and pack size in grams."""
    if pack_size_g <= 0:
        return 0
    return (price / pack_size_g) * 1000


def parse_packaging_kg(pack_size_g: float) -> Optional[float]:
    """Convert pack size in grams to kg.

    Examples:
        100 → 0.1
        250 → 0.25
        1000 → 1.0
        25000 → 25.0
    """
    if not pack_size_g or pack_size_g <= 0:
        return None
    return round(pack_size_g / 1000, 4)


def convert_stock_status(in_stock: bool) -> str:
    """Convert boolean in_stock to string status."""
    if in_stock is None:
        return 'unknown'
    return 'in_stock' if in_stock else 'out_of_stock'


def format_product_details(rows: List[Dict], verbose: bool = True) -> str:
    """
    Format product details as a table for console output.

    Shows each variant with packaging, price, $/kg, and stock status.
    """
    if not rows or not verbose:
        return ""

    lines = []

    # Table header
    lines.append(f"    {'Packaging':<22} {'Size':>8} {'Price':>10} {'$/kg':>10} {'Stock':<10}")
    lines.append(f"    {'-'*22} {'-'*8} {'-'*10} {'-'*10} {'-'*10}")

    # Sort rows by pack_size_g for consistent display
    sorted_rows = sorted(rows, key=lambda r: r.get('pack_size_g', 0) or 0)

    for row in sorted_rows:
        packaging = row.get('packaging', 'N/A')
        if len(packaging) > 22:
            packaging = packaging[:20] + '..'

        packaging_kg = row.get('packaging_kg')
        size_str = f"{packaging_kg}kg" if packaging_kg else '-'

        price = row.get('price', 0)
        try:
            price_val = float(price) if price else 0
        except:
            price_val = 0

        price_per_kg = row.get('price_per_kg', 0) or 0
        stock_status = row.get('stock_status', 'unknown')

        lines.append(f"    {packaging:<22} {size_str:>8} {f'${price_val:,.2f}':>10} {f'${price_per_kg:,.2f}':>10} {stock_status:<10}")

    return '\n'.join(lines)


# =============================================================================
# Database Functions
# =============================================================================

# Database connection type
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
    return os.environ.get('DATABASE_URL')


def is_postgres(conn) -> bool:
    """Check if connection is PostgreSQL."""
    return HAS_POSTGRES and hasattr(conn, 'info')


def db_placeholder(conn) -> str:
    """Return the correct placeholder for the database type."""
    return '%s' if is_postgres(conn) else '?'


def init_database(db_path: str = None) -> DbConnection:
    """
    Initialize database with schema and seed data.
    Uses PostgreSQL if available, falls back to SQLite.
    """
    postgres_url = get_postgres_url()
    if USE_POSTGRES and HAS_POSTGRES and postgres_url:
        return init_postgres_database(postgres_url)
    else:
        if not HAS_POSTGRES:
            print("  (psycopg2 not installed, using SQLite)")
        elif not postgres_url:
            print("  (DATABASE_URL not set, using SQLite)")
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

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS VendorInventory (
            inventory_id SERIAL PRIMARY KEY,
            vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id),
            source_id INTEGER REFERENCES ScrapeSources(source_id),
            stock_status TEXT DEFAULT 'unknown',
            last_updated TEXT,
            UNIQUE(vendor_ingredient_id)
        )
    ''')

    # Seed data (PostgreSQL ON CONFLICT syntax)
    for name, type_, factor, base in [('kg', 'weight', 1.0, 'kg'), ('g', 'weight', 0.001, 'kg'), ('lb', 'weight', 0.45359237, 'kg')]:
        cursor.execute('INSERT INTO Units (name, type, conversion_factor, base_unit) VALUES (%s, %s, %s, %s) ON CONFLICT (name) DO NOTHING', (name, type_, factor, base))

    for name, desc in [('fixed_multiple', 'Must order in exact multiples'), ('fixed_pack', 'Must order specific pack sizes'), ('range', 'Any quantity within min-max')]:
        cursor.execute('INSERT INTO OrderRuleTypes (name, description) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING', (name, desc))

    for name, desc in [('per_unit', 'Price per kg/lb'), ('per_package', 'Fixed price per package'), ('tiered_unit', 'Volume discount per unit'), ('tiered_package', 'Volume discount per package')]:
        cursor.execute('INSERT INTO PricingModels (name, description) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING', (name, desc))

    cursor.execute('INSERT INTO Vendors (name, pricing_model, status) VALUES (%s, %s, %s) ON CONFLICT (name) DO NOTHING', ('IngredientsOnline', 'per_unit', 'active'))
    cursor.execute('INSERT INTO Vendors (name, pricing_model, status) VALUES (%s, %s, %s) ON CONFLICT (name) DO NOTHING', ('BulkSupplements', 'per_package', 'active'))

    for name, state in [('Chino', 'CA'), ('Edison', 'NJ'), ('Southwest', None)]:
        cursor.execute('INSERT INTO Locations (name, state) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING', (name, state))

    conn.commit()
    print("  PostgreSQL database initialized (Supabase)")
    return conn


def init_sqlite_database(db_path: str):
    """Initialize SQLite database with schema (fallback)."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS Units (unit_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, type TEXT NOT NULL, conversion_factor REAL NOT NULL, base_unit TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Categories (category_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, description TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Locations (location_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, state TEXT, is_active INTEGER DEFAULT 1)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Manufacturers (manufacturer_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, status TEXT DEFAULT 'active')''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS OrderRuleTypes (type_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, description TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS PricingModels (model_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, description TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Vendors (vendor_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, pricing_model TEXT, status TEXT DEFAULT 'active')''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Ingredients (ingredient_id INTEGER PRIMARY KEY AUTOINCREMENT, category_id INTEGER REFERENCES Categories(category_id), name TEXT NOT NULL, status TEXT DEFAULT 'active')''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS IngredientVariants (variant_id INTEGER PRIMARY KEY AUTOINCREMENT, ingredient_id INTEGER NOT NULL REFERENCES Ingredients(ingredient_id), manufacturer_id INTEGER REFERENCES Manufacturers(manufacturer_id), variant_name TEXT NOT NULL, status TEXT DEFAULT 'active')''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS ScrapeSources (source_id INTEGER PRIMARY KEY AUTOINCREMENT, vendor_id INTEGER NOT NULL REFERENCES Vendors(vendor_id), product_url TEXT NOT NULL, scraped_at TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS VendorIngredients (vendor_ingredient_id INTEGER PRIMARY KEY AUTOINCREMENT, vendor_id INTEGER NOT NULL REFERENCES Vendors(vendor_id), variant_id INTEGER NOT NULL REFERENCES IngredientVariants(variant_id), sku TEXT, raw_product_name TEXT, shipping_responsibility TEXT, shipping_terms TEXT, current_source_id INTEGER REFERENCES ScrapeSources(source_id), status TEXT DEFAULT 'active', UNIQUE(vendor_id, variant_id, sku))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS PriceTiers (price_tier_id INTEGER PRIMARY KEY AUTOINCREMENT, vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id), pricing_model_id INTEGER NOT NULL REFERENCES PricingModels(model_id), unit_id INTEGER REFERENCES Units(unit_id), source_id INTEGER REFERENCES ScrapeSources(source_id), min_quantity REAL DEFAULT 0, price REAL NOT NULL, original_price REAL, discount_percent REAL, price_per_kg REAL, effective_date TEXT NOT NULL, includes_shipping INTEGER DEFAULT 0)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS OrderRules (rule_id INTEGER PRIMARY KEY AUTOINCREMENT, vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id), rule_type_id INTEGER NOT NULL REFERENCES OrderRuleTypes(type_id), unit_id INTEGER REFERENCES Units(unit_id), base_quantity REAL, min_quantity REAL, effective_date TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS PackagingSizes (package_id INTEGER PRIMARY KEY AUTOINCREMENT, vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id), unit_id INTEGER REFERENCES Units(unit_id), description TEXT, quantity REAL NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS InventoryLocations (inventory_location_id INTEGER PRIMARY KEY AUTOINCREMENT, vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id), location_id INTEGER NOT NULL REFERENCES Locations(location_id), is_primary INTEGER DEFAULT 0, UNIQUE(vendor_ingredient_id, location_id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS InventoryLevels (level_id INTEGER PRIMARY KEY AUTOINCREMENT, inventory_location_id INTEGER NOT NULL REFERENCES InventoryLocations(inventory_location_id), unit_id INTEGER REFERENCES Units(unit_id), source_id INTEGER REFERENCES ScrapeSources(source_id), quantity_available REAL NOT NULL DEFAULT 0, lead_time_days INTEGER, expected_arrival TEXT, stock_status TEXT DEFAULT 'unknown', last_updated TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS VendorInventory (inventory_id INTEGER PRIMARY KEY AUTOINCREMENT, vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients(vendor_ingredient_id), source_id INTEGER REFERENCES ScrapeSources(source_id), stock_status TEXT DEFAULT 'unknown', last_updated TEXT, UNIQUE(vendor_ingredient_id))''')

    cursor.executemany('INSERT OR IGNORE INTO Units (name, type, conversion_factor, base_unit) VALUES (?, ?, ?, ?)', [('kg', 'weight', 1.0, 'kg'), ('g', 'weight', 0.001, 'kg'), ('lb', 'weight', 0.45359237, 'kg')])
    cursor.executemany('INSERT OR IGNORE INTO OrderRuleTypes (name, description) VALUES (?, ?)', [('fixed_multiple', 'Must order in exact multiples'), ('fixed_pack', 'Must order specific pack sizes'), ('range', 'Any quantity within min-max')])
    cursor.executemany('INSERT OR IGNORE INTO PricingModels (name, description) VALUES (?, ?)', [('per_unit', 'Price per kg/lb'), ('per_package', 'Fixed price per package'), ('tiered_unit', 'Volume discount per unit'), ('tiered_package', 'Volume discount per package')])
    cursor.execute('INSERT OR IGNORE INTO Vendors (name, pricing_model, status) VALUES (?, ?, ?)', ('IngredientsOnline', 'per_unit', 'active'))
    cursor.execute('INSERT OR IGNORE INTO Vendors (name, pricing_model, status) VALUES (?, ?, ?)', ('BulkSupplements', 'per_package', 'active'))
    cursor.executemany('INSERT OR IGNORE INTO Locations (name, state) VALUES (?, ?)', [('Chino', 'CA'), ('Edison', 'NJ'), ('Southwest', None)])

    conn.commit()
    print(f"  SQLite database initialized: {db_path}")
    return conn


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


def get_or_create_manufacturer(conn, name: str) -> int:
    """Get existing manufacturer_id or create new one."""
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


def get_or_create_variant(conn, ingredient_id: int, manufacturer_id: int, variant_name: str) -> int:
    """Get existing variant_id or create new one."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(
        f'SELECT variant_id FROM IngredientVariants WHERE ingredient_id = {ph} AND manufacturer_id = {ph} AND variant_name = {ph}',
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
                             sku: str, raw_name: str, source_id: int) -> UpsertResult:
    """Insert or update vendor ingredient, return UpsertResult with tracking info."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    now = datetime.now().isoformat()

    # Check if exists and get current status for reactivation detection
    cursor.execute(
        f'''SELECT vendor_ingredient_id, status, stale_since FROM VendorIngredients
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
            f'''UPDATE VendorIngredients SET raw_product_name = {ph},
               shipping_responsibility = {ph}, current_source_id = {ph},
               last_seen_at = {ph}, status = 'active', stale_since = NULL
               WHERE vendor_ingredient_id = {ph}''',
            (raw_name, BS_BUSINESS_MODEL['shipping_responsibility'],
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
            f'''INSERT INTO VendorIngredients
               (vendor_id, variant_id, sku, raw_product_name, shipping_responsibility,
                current_source_id, last_seen_at, status)
               VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, 'active')
               RETURNING vendor_ingredient_id''',
            (vendor_id, variant_id, sku, raw_name,
             BS_BUSINESS_MODEL['shipping_responsibility'], source_id, now)
        )
        vendor_ingredient_id = cursor.fetchone()[0]
    else:
        cursor.execute(
            f'''INSERT INTO VendorIngredients
               (vendor_id, variant_id, sku, raw_product_name, shipping_responsibility,
                current_source_id, last_seen_at, status)
               VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, 'active')''',
            (vendor_id, variant_id, sku, raw_name,
             BS_BUSINESS_MODEL['shipping_responsibility'], source_id, now)
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
        f'''SELECT price FROM PriceTiers
           WHERE vendor_ingredient_id = {ph}
           ORDER BY effective_date DESC LIMIT 1''',
        (vendor_ingredient_id,)
    )
    row = cursor.fetchone()
    return float(row[0]) if row and row[0] else None


def delete_old_price_tiers(conn, vendor_ingredient_id: int) -> None:
    """Delete existing price tiers for a vendor ingredient (simple upsert approach)."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(f'DELETE FROM PriceTiers WHERE vendor_ingredient_id = {ph}', (vendor_ingredient_id,))


def insert_price_tier(conn, vendor_ingredient_id: int, row_data: dict, source_id: int) -> None:
    """Insert price tier record for BS (per_package pricing)."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    # Get g unit_id
    cursor.execute(f'SELECT unit_id FROM Units WHERE name = {ph}', ('g',))
    unit_row = cursor.fetchone()
    unit_id = unit_row[0] if unit_row else None

    # Get per_package pricing model id
    cursor.execute(f'SELECT model_id FROM PricingModels WHERE name = {ph}', ('per_package',))
    model_row = cursor.fetchone()
    pricing_model_id = model_row[0] if model_row else 2

    # Parse price
    try:
        price = float(row_data.get('price', 0))
    except:
        price = 0

    # Parse compare_at_price
    original_price = None
    compare_at = row_data.get('compare_at_price')
    if compare_at:
        try:
            original_price = float(compare_at)
        except:
            pass

    # Calculate discount percent
    discount_percent = 0
    if original_price and original_price > price:
        discount_percent = ((original_price - price) / original_price) * 100

    cursor.execute(
        f'''INSERT INTO PriceTiers
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


def upsert_order_rule(conn, vendor_ingredient_id: int, pack_size_g: float, scraped_at: str) -> None:
    """Insert or update order rule for BS fixed_pack."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    # Get rule type id for fixed_pack
    cursor.execute(f'SELECT type_id FROM OrderRuleTypes WHERE name = {ph}', ('fixed_pack',))
    type_row = cursor.fetchone()
    rule_type_id = type_row[0] if type_row else 2

    # Get g unit_id
    cursor.execute(f'SELECT unit_id FROM Units WHERE name = {ph}', ('g',))
    unit_row = cursor.fetchone()
    unit_id = unit_row[0] if unit_row else None

    # Delete existing and insert new
    cursor.execute(f'DELETE FROM OrderRules WHERE vendor_ingredient_id = {ph}', (vendor_ingredient_id,))
    cursor.execute(
        f'''INSERT INTO OrderRules
           (vendor_ingredient_id, rule_type_id, unit_id, base_quantity, min_quantity, effective_date)
           VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})''',
        (vendor_ingredient_id, rule_type_id, unit_id, pack_size_g, pack_size_g, scraped_at)
    )


def upsert_packaging_size(conn, vendor_ingredient_id: int, pack_size_g: float, description: str) -> None:
    """Insert or update packaging size for BS variable pack sizes."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    # Get g unit_id
    cursor.execute(f'SELECT unit_id FROM Units WHERE name = {ph}', ('g',))
    unit_row = cursor.fetchone()
    unit_id = unit_row[0] if unit_row else None

    # Delete existing and insert new
    cursor.execute(f'DELETE FROM PackagingSizes WHERE vendor_ingredient_id = {ph}', (vendor_ingredient_id,))
    cursor.execute(
        f'''INSERT INTO PackagingSizes (vendor_ingredient_id, unit_id, description, quantity)
           VALUES ({ph}, {ph}, {ph}, {ph})''',
        (vendor_ingredient_id, unit_id, description, pack_size_g)
    )


def get_existing_stock_status(conn, vendor_ingredient_id: int) -> Optional[str]:
    """Get the existing stock status for a vendor ingredient (for comparison)."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
    cursor.execute(
        f'''SELECT stock_status FROM VendorInventory
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
        # PostgreSQL uses ON CONFLICT for upsert
        cursor.execute(
            f'''INSERT INTO VendorInventory (vendor_ingredient_id, source_id, stock_status, last_updated)
               VALUES ({ph}, {ph}, {ph}, {ph})
               ON CONFLICT (vendor_ingredient_id) DO UPDATE SET source_id = EXCLUDED.source_id, stock_status = EXCLUDED.stock_status, last_updated = EXCLUDED.last_updated''',
            (vendor_ingredient_id, source_id, stock_status, datetime.now().isoformat())
        )
    else:
        cursor.execute(
            f'''INSERT OR REPLACE INTO VendorInventory
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
           FROM VendorIngredients
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
        f'''UPDATE VendorIngredients
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


def save_to_database(conn, rows: List[Dict],
                     stats: Optional['StatsTracker'] = None) -> None:
    """Save processed product rows to the database with change tracking."""
    if not rows:
        return

    cursor = conn.cursor()
    ph = db_placeholder(conn)

    # Get vendor_id for BulkSupplements
    cursor.execute(f'SELECT vendor_id FROM Vendors WHERE name = {ph}', ('BulkSupplements',))
    vendor_row = cursor.fetchone()
    vendor_id = vendor_row[0] if vendor_row else 2

    # All rows for same product share same base info
    first_row = rows[0]
    product_title = first_row.get('product_title', '')
    url = first_row.get('url', '')
    scraped_at = first_row.get('scraped_at', datetime.now().isoformat())
    ingredient_name = first_row.get('ingredient_name', product_title)
    category = first_row.get('category', '')

    # Create source record
    source_id = insert_scrape_source(conn, vendor_id, url, scraped_at)

    # Create category, ingredient, manufacturer, variant
    category_id = get_or_create_category(conn, category)
    ingredient_id = get_or_create_ingredient(conn, ingredient_name, category_id)
    manufacturer_id = get_or_create_manufacturer(conn, 'BulkSupplements')
    variant_id = get_or_create_variant(conn, ingredient_id, manufacturer_id, ingredient_name)

    # Process each variant row
    for row in rows:
        sku = row.get('variant_sku', '')
        pack_size_g = row.get('pack_size_g', 0)
        pack_description = row.get('pack_size_description', '')
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

        # Insert order rule and packaging
        upsert_order_rule(conn, vendor_ingredient_id, pack_size_g, scraped_at)
        upsert_packaging_size(conn, vendor_ingredient_id, pack_size_g, pack_description)

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

        print(f"[{timestamp}] [{self.completed}/{self.total}] ({pct:5.1f}%) "
              f"{item_name[:40]:<40} [{status}] "
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
# Statistics Tracker
# =============================================================================

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
        return 0


# =============================================================================
# Checkpoint Functions
# =============================================================================

def save_checkpoint(processed_handles: List[str], all_data: List[Dict],
                    all_handles: List[str], output_file: str = None) -> None:
    """Save scraping progress to checkpoint file."""
    checkpoint = {
        'processed_handles': processed_handles,
        'all_handles': all_handles,
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

def fetch_with_backoff(url: str, session: requests.Session, is_json: bool = True):
    """Fetch URL with exponential backoff retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, headers=HEADERS, timeout=30)

            if response.status_code == 429:
                # Rate limited - exponential backoff
                delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                print(f"    Rate limited, backoff {delay}s...", flush=True)
                time.sleep(delay)
                continue

            response.raise_for_status()

            if is_json:
                return response.json()
            else:
                return response.text

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                return None
            # Exponential backoff
            delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
            time.sleep(delay)

    return None


# =============================================================================
# Product Discovery
# =============================================================================

def discover_products(session: requests.Session) -> List[str]:
    """Discover all product handles using /products.json pagination."""
    all_handles = []
    page_num = 1

    print("\n" + "=" * 60, flush=True)
    print("PHASE 1: Discovering products via /products.json", flush=True)
    print("=" * 60, flush=True)

    while True:
        url = f"{BASE_URL}/products.json?limit=250&page={page_num}"
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] Fetching page {page_num}...", end=" ", flush=True)

        try:
            data = fetch_with_backoff(url, session, is_json=True)

            if not data:
                print("error fetching", flush=True)
                break

            products = data.get('products', [])

            if not products:
                print("empty - done!", flush=True)
                break

            page_handles = [p.get('handle') for p in products if p.get('handle')]
            all_handles.extend(page_handles)

            print(f"found {len(products)} products (total: {len(all_handles)})", flush=True)

            page_num += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"error: {e}", flush=True)
            if page_num > 1:
                break
            raise

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
# Availability Extraction
# =============================================================================

def extract_availability_from_html(html: str) -> Dict[str, bool]:
    """
    Extract variant availability from HTML page's schema.org JSON-LD data.
    Returns dict mapping SKU to availability (True = in stock).
    """
    availability = {}

    try:
        soup = BeautifulSoup(html, 'html.parser')

        # Find all script tags with JSON-LD
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)

                # Handle both single objects and arrays
                items = data if isinstance(data, list) else [data]

                for item in items:
                    if item.get('@type') == 'Product':
                        offers = item.get('offers', {})

                        # Handle array of offers (multiple variants)
                        if isinstance(offers, list):
                            for offer in offers:
                                sku = offer.get('sku', '')
                                avail = offer.get('availability', '')
                                is_available = 'InStock' in avail
                                if sku:
                                    availability[sku] = is_available

                        # Handle single offer
                        elif isinstance(offers, dict):
                            sku = offers.get('sku', '')
                            avail = offers.get('availability', '')
                            is_available = 'InStock' in avail
                            if sku:
                                availability[sku] = is_available

            except json.JSONDecodeError:
                continue

    except Exception:
        pass

    return availability


# =============================================================================
# Product Scraping
# =============================================================================

def parse_product(product_data: Dict, availability: Dict[str, bool]) -> List[Dict]:
    """Parse a product's JSON data into rows (one per powder variant only)."""
    rows = []
    timestamp = datetime.now().isoformat()

    try:
        product = product_data.get('product', {})

        product_id = product.get('id')
        title = product.get('title', 'Unknown')
        handle = product.get('handle', '')
        product_type = product.get('product_type', '')
        product_url = f"{BASE_URL}/products/{handle}"

        variants = product.get('variants', [])

        for variant in variants:
            option1 = variant.get('option1', '')
            option2 = variant.get('option2', '')

            # FILTER: Only include powder variants
            if option1.lower() != 'powder':
                continue

            sku = variant.get('sku', '')
            is_available = availability.get(sku, None)

            # Parse pack size from option2
            pack_size_g = parse_pack_size_g(option2)
            packaging_kg = parse_packaging_kg(pack_size_g)

            # Calculate price per kg
            try:
                price_val = float(variant.get('price', 0))
            except:
                price_val = 0
            price_per_kg = calculate_price_per_kg(price_val, pack_size_g)

            row = {
                'product_id': product_id,
                'product_title': title,
                'ingredient_name': title,  # Same as product_title for BS
                'category': product_type,  # Renamed from product_type
                'variant_id': variant.get('id'),
                'variant_sku': sku,
                'packaging': option2,  # Human-readable packaging description
                'packaging_kg': packaging_kg,  # Numeric kg value
                'pack_size_g': pack_size_g,
                'pack_size_description': option2,
                'price': variant.get('price', ''),
                'compare_at_price': variant.get('compare_at_price', ''),
                'price_per_kg': round(price_per_kg, 2) if price_per_kg else None,
                'in_stock': is_available,
                'stock_status': convert_stock_status(is_available),
                'order_rule_type': BS_BUSINESS_MODEL['order_rule_type'],
                'shipping_responsibility': BS_BUSINESS_MODEL['shipping_responsibility'],
                'url': product_url,  # Renamed from product_url
                'scraped_at': timestamp,
            }
            rows.append(row)

        return rows

    except Exception:
        return []


def scrape_product(handle: str, session: requests.Session) -> List[Dict]:
    """Scrape a single product - fetch both JSON and HTML for availability."""
    json_url = f"{BASE_URL}/products/{handle}.json"
    html_url = f"{BASE_URL}/products/{handle}"

    # Fetch product JSON
    product_data = fetch_with_backoff(json_url, session, is_json=True)

    if not product_data:
        return []

    # Fetch HTML for availability
    html = fetch_with_backoff(html_url, session, is_json=False)

    availability = {}
    if html:
        availability = extract_availability_from_html(html)

    return parse_product(product_data, availability)


# =============================================================================
# Main
# =============================================================================

def save_to_csv(data: List[Dict], output_dir: str = "output") -> str:
    """Save scraped data to a timestamped CSV file."""
    if not data:
        print("No data to save")
        return ""

    df = pd.DataFrame(data)

    # Reorder columns per scraper-specifications.md
    priority_cols = [
        'product_id', 'product_title', 'ingredient_name', 'category',
        'variant_id', 'variant_sku',
        'packaging', 'packaging_kg',  # NEW: packaging info
        'pack_size_g', 'pack_size_description',
        'price', 'compare_at_price', 'price_per_kg',
        'in_stock', 'stock_status',
        'order_rule_type', 'shipping_responsibility',
        'url', 'scraped_at'
    ]
    other_cols = [c for c in df.columns if c not in priority_cols]
    ordered_cols = [c for c in priority_cols if c in df.columns] + other_cols
    df = df[ordered_cols]

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"bulksupplements_products_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False)
    print(f"\nSaved {len(data)} rows to: {filepath}")

    return filepath


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='BulkSupplements.com Product Scraper'
    )
    parser.add_argument('--resume', action='store_true',
                        help='Resume from checkpoint if available')
    parser.add_argument('--max-products', type=int, default=None,
                        help='Maximum products to scrape (for testing)')
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs("output", exist_ok=True)

    print("=" * 60, flush=True)
    print("BulkSupplements.com Product Scraper", flush=True)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print("=" * 60, flush=True)

    # Track scrape start time for staleness detection
    scrape_start_time = datetime.now().isoformat()

    # Create session for connection pooling
    session = requests.Session()

    # Check for checkpoint
    checkpoint = load_checkpoint()
    processed_handles = []
    all_data = []
    handles = []
    output_file = None

    if checkpoint and args.resume:
        print(f"\nFound checkpoint from {checkpoint['timestamp']}", flush=True)
        print(f"  Processed: {len(checkpoint['processed_handles'])} products", flush=True)
        print(f"  Remaining: {len(checkpoint['all_handles']) - len(checkpoint['processed_handles'])} products", flush=True)

        resume = input("\nResume from checkpoint? [Y/n]: ").strip().lower()
        if resume != 'n':
            processed_handles = checkpoint['processed_handles']
            handles = checkpoint['all_handles']
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
    if not handles:
        handles = discover_products(session)

        if not handles:
            print("No products discovered. Exiting.", flush=True)
            sys.exit(1)

    # Apply max products limit for testing
    if args.max_products:
        handles = handles[:args.max_products]
        print(f"Limited to {args.max_products} products for testing", flush=True)

    # Generate output filename
    if not output_file:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        output_file = f"bulksupplements_products_{timestamp}.csv"

    # Initialize database with auto-reconnect wrapper
    db_path = DATABASE_FILE
    print(f"\nInitializing database: {db_path}")
    db_wrapper = DatabaseConnection(db_path)
    db_wrapper.connect()
    print("Database initialized")

    # Initialize StatsTracker
    stats = StatsTracker(
        vendor_id=4,  # BulkSupplements
        is_full_scrape=(args.max_products is None),
        max_products_limit=args.max_products
    )
    stats.products_discovered = len(handles)

    # Cleanup old alerts (30 day retention)
    try:
        cleanup_old_alerts(db_wrapper.connection)
    except Exception:
        pass

    # Filter out already processed handles
    if processed_handles:
        remaining_handles = [h for h in handles if h not in processed_handles]
        print(f"\nResuming: {len(remaining_handles)} products remaining", flush=True)
    else:
        remaining_handles = handles

    # Scrape products
    print("\n" + "=" * 60, flush=True)
    print("PHASE 2: Scraping product details", flush=True)
    print("=" * 60, flush=True)

    total_handles = len(handles)
    progress = ProgressTracker(len(remaining_handles))

    for i, handle in enumerate(remaining_handles, 1):
        try:
            rows = scrape_product(handle, session)
            if rows:
                all_data.extend(rows)
                # Save to database with auto-retry
                db_wrapper.execute_with_retry(save_to_database, rows, stats)
                processed_handles.append(handle)
                progress.update(success=True, item_name=handle)
                stats.products_processed += 1

                # Print detailed variant table
                print(f"    → {len(rows)} variants", flush=True)
                details = format_product_details(rows)
                if details:
                    print(details, flush=True)
                print(flush=True)  # Blank line between products
            else:
                progress.update(success=False, item_name=handle, status="SKIPPED-NO_POWDER")
                stats.products_skipped += 1
                print(flush=True)  # Blank line for skipped products too
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            progress.update(success=False, item_name=handle, status="ERROR")
            stats.record_failure(handle, "HTTP", str(e))

        # Save checkpoint periodically
        if len(processed_handles) % CHECKPOINT_INTERVAL == 0:
            print(f"\n>>> Checkpoint saved: {len(processed_handles)} products <<<\n", flush=True)
            # Commit database
            db_wrapper.commit()
            save_checkpoint(processed_handles, all_data, handles, output_file)

        # Rate limiting
        if i < len(remaining_handles):
            time.sleep(REQUEST_DELAY)

    progress.summary()

    # Save final results
    print("\n" + "=" * 60, flush=True)
    print("PHASE 3: Saving results", flush=True)
    print("=" * 60, flush=True)

    if all_data:
        filepath = save_to_csv(all_data)

        # Final database commit
        db_wrapper.commit()

        # Mark stale variants (only for full scrapes, not --max-products)
        if not args.max_products:
            print("\nChecking for stale products...", flush=True)
            stale_variants = db_wrapper.execute_with_retry(
                mark_stale_variants, 4, scrape_start_time, stats  # vendor_id=4 for BulkSupplements
            )
            db_wrapper.commit()

        # Save scrape run and alerts, then print report
        try:
            save_scrape_run(db_wrapper.connection, stats)
            save_alerts(db_wrapper.connection, stats)
            db_wrapper.commit()
        except Exception as e:
            print(f"  Note: Could not persist run data: {e}")

        stats.print_report()

        db_wrapper.close()

        clear_checkpoint()

        print("\n" + "=" * 60, flush=True)
        print("SCRAPING COMPLETE", flush=True)
        print("=" * 60, flush=True)
        print(f"Total products scraped: {len(set(d['product_id'] for d in all_data))}", flush=True)
        print(f"Total powder variants extracted: {len(all_data)}", flush=True)
        print(f"Output file: {filepath}", flush=True)
        print(f"Database file: {db_path}", flush=True)

        # Preview
        print("\nData preview:", flush=True)
        df = pd.DataFrame(all_data)
        preview_cols = ['product_title', 'pack_size_description', 'price', 'price_per_kg', 'stock_status']
        available_cols = [c for c in preview_cols if c in df.columns]
        print(df[available_cols].head(10).to_string(), flush=True)

        # Stats
        print("\n" + "-" * 40, flush=True)
        print("Statistics:", flush=True)
        print(f"  Unique products: {df['product_id'].nunique()}", flush=True)
        print(f"  Total powder variants: {len(df)}", flush=True)
        if 'stock_status' in df.columns:
            in_stock = (df['stock_status'] == 'in_stock').sum()
            out_of_stock = len(df) - in_stock
            print(f"  In stock: {in_stock}", flush=True)
            print(f"  Out of stock: {out_of_stock}", flush=True)
        try:
            prices = df['price'].astype(float)
            print(f"  Price range: ${prices.min():.2f} - ${prices.max():.2f}", flush=True)
        except:
            pass
        if 'price_per_kg' in df.columns:
            try:
                ppk = df['price_per_kg'].dropna().astype(float)
                if len(ppk) > 0:
                    print(f"  Price/kg range: ${ppk.min():.2f} - ${ppk.max():.2f}", flush=True)
            except:
                pass
    else:
        print("\nNo data was extracted.", flush=True)
        # Still print stats and close database
        stats.print_report()
        db_wrapper.close()


if __name__ == "__main__":
    main()
