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
from typing import List, Dict, Optional, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Database support - PostgreSQL (Supabase) or SQLite fallback
try:
    import psycopg2
    import psycopg2.extras
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False
    import sqlite3


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
CHECKPOINT_FILE = ".bulksupplements_checkpoint.json"

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
    return os.environ.get('SUPABASE_DB_URL')


def is_postgres(conn) -> bool:
    """Check if connection is PostgreSQL."""
    return HAS_POSTGRES and hasattr(conn, 'info')


def db_placeholder(conn) -> str:
    """Return the correct placeholder for the database type."""
    return '%s' if is_postgres(conn) else '?'


def init_database(db_path: str = None) -> DbConnection:
    """
    Initialize database with schema and seed data.
    Uses PostgreSQL (Supabase) if available, falls back to SQLite.
    """
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

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS BSPricing (
            id SERIAL PRIMARY KEY,
            product_id TEXT,
            product_title TEXT,
            ingredient_name TEXT,
            category TEXT,
            variant_id TEXT,
            variant_sku TEXT,
            packaging TEXT,
            packaging_kg REAL,
            pack_size_g REAL,
            price REAL,
            price_per_kg REAL,
            stock_status TEXT,
            url TEXT,
            scraped_at TEXT
        )
    ''')

    conn.commit()
    print("  PostgreSQL database initialized (Supabase)")
    return conn


def init_sqlite_database(db_path: str):
    """Initialize SQLite database with schema (fallback)."""
    import sqlite3
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

    cursor.execute('''CREATE TABLE IF NOT EXISTS BSPricing (id INTEGER PRIMARY KEY AUTOINCREMENT, product_id TEXT, product_title TEXT, ingredient_name TEXT, category TEXT, variant_id TEXT, variant_sku TEXT, packaging TEXT, packaging_kg REAL, pack_size_g REAL, price REAL, price_per_kg REAL, stock_status TEXT, url TEXT, scraped_at TEXT)''')

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
                             sku: str, raw_name: str, source_id: int) -> int:
    """Insert or update vendor ingredient, return vendor_ingredient_id."""
    cursor = conn.cursor()
    ph = db_placeholder(conn)
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
               shipping_responsibility = {ph}, current_source_id = {ph}
               WHERE vendor_ingredient_id = {ph}''',
            (raw_name, BS_BUSINESS_MODEL['shipping_responsibility'],
             source_id, vendor_ingredient_id)
        )
        return vendor_ingredient_id
    if is_postgres(conn):
        cursor.execute(
            f'''INSERT INTO VendorIngredients
               (vendor_id, variant_id, sku, raw_product_name, shipping_responsibility, current_source_id)
               VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}) RETURNING vendor_ingredient_id''',
            (vendor_id, variant_id, sku, raw_name,
             BS_BUSINESS_MODEL['shipping_responsibility'], source_id)
        )
        return cursor.fetchone()[0]
    else:
        cursor.execute(
            f'''INSERT INTO VendorIngredients
               (vendor_id, variant_id, sku, raw_product_name, shipping_responsibility, current_source_id)
               VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})''',
            (vendor_id, variant_id, sku, raw_name,
             BS_BUSINESS_MODEL['shipping_responsibility'], source_id)
        )
        return cursor.lastrowid


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


def save_to_database(conn, rows: List[Dict]) -> None:
    """Save processed product rows to the database."""
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

        # Create/update vendor ingredient
        vendor_ingredient_id = upsert_vendor_ingredient(
            conn, vendor_id, variant_id, sku, product_title, source_id
        )

        # Delete old price tier and insert new
        delete_old_price_tiers(conn, vendor_ingredient_id)
        insert_price_tier(conn, vendor_ingredient_id, row, source_id)

        # Insert order rule and packaging
        upsert_order_rule(conn, vendor_ingredient_id, pack_size_g, scraped_at)
        upsert_packaging_size(conn, vendor_ingredient_id, pack_size_g, pack_description)

        # Insert inventory status
        upsert_inventory_simple(conn, vendor_ingredient_id, stock_status, source_id)


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

def save_to_csv(data: List[Dict], output_dir: str = ".") -> str:
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

    print("=" * 60, flush=True)
    print("BulkSupplements.com Product Scraper", flush=True)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print("=" * 60, flush=True)

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

    # Initialize SQLite database (shared with IO scraper)
    db_path = DATABASE_FILE
    print(f"\nInitializing database: {db_path}")
    db_conn = init_database(db_path)
    print("Database initialized")

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
                # Save to database
                try:
                    save_to_database(db_conn, rows)
                except Exception as db_error:
                    print(f"    DB error: {db_error}", flush=True)
                processed_handles.append(handle)
                progress.update(success=True, item_name=handle)

                # Print detailed variant table
                print(f"    → {len(rows)} variants", flush=True)
                details = format_product_details(rows)
                if details:
                    print(details, flush=True)
                print(flush=True)  # Blank line between products
            else:
                progress.update(success=False, item_name=handle, status="SKIPPED-NO_POWDER")
                print(flush=True)  # Blank line for skipped products too
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            progress.update(success=False, item_name=handle, status="ERROR")

        # Save checkpoint periodically
        if len(processed_handles) % CHECKPOINT_INTERVAL == 0:
            print(f"\n>>> Checkpoint saved: {len(processed_handles)} products <<<\n", flush=True)
            # Commit database
            db_conn.commit()
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

        # Final database commit and close
        db_conn.commit()
        db_conn.close()

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
        # Still close database
        db_conn.close()


if __name__ == "__main__":
    main()
