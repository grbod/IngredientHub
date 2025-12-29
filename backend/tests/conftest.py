"""
Pytest fixtures and test infrastructure for scraper database tests.
"""
import pytest
import sqlite3
import os
import sys
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def sqlite_conn():
    """In-memory SQLite database for isolated testing."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    setup_test_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def postgres_conn():
    """Test PostgreSQL connection (requires TEST_DATABASE_URL env var)."""
    import psycopg2
    url = os.environ.get('TEST_DATABASE_URL')
    if not url:
        pytest.skip("TEST_DATABASE_URL not set")
    conn = psycopg2.connect(url)
    setup_test_schema_postgres(conn)
    yield conn
    conn.rollback()  # Don't persist test data
    conn.close()


def setup_test_schema(conn):
    """Create minimal schema for SQLite testing."""
    cursor = conn.cursor()
    cursor.executescript('''
        -- Core lookup tables
        CREATE TABLE IF NOT EXISTS vendors (
            vendor_id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            pricing_model TEXT,
            status TEXT DEFAULT 'active'
        );

        CREATE TABLE IF NOT EXISTS categories (
            category_id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        );

        CREATE TABLE IF NOT EXISTS ingredients (
            ingredient_id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            category_id INTEGER
        );

        CREATE TABLE IF NOT EXISTS manufacturers (
            manufacturer_id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        );

        CREATE TABLE IF NOT EXISTS ingredientvariants (
            variant_id INTEGER PRIMARY KEY,
            ingredient_id INTEGER,
            manufacturer_id INTEGER,
            variant_name TEXT,
            UNIQUE(ingredient_id, manufacturer_id, variant_name)
        );

        CREATE TABLE IF NOT EXISTS scrapesources (
            source_id INTEGER PRIMARY KEY,
            vendor_id INTEGER,
            product_url TEXT,
            scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS vendoringredients (
            vendor_ingredient_id INTEGER PRIMARY KEY,
            vendor_id INTEGER,
            variant_id INTEGER,
            sku TEXT,
            raw_product_name TEXT,
            shipping_responsibility TEXT,
            shipping_terms TEXT,
            current_source_id INTEGER,
            last_seen_at TEXT,
            status TEXT DEFAULT 'active',
            stale_since TEXT,
            UNIQUE(vendor_id, variant_id, sku)
        );

        CREATE TABLE IF NOT EXISTS pricetiers (
            tier_id INTEGER PRIMARY KEY,
            vendor_ingredient_id INTEGER,
            pricing_model_id INTEGER,
            unit_id INTEGER,
            source_id INTEGER,
            min_quantity REAL,
            price REAL,
            original_price REAL,
            discount_percent REAL,
            price_per_kg REAL,
            effective_date TEXT,
            includes_shipping INTEGER
        );

        CREATE TABLE IF NOT EXISTS packagingsizes (
            packaging_id INTEGER PRIMARY KEY,
            vendor_ingredient_id INTEGER,
            unit_id INTEGER,
            description TEXT,
            quantity REAL
        );

        CREATE TABLE IF NOT EXISTS orderrules (
            rule_id INTEGER PRIMARY KEY,
            vendor_ingredient_id INTEGER,
            rule_type_id INTEGER,
            unit_id INTEGER,
            base_quantity REAL,
            min_quantity REAL,
            effective_date TEXT
        );

        CREATE TABLE IF NOT EXISTS vendorinventory (
            inventory_id INTEGER PRIMARY KEY,
            vendor_ingredient_id INTEGER UNIQUE,
            source_id INTEGER,
            stock_status TEXT,
            last_updated TEXT
        );

        CREATE TABLE IF NOT EXISTS units (
            unit_id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            conversion_factor REAL
        );

        CREATE TABLE IF NOT EXISTS pricingmodels (
            model_id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        );

        CREATE TABLE IF NOT EXISTS orderruletypes (
            type_id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        );

        -- Flat tables for each scraper
        CREATE TABLE IF NOT EXISTS BSPricing (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            product_title TEXT,
            variant_id INTEGER UNIQUE,
            variant_sku TEXT,
            packaging TEXT,
            pack_size_g REAL,
            price REAL,
            compare_at_price REAL,
            price_per_kg REAL,
            available INTEGER,
            stock_status TEXT,
            url TEXT,
            scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS "BoxNutraPricing" (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            product_title TEXT,
            variant_id INTEGER UNIQUE,
            variant_sku TEXT,
            packaging TEXT,
            packaging_kg REAL,
            pack_size_g REAL,
            price REAL,
            compare_at_price REAL,
            price_per_kg REAL,
            available INTEGER,
            stock_status TEXT,
            order_rule_type TEXT,
            shipping_responsibility TEXT,
            url TEXT,
            scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS TrafaPricing (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            product_code TEXT,
            product_name TEXT,
            ingredient_name TEXT,
            category TEXT,
            size_id TEXT,
            size_name TEXT,
            size_kg REAL,
            price REAL,
            price_per_kg REAL,
            stock_status TEXT,
            order_rule_type TEXT,
            shipping_responsibility TEXT,
            url TEXT,
            scraped_at TEXT,
            UNIQUE(product_id, size_id)
        );

        -- Seed data for vendors
        INSERT INTO vendors (vendor_id, name, pricing_model) VALUES
            (1, 'IngredientsOnline', 'per_unit'),
            (4, 'BulkSupplements', 'per_package'),
            (25, 'BoxNutra', 'per_package'),
            (26, 'TrafaPharma', 'fixed_pack');

        -- Seed data for units
        INSERT INTO units (unit_id, name, conversion_factor) VALUES
            (1, 'kg', 1.0),
            (2, 'g', 0.001),
            (3, 'lb', 0.453592);

        -- Seed data for pricing models
        INSERT INTO pricingmodels (model_id, name) VALUES
            (1, 'per_unit'),
            (2, 'per_package');

        -- Seed data for order rule types
        INSERT INTO orderruletypes (type_id, name) VALUES
            (1, 'fixed_multiple'),
            (2, 'fixed_pack');
    ''')
    conn.commit()


def setup_test_schema_postgres(conn):
    """Create minimal schema for PostgreSQL testing."""
    cursor = conn.cursor()

    # Drop and recreate test tables
    cursor.execute('DROP TABLE IF EXISTS pricetiers CASCADE')
    cursor.execute('DROP TABLE IF EXISTS packagingsizes CASCADE')
    cursor.execute('DROP TABLE IF EXISTS orderrules CASCADE')
    cursor.execute('DROP TABLE IF EXISTS vendorinventory CASCADE')
    cursor.execute('DROP TABLE IF EXISTS vendoringredients CASCADE')
    cursor.execute('DROP TABLE IF EXISTS scrapesources CASCADE')
    cursor.execute('DROP TABLE IF EXISTS ingredientvariants CASCADE')
    cursor.execute('DROP TABLE IF EXISTS ingredients CASCADE')
    cursor.execute('DROP TABLE IF EXISTS manufacturers CASCADE')
    cursor.execute('DROP TABLE IF EXISTS categories CASCADE')

    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS categories (
            category_id SERIAL PRIMARY KEY,
            name TEXT UNIQUE
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ingredients (
            ingredient_id SERIAL PRIMARY KEY,
            name TEXT UNIQUE,
            category_id INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS manufacturers (
            manufacturer_id SERIAL PRIMARY KEY,
            name TEXT UNIQUE
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ingredientvariants (
            variant_id SERIAL PRIMARY KEY,
            ingredient_id INTEGER,
            manufacturer_id INTEGER,
            variant_name TEXT,
            UNIQUE(ingredient_id, manufacturer_id, variant_name)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scrapesources (
            source_id SERIAL PRIMARY KEY,
            vendor_id INTEGER,
            product_url TEXT,
            scraped_at TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS vendoringredients (
            vendor_ingredient_id SERIAL PRIMARY KEY,
            vendor_id INTEGER,
            variant_id INTEGER,
            sku TEXT,
            raw_product_name TEXT,
            shipping_responsibility TEXT,
            shipping_terms TEXT,
            current_source_id INTEGER,
            last_seen_at TEXT,
            status TEXT DEFAULT 'active',
            stale_since TEXT,
            UNIQUE(vendor_id, variant_id, sku)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pricetiers (
            tier_id SERIAL PRIMARY KEY,
            vendor_ingredient_id INTEGER,
            pricing_model_id INTEGER,
            unit_id INTEGER,
            source_id INTEGER,
            min_quantity REAL,
            price REAL,
            original_price REAL,
            discount_percent REAL,
            price_per_kg REAL,
            effective_date TEXT,
            includes_shipping INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS packagingsizes (
            packaging_id SERIAL PRIMARY KEY,
            vendor_ingredient_id INTEGER,
            unit_id INTEGER,
            description TEXT,
            quantity REAL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orderrules (
            rule_id SERIAL PRIMARY KEY,
            vendor_ingredient_id INTEGER,
            rule_type_id INTEGER,
            unit_id INTEGER,
            base_quantity REAL,
            min_quantity REAL,
            effective_date TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS vendorinventory (
            inventory_id SERIAL PRIMARY KEY,
            vendor_ingredient_id INTEGER UNIQUE,
            source_id INTEGER,
            stock_status TEXT,
            last_updated TEXT
        )
    ''')

    conn.commit()


# Helper functions for tests
def create_test_vendor_ingredient(conn, vendor_id=4, variant_id=1, sku='TEST-SKU'):
    """Helper to create a vendor ingredient for testing."""
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO vendoringredients (vendor_id, variant_id, sku, status)
        VALUES (?, ?, ?, 'active')
    ''', (vendor_id, variant_id, sku))
    vi_id = cursor.lastrowid
    conn.commit()
    return vi_id


def create_test_ingredient(conn, name='Test Ingredient'):
    """Helper to create an ingredient for testing."""
    cursor = conn.cursor()
    cursor.execute('INSERT INTO ingredients (name) VALUES (?)', (name,))
    ing_id = cursor.lastrowid
    conn.commit()
    return ing_id


def create_test_manufacturer(conn, name='Test Manufacturer'):
    """Helper to create a manufacturer for testing."""
    cursor = conn.cursor()
    cursor.execute('INSERT INTO manufacturers (name) VALUES (?)', (name,))
    mfr_id = cursor.lastrowid
    conn.commit()
    return mfr_id
