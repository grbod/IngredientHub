"""
Tests for upsert functions across all scrapers.
Tests insert-or-update logic with proper collision handling.
"""
import pytest
import time
from datetime import datetime


class TestUpsertVendorIngredient:
    """Test vendor ingredient upsert with (vendor_id, variant_id, sku) key."""

    def test_insert_new_vendor_ingredient_bs(self, sqlite_conn):
        """BulkSupplements: First upsert inserts new record with all fields."""
        from bulksupplements_scraper import upsert_vendor_ingredient

        # First create a source record
        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (4, 'https://test.com', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        vi_id = upsert_vendor_ingredient(
            sqlite_conn,
            vendor_id=4,
            variant_id=100,
            sku='TEST-SKU-001',
            raw_name='Test Product',
            source_id=source_id
        )

        cursor.execute('''
            SELECT vendor_id, variant_id, sku, raw_product_name, status, last_seen_at
            FROM vendoringredients WHERE vendor_ingredient_id = ?
        ''', (vi_id,))
        row = cursor.fetchone()

        assert row['vendor_id'] == 4
        assert row['variant_id'] == 100
        assert row['sku'] == 'TEST-SKU-001'
        assert row['raw_product_name'] == 'Test Product'
        assert row['status'] == 'active'
        assert row['last_seen_at'] is not None

    def test_insert_new_vendor_ingredient_boxnutra(self, sqlite_conn):
        """BoxNutra: First upsert inserts new record with all fields."""
        from boxnutra_scraper import upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (25, 'https://boxnutra.com/test', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        vi_id = upsert_vendor_ingredient(
            sqlite_conn,
            vendor_id=25,
            variant_id=200,
            sku='BN-TEST-001',
            raw_name='BoxNutra Test Product',
            source_id=source_id
        )

        cursor.execute('''
            SELECT vendor_id, variant_id, sku, raw_product_name, status, last_seen_at
            FROM vendoringredients WHERE vendor_ingredient_id = ?
        ''', (vi_id,))
        row = cursor.fetchone()

        assert row['vendor_id'] == 25
        assert row['variant_id'] == 200
        assert row['sku'] == 'BN-TEST-001'
        assert row['status'] == 'active'

    def test_insert_new_vendor_ingredient_trafapharma(self, sqlite_conn):
        """TrafaPharma: First upsert inserts new record."""
        from trafapharma_scraper import upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (26, 'https://trafapharma.com/test', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        result = upsert_vendor_ingredient(
            sqlite_conn,
            vendor_id=26,
            variant_id=300,
            sku='123-1',
            raw_name='TrafaPharma Test Product',
            source_id=source_id
        )
        vi_id = result.vendor_ingredient_id

        cursor.execute('''
            SELECT vendor_id, variant_id, sku, status
            FROM vendoringredients WHERE vendor_ingredient_id = ?
        ''', (vi_id,))
        row = cursor.fetchone()

        assert row['vendor_id'] == 26
        assert row['sku'] == '123-1'
        assert row['status'] == 'active'

    def test_update_existing_vendor_ingredient(self, sqlite_conn):
        """Second upsert updates fields, same ID returned."""
        from bulksupplements_scraper import upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (4, 'https://test.com', ?)
        ''', (datetime.now().isoformat(),))
        source_id1 = cursor.lastrowid
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (4, 'https://test.com', ?)
        ''', (datetime.now().isoformat(),))
        source_id2 = cursor.lastrowid
        sqlite_conn.commit()

        id1 = upsert_vendor_ingredient(sqlite_conn, 4, 100, 'SKU-A', 'Name V1', source_id1)
        id2 = upsert_vendor_ingredient(sqlite_conn, 4, 100, 'SKU-A', 'Name V2', source_id2)

        assert id1 == id2

        cursor.execute('SELECT raw_product_name, current_source_id FROM vendoringredients WHERE vendor_ingredient_id = ?', (id1,))
        row = cursor.fetchone()
        assert row[0] == 'Name V2'  # Updated
        assert row[1] == source_id2  # New source_id

    def test_last_seen_at_updated_on_upsert(self, sqlite_conn):
        """last_seen_at timestamp refreshes on each upsert."""
        from bulksupplements_scraper import upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (4, 'https://test.com', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        upsert_vendor_ingredient(sqlite_conn, 4, 100, 'SKU-B', 'Product', source_id)

        cursor.execute('SELECT last_seen_at FROM vendoringredients WHERE sku = ?', ('SKU-B',))
        time1 = cursor.fetchone()[0]

        time.sleep(0.01)  # Small delay

        upsert_vendor_ingredient(sqlite_conn, 4, 100, 'SKU-B', 'Product', source_id)
        cursor.execute('SELECT last_seen_at FROM vendoringredients WHERE sku = ?', ('SKU-B',))
        time2 = cursor.fetchone()[0]

        assert time2 > time1  # Timestamp updated

    def test_status_reset_to_active_on_upsert(self, sqlite_conn):
        """Previously inactive variant becomes active when seen again."""
        from bulksupplements_scraper import upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (4, 'https://test.com', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        # Create and manually set to inactive
        vi_id = upsert_vendor_ingredient(sqlite_conn, 4, 100, 'SKU-INACTIVE', 'Old', source_id)
        cursor.execute('UPDATE vendoringredients SET status = ? WHERE vendor_ingredient_id = ?',
                      ('inactive', vi_id))
        sqlite_conn.commit()

        # Upsert again - should reset to active
        vi_id2 = upsert_vendor_ingredient(sqlite_conn, 4, 100, 'SKU-INACTIVE', 'Renewed', source_id)

        cursor.execute('SELECT status FROM vendoringredients WHERE vendor_ingredient_id = ?', (vi_id2,))
        assert cursor.fetchone()[0] == 'active'

    def test_different_sku_same_variant_creates_new(self, sqlite_conn):
        """Same (vendor_id, variant_id) but different SKU = new record."""
        from bulksupplements_scraper import upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (4, 'https://test.com', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        id1 = upsert_vendor_ingredient(sqlite_conn, 4, 100, 'SKU-100G', 'Product 100g', source_id)
        id2 = upsert_vendor_ingredient(sqlite_conn, 4, 100, 'SKU-500G', 'Product 500g', source_id)

        assert id1 != id2  # Different SKU = different vendor_ingredient


class TestUpsertInventorySimple:
    """Test simple inventory upsert (single status per vendor_ingredient)."""

    def test_insert_inventory_status(self, sqlite_conn):
        """Creates inventory record for vendor_ingredient."""
        from bulksupplements_scraper import upsert_inventory_simple

        # First create a vendor_ingredient
        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO vendoringredients (vendor_id, variant_id, sku, status)
            VALUES (4, 1, 'TEST', 'active')
        ''')
        vi_id = cursor.lastrowid
        sqlite_conn.commit()

        upsert_inventory_simple(sqlite_conn, vi_id, 'in_stock', 1)

        cursor.execute('SELECT stock_status FROM vendorinventory WHERE vendor_ingredient_id = ?', (vi_id,))
        assert cursor.fetchone()[0] == 'in_stock'

    def test_update_inventory_status(self, sqlite_conn):
        """Updates existing inventory record (no duplicate)."""
        from bulksupplements_scraper import upsert_inventory_simple

        cursor = sqlite_conn.cursor()
        cursor.execute('INSERT INTO vendoringredients (vendor_id, variant_id, sku) VALUES (4, 1, "X")')
        vi_id = cursor.lastrowid
        sqlite_conn.commit()

        upsert_inventory_simple(sqlite_conn, vi_id, 'in_stock', 1)
        upsert_inventory_simple(sqlite_conn, vi_id, 'out_of_stock', 2)

        cursor.execute('SELECT COUNT(*) FROM vendorinventory WHERE vendor_ingredient_id = ?', (vi_id,))
        assert cursor.fetchone()[0] == 1  # Only one record

        cursor.execute('SELECT stock_status FROM vendorinventory WHERE vendor_ingredient_id = ?', (vi_id,))
        assert cursor.fetchone()[0] == 'out_of_stock'


class TestUpsertPackagingSize:
    """Test packaging size upsert (delete-then-insert pattern)."""

    def test_replaces_existing_packaging(self, sqlite_conn):
        """Packaging is replaced, not accumulated."""
        from bulksupplements_scraper import upsert_packaging_size

        cursor = sqlite_conn.cursor()
        cursor.execute('INSERT INTO vendoringredients (vendor_id, variant_id, sku) VALUES (4, 1, "Y")')
        vi_id = cursor.lastrowid
        sqlite_conn.commit()

        upsert_packaging_size(sqlite_conn, vi_id, 100.0, '100 Grams')
        upsert_packaging_size(sqlite_conn, vi_id, 500.0, '500 Grams')

        cursor.execute('SELECT quantity, description FROM packagingsizes WHERE vendor_ingredient_id = ?', (vi_id,))
        row = cursor.fetchone()
        assert row[0] == 500.0  # Latest value
        assert row[1] == '500 Grams'

        cursor.execute('SELECT COUNT(*) FROM packagingsizes WHERE vendor_ingredient_id = ?', (vi_id,))
        assert cursor.fetchone()[0] == 1  # Only one record


class TestUpsertOrderRule:
    """Test order rule upsert (delete-then-insert pattern)."""

    def test_replaces_existing_order_rule(self, sqlite_conn):
        """Order rule is replaced, not accumulated."""
        from bulksupplements_scraper import upsert_order_rule

        cursor = sqlite_conn.cursor()
        cursor.execute('INSERT INTO vendoringredients (vendor_id, variant_id, sku) VALUES (4, 1, "Z")')
        vi_id = cursor.lastrowid
        sqlite_conn.commit()

        scraped_at = datetime.now().isoformat()
        upsert_order_rule(sqlite_conn, vi_id, 100.0, scraped_at)
        upsert_order_rule(sqlite_conn, vi_id, 500.0, scraped_at)

        cursor.execute('SELECT base_quantity FROM orderrules WHERE vendor_ingredient_id = ?', (vi_id,))
        row = cursor.fetchone()
        assert row[0] == 500.0  # Latest value

        cursor.execute('SELECT COUNT(*) FROM orderrules WHERE vendor_ingredient_id = ?', (vi_id,))
        assert cursor.fetchone()[0] == 1  # Only one record
