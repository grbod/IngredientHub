"""
Tests for price tier operations across all scrapers.
Tests delete-before-insert patterns and price tier management.
"""
import pytest
from datetime import datetime


class TestDeleteOldPriceTiers:
    """Test price tier deletion before insert."""

    def test_deletes_all_tiers_for_variant_bs(self, sqlite_conn):
        """BulkSupplements: All existing price tiers deleted for vendor_ingredient."""
        from bulksupplements_scraper import delete_old_price_tiers

        cursor = sqlite_conn.cursor()
        cursor.execute('INSERT INTO vendoringredients (vendor_id, variant_id, sku) VALUES (4, 1, "Z")')
        vi_id = cursor.lastrowid

        # Insert multiple tiers
        for qty in [100, 500, 1000]:
            cursor.execute('''
                INSERT INTO pricetiers (vendor_ingredient_id, min_quantity, price)
                VALUES (?, ?, ?)
            ''', (vi_id, qty, qty * 0.1))
        sqlite_conn.commit()

        cursor.execute('SELECT COUNT(*) FROM pricetiers WHERE vendor_ingredient_id = ?', (vi_id,))
        assert cursor.fetchone()[0] == 3

        delete_old_price_tiers(sqlite_conn, vi_id)

        cursor.execute('SELECT COUNT(*) FROM pricetiers WHERE vendor_ingredient_id = ?', (vi_id,))
        assert cursor.fetchone()[0] == 0

    def test_deletes_all_tiers_for_variant_boxnutra(self, sqlite_conn):
        """BoxNutra: All existing price tiers deleted."""
        from boxnutra_scraper import delete_old_price_tiers

        cursor = sqlite_conn.cursor()
        cursor.execute('INSERT INTO vendoringredients (vendor_id, variant_id, sku) VALUES (25, 1, "BN")')
        vi_id = cursor.lastrowid

        cursor.execute('INSERT INTO pricetiers (vendor_ingredient_id, min_quantity, price) VALUES (?, 100, 10.0)', (vi_id,))
        cursor.execute('INSERT INTO pricetiers (vendor_ingredient_id, min_quantity, price) VALUES (?, 500, 40.0)', (vi_id,))
        sqlite_conn.commit()

        delete_old_price_tiers(sqlite_conn, vi_id)

        cursor.execute('SELECT COUNT(*) FROM pricetiers WHERE vendor_ingredient_id = ?', (vi_id,))
        assert cursor.fetchone()[0] == 0

    def test_only_deletes_specified_vendor_ingredient(self, sqlite_conn):
        """Only deletes tiers for the specified vendor_ingredient."""
        from bulksupplements_scraper import delete_old_price_tiers

        cursor = sqlite_conn.cursor()
        cursor.execute('INSERT INTO vendoringredients (vendor_id, variant_id, sku) VALUES (4, 1, "A")')
        vi_id1 = cursor.lastrowid
        cursor.execute('INSERT INTO vendoringredients (vendor_id, variant_id, sku) VALUES (4, 2, "B")')
        vi_id2 = cursor.lastrowid

        cursor.execute('INSERT INTO pricetiers (vendor_ingredient_id, min_quantity, price) VALUES (?, 100, 10.0)', (vi_id1,))
        cursor.execute('INSERT INTO pricetiers (vendor_ingredient_id, min_quantity, price) VALUES (?, 100, 20.0)', (vi_id2,))
        sqlite_conn.commit()

        delete_old_price_tiers(sqlite_conn, vi_id1)

        cursor.execute('SELECT COUNT(*) FROM pricetiers WHERE vendor_ingredient_id = ?', (vi_id1,))
        assert cursor.fetchone()[0] == 0

        cursor.execute('SELECT COUNT(*) FROM pricetiers WHERE vendor_ingredient_id = ?', (vi_id2,))
        assert cursor.fetchone()[0] == 1  # Unaffected


class TestInsertPriceTier:
    """Test price tier insertion."""

    def test_inserts_price_tier_bs_per_package(self, sqlite_conn):
        """BulkSupplements: per-package pricing model."""
        from bulksupplements_scraper import insert_price_tier

        cursor = sqlite_conn.cursor()
        cursor.execute('INSERT INTO vendoringredients (vendor_id, variant_id, sku) VALUES (4, 1, "W")')
        vi_id = cursor.lastrowid
        sqlite_conn.commit()

        row_data = {
            'pack_size_g': 250,
            'price': 24.99,
            'compare_at_price': 29.99,
            'price_per_kg': 99.96,
            'scraped_at': datetime.now().isoformat()
        }

        insert_price_tier(sqlite_conn, vi_id, row_data, source_id=1)

        cursor.execute('''
            SELECT min_quantity, price, original_price, price_per_kg, includes_shipping
            FROM pricetiers WHERE vendor_ingredient_id = ?
        ''', (vi_id,))
        row = cursor.fetchone()

        assert row[0] == 250  # min_quantity = pack_size_g
        assert row[1] == 24.99
        assert row[2] == 29.99  # compare_at_price
        assert row[3] == 99.96
        assert row[4] == 1  # includes_shipping = True for BS

    def test_skips_null_price_trafapharma(self, sqlite_conn):
        """TrafaPharma: NULL price (Inquire) skips insertion."""
        from trafapharma_scraper import insert_price_tier

        cursor = sqlite_conn.cursor()
        cursor.execute('INSERT INTO vendoringredients (vendor_id, variant_id, sku) VALUES (26, 1, "TP")')
        vi_id = cursor.lastrowid
        sqlite_conn.commit()

        row_data = {
            'size_kg': 25.0,
            'price': None,  # Inquire Bulk Price
            'price_per_kg': None,
            'scraped_at': datetime.now().isoformat()
        }

        insert_price_tier(sqlite_conn, vi_id, row_data, source_id=1)

        cursor.execute('SELECT COUNT(*) FROM pricetiers WHERE vendor_ingredient_id = ?', (vi_id,))
        assert cursor.fetchone()[0] == 0  # No tier inserted

    def test_inserts_price_tier_with_valid_price(self, sqlite_conn):
        """TrafaPharma: Valid price inserts tier correctly."""
        from trafapharma_scraper import insert_price_tier

        cursor = sqlite_conn.cursor()
        cursor.execute('INSERT INTO vendoringredients (vendor_id, variant_id, sku) VALUES (26, 1, "TP2")')
        vi_id = cursor.lastrowid
        sqlite_conn.commit()

        row_data = {
            'size_kg': 1.0,
            'price': 45.00,
            'price_per_kg': 45.00,
            'scraped_at': datetime.now().isoformat()
        }

        insert_price_tier(sqlite_conn, vi_id, row_data, source_id=1)

        cursor.execute('SELECT price, price_per_kg FROM pricetiers WHERE vendor_ingredient_id = ?', (vi_id,))
        row = cursor.fetchone()
        assert row[0] == 45.00
        assert row[1] == 45.00

    def test_no_compare_at_price(self, sqlite_conn):
        """Price tier with no compare_at_price stores NULL for original_price."""
        from bulksupplements_scraper import insert_price_tier

        cursor = sqlite_conn.cursor()
        cursor.execute('INSERT INTO vendoringredients (vendor_id, variant_id, sku) VALUES (4, 1, "X")')
        vi_id = cursor.lastrowid
        sqlite_conn.commit()

        row_data = {
            'pack_size_g': 100,
            'price': 9.99,
            'compare_at_price': None,
            'price_per_kg': 99.90,
            'scraped_at': datetime.now().isoformat()
        }

        insert_price_tier(sqlite_conn, vi_id, row_data, source_id=1)

        cursor.execute('SELECT original_price FROM pricetiers WHERE vendor_ingredient_id = ?', (vi_id,))
        row = cursor.fetchone()
        assert row[0] is None


class TestDeleteThenInsertPattern:
    """Test the full delete-then-insert pattern for price tiers."""

    def test_replace_all_tiers(self, sqlite_conn):
        """Old tiers replaced with new ones."""
        from bulksupplements_scraper import delete_old_price_tiers, insert_price_tier

        cursor = sqlite_conn.cursor()
        cursor.execute('INSERT INTO vendoringredients (vendor_id, variant_id, sku) VALUES (4, 1, "R")')
        vi_id = cursor.lastrowid

        # Insert old tier
        cursor.execute('INSERT INTO pricetiers (vendor_ingredient_id, min_quantity, price) VALUES (?, 100, 10.0)', (vi_id,))
        sqlite_conn.commit()

        # Delete old, insert new
        delete_old_price_tiers(sqlite_conn, vi_id)

        row_data = {
            'pack_size_g': 500,
            'price': 40.0,
            'compare_at_price': None,
            'price_per_kg': 80.0,
            'scraped_at': datetime.now().isoformat()
        }
        insert_price_tier(sqlite_conn, vi_id, row_data, source_id=1)

        cursor.execute('SELECT min_quantity, price FROM pricetiers WHERE vendor_ingredient_id = ?', (vi_id,))
        row = cursor.fetchone()
        assert row[0] == 500  # New tier
        assert row[1] == 40.0
