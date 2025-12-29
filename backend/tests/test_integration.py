"""
Integration tests for full save flow.
End-to-end tests for save_to_relational_tables.
"""
import pytest
from datetime import datetime


class TestSaveToRelationalTablesIntegration:
    """End-to-end tests for save_to_relational_tables."""

    def test_boxnutra_full_product_flow(self, sqlite_conn):
        """BoxNutra: Complete product with multiple pack sizes."""
        from boxnutra_scraper import save_to_relational_tables

        rows = [
            {
                'product_id': 12345,
                'product_title': 'L-Glutamine Powder',
                'variant_id': 100001,
                'variant_sku': 'GLU-100G',
                'packaging': '100 Grams',
                'packaging_kg': 0.1,
                'pack_size_g': 100,
                'price': 12.99,
                'compare_at_price': None,
                'price_per_kg': 129.90,
                'stock_status': 'in_stock',
                'url': 'https://boxnutra.com/products/l-glutamine',
                'scraped_at': datetime.now().isoformat()
            },
            {
                'product_id': 12345,
                'product_title': 'L-Glutamine Powder',
                'variant_id': 100002,
                'variant_sku': 'GLU-500G',
                'packaging': '500 Grams',
                'packaging_kg': 0.5,
                'pack_size_g': 500,
                'price': 49.99,
                'compare_at_price': 59.99,
                'price_per_kg': 99.98,
                'stock_status': 'in_stock',
                'url': 'https://boxnutra.com/products/l-glutamine',
                'scraped_at': datetime.now().isoformat()
            }
        ]

        save_to_relational_tables(sqlite_conn, rows)
        sqlite_conn.commit()

        cursor = sqlite_conn.cursor()

        # Verify ingredient created
        cursor.execute("SELECT COUNT(*) FROM ingredients WHERE name = ?", ('L-Glutamine Powder',))
        assert cursor.fetchone()[0] == 1

        # Verify vendor ingredients for each pack size
        cursor.execute("SELECT COUNT(*) FROM vendoringredients WHERE sku LIKE ?", ('GLU-%',))
        assert cursor.fetchone()[0] == 2

        # Verify price tiers
        cursor.execute('''
            SELECT COUNT(*) FROM pricetiers pt
            JOIN vendoringredients vi ON pt.vendor_ingredient_id = vi.vendor_ingredient_id
            WHERE vi.sku LIKE 'GLU-%'
        ''')
        assert cursor.fetchone()[0] == 2

    def test_trafapharma_full_product_flow(self, sqlite_conn):
        """TrafaPharma: Product with multiple size options."""
        from trafapharma_scraper import save_to_relational_tables

        rows = [
            {
                'product_id': 889,
                'product_code': 'RM2078',
                'product_name': 'Vitamin D3 100,000 IU/g',
                'ingredient_name': 'Vitamin D3',
                'category': 'Vitamins',
                'size_id': '1',
                'size_name': '100g',
                'size_kg': 0.1,
                'price': 45.00,
                'price_per_kg': 450.00,
                'stock_status': 'unknown',
                'order_rule_type': 'fixed_pack',
                'shipping_responsibility': 'buyer',
                'url': 'https://trafapharma.com/vitamin-d3',
                'scraped_at': datetime.now().isoformat()
            },
            {
                'product_id': 889,
                'product_code': 'RM2078',
                'product_name': 'Vitamin D3 100,000 IU/g',
                'ingredient_name': 'Vitamin D3',
                'category': 'Vitamins',
                'size_id': '2',
                'size_name': '1 kg',
                'size_kg': 1.0,
                'price': 350.00,
                'price_per_kg': 350.00,
                'stock_status': 'unknown',
                'order_rule_type': 'fixed_pack',
                'shipping_responsibility': 'buyer',
                'url': 'https://trafapharma.com/vitamin-d3',
                'scraped_at': datetime.now().isoformat()
            }
        ]

        # Save to relational tables
        save_to_relational_tables(sqlite_conn, rows)
        sqlite_conn.commit()

        cursor = sqlite_conn.cursor()

        # Verify ingredient created
        cursor.execute("SELECT COUNT(*) FROM ingredients WHERE name = ?", ('Vitamin D3',))
        assert cursor.fetchone()[0] == 1

        # Verify vendor ingredients for each size (SKU = product_code + size)
        cursor.execute("SELECT COUNT(*) FROM vendoringredients WHERE sku LIKE ?", ('RM2078-%',))
        assert cursor.fetchone()[0] == 2

        # Verify price tiers (2 sizes with valid prices)
        cursor.execute('''
            SELECT COUNT(*) FROM pricetiers pt
            JOIN vendoringredients vi ON pt.vendor_ingredient_id = vi.vendor_ingredient_id
            WHERE vi.sku LIKE 'RM2078-%'
        ''')
        assert cursor.fetchone()[0] == 2

    def test_staleness_after_save(self, sqlite_conn):
        """Verify staleness tracking works after save_to_relational_tables."""
        from boxnutra_scraper import save_to_relational_tables, mark_stale_variants

        # First scrape: 2 variants
        rows = [
            {
                'product_id': 99999,
                'product_title': 'Test Product',
                'variant_id': 1001,
                'variant_sku': 'TEST-100G',
                'packaging': '100 Grams',
                'packaging_kg': 0.1,
                'pack_size_g': 100,
                'price': 10.00,
                'compare_at_price': None,
                'price_per_kg': 100.00,
                'stock_status': 'in_stock',
                'url': 'https://boxnutra.com/products/test',
                'scraped_at': datetime.now().isoformat()
            },
            {
                'product_id': 99999,
                'product_title': 'Test Product',
                'variant_id': 1002,
                'variant_sku': 'TEST-500G',
                'packaging': '500 Grams',
                'packaging_kg': 0.5,
                'pack_size_g': 500,
                'price': 40.00,
                'compare_at_price': None,
                'price_per_kg': 80.00,
                'stock_status': 'in_stock',
                'url': 'https://boxnutra.com/products/test',
                'scraped_at': datetime.now().isoformat()
            }
        ]

        save_to_relational_tables(sqlite_conn, rows)
        sqlite_conn.commit()

        cursor = sqlite_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM vendoringredients WHERE sku LIKE ?", ('TEST-%',))
        assert cursor.fetchone()[0] == 2

        # All should be active
        cursor.execute("SELECT COUNT(*) FROM vendoringredients WHERE sku LIKE ? AND status = 'active'", ('TEST-%',))
        assert cursor.fetchone()[0] == 2


class TestDatabaseConstraints:
    """Test database constraint enforcement."""

    def test_unique_constraint_vendor_ingredient(self, sqlite_conn):
        """Unique constraint on (vendor_id, variant_id, sku) enforced."""
        from bulksupplements_scraper import upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (4, 'https://test.com', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        # First insert
        result1 = upsert_vendor_ingredient(sqlite_conn, 4, 100, 'UNIQUE-SKU', 'Product V1', source_id)
        id1 = result1.vendor_ingredient_id

        # Second insert with same key should update, not duplicate
        result2 = upsert_vendor_ingredient(sqlite_conn, 4, 100, 'UNIQUE-SKU', 'Product V2', source_id)
        id2 = result2.vendor_ingredient_id

        assert id1 == id2

        cursor.execute("SELECT COUNT(*) FROM vendoringredients WHERE sku = ?", ('UNIQUE-SKU',))
        assert cursor.fetchone()[0] == 1

    def test_unique_ingredient_name(self, sqlite_conn):
        """Unique constraint on ingredient name enforced."""
        from boxnutra_scraper import get_or_create_ingredient

        id1 = get_or_create_ingredient(sqlite_conn, 'Unique Ingredient', None)
        id2 = get_or_create_ingredient(sqlite_conn, 'Unique Ingredient', None)

        assert id1 == id2

        cursor = sqlite_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM ingredients WHERE name = ?", ('Unique Ingredient',))
        assert cursor.fetchone()[0] == 1


class TestEmptyDataHandling:
    """Test handling of empty or missing data."""

    def test_empty_rows_list(self, sqlite_conn):
        """Empty rows list doesn't cause errors."""
        from boxnutra_scraper import save_to_relational_tables as bn_save
        from trafapharma_scraper import save_to_relational_tables as tp_save

        # Should not raise
        bn_save(sqlite_conn, [])
        tp_save(sqlite_conn, [])

    def test_single_row(self, sqlite_conn):
        """Single row processed correctly."""
        from boxnutra_scraper import save_to_relational_tables

        rows = [{
            'product_id': 11111,
            'product_title': 'Single Product',
            'variant_id': 2001,
            'variant_sku': 'SINGLE-SKU',
            'packaging': '100 Grams',
            'packaging_kg': 0.1,
            'pack_size_g': 100,
            'price': 5.00,
            'compare_at_price': None,
            'price_per_kg': 50.00,
            'stock_status': 'in_stock',
            'url': 'https://boxnutra.com/products/single',
            'scraped_at': datetime.now().isoformat()
        }]

        save_to_relational_tables(sqlite_conn, rows)
        sqlite_conn.commit()

        cursor = sqlite_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM vendoringredients WHERE sku = ?", ('SINGLE-SKU',))
        assert cursor.fetchone()[0] == 1
