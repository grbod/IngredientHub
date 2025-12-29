"""
Tests for staleness tracking across all scrapers.
Tests mark_stale_variants and mark_missing_variants_for_product functions.
"""
import pytest
from datetime import datetime


class TestMarkStaleVariants:
    """Test full-scrape staleness marking."""

    def test_marks_old_variants_inactive_bs(self, sqlite_conn):
        """BulkSupplements: Variants with old last_seen_at marked stale."""
        from bulksupplements_scraper import mark_stale_variants

        cursor = sqlite_conn.cursor()
        # Insert variant with old timestamp
        cursor.execute('''
            INSERT INTO vendoringredients
            (vendor_id, variant_id, sku, last_seen_at, status)
            VALUES (4, 1, 'OLD-SKU', '2020-01-01T00:00:00', 'active')
        ''')
        sqlite_conn.commit()

        # Run staleness check with recent time
        stale_variants = mark_stale_variants(sqlite_conn, vendor_id=4,
                                          scrape_start_time='2025-01-01T00:00:00')

        assert len(stale_variants) == 1
        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('OLD-SKU',))
        assert cursor.fetchone()[0] == 'stale'

    def test_marks_old_variants_inactive_boxnutra(self, sqlite_conn):
        """BoxNutra: Variants with old last_seen_at marked stale."""
        from boxnutra_scraper import mark_stale_variants

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO vendoringredients
            (vendor_id, variant_id, sku, last_seen_at, status)
            VALUES (25, 1, 'BN-OLD-SKU', '2020-01-01T00:00:00', 'active')
        ''')
        sqlite_conn.commit()

        stale_variants = mark_stale_variants(sqlite_conn, vendor_id=25,
                                          scrape_start_time='2025-01-01T00:00:00')

        assert len(stale_variants) == 1
        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('BN-OLD-SKU',))
        assert cursor.fetchone()[0] == 'stale'

    def test_marks_old_variants_inactive_trafapharma(self, sqlite_conn):
        """TrafaPharma: Variants with old last_seen_at marked inactive."""
        from trafapharma_scraper import mark_stale_variants

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO vendoringredients
            (vendor_id, variant_id, sku, last_seen_at, status)
            VALUES (26, 1, 'TP-OLD-SKU', '2020-01-01T00:00:00', 'active')
        ''')
        sqlite_conn.commit()

        stale_variants = mark_stale_variants(sqlite_conn, vendor_id=26,
                                          scrape_start_time='2025-01-01T00:00:00')

        assert len(stale_variants) == 1
        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('TP-OLD-SKU',))
        assert cursor.fetchone()[0] == 'stale'

    def test_keeps_recent_variants_active(self, sqlite_conn):
        """Variants seen in current scrape stay active."""
        from bulksupplements_scraper import mark_stale_variants, upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (4, 'https://test.com', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        scrape_time = datetime.now().isoformat()

        # Upsert during "current" scrape
        upsert_vendor_ingredient(sqlite_conn, 4, 1, 'NEW-SKU', 'Fresh Product', source_id)

        # Run staleness check
        mark_stale_variants(sqlite_conn, vendor_id=4, scrape_start_time=scrape_time)

        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('NEW-SKU',))
        assert cursor.fetchone()[0] == 'active'

    def test_null_last_seen_at_marked_inactive(self, sqlite_conn):
        """Variants with NULL last_seen_at are marked stale."""
        from bulksupplements_scraper import mark_stale_variants

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO vendoringredients
            (vendor_id, variant_id, sku, last_seen_at, status)
            VALUES (4, 1, 'NULL-TIME-SKU', NULL, 'active')
        ''')
        sqlite_conn.commit()

        mark_stale_variants(sqlite_conn, vendor_id=4, scrape_start_time='2025-01-01T00:00:00')

        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('NULL-TIME-SKU',))
        assert cursor.fetchone()[0] == 'stale'

    def test_only_affects_specified_vendor(self, sqlite_conn):
        """Staleness check scoped to vendor_id."""
        from bulksupplements_scraper import mark_stale_variants

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO vendoringredients (vendor_id, variant_id, sku, last_seen_at, status)
            VALUES
                (4, 1, 'BS-OLD', '2020-01-01', 'active'),
                (25, 2, 'BN-OLD', '2020-01-01', 'active')
        ''')
        sqlite_conn.commit()

        # Only mark BS (vendor_id=4) stale
        mark_stale_variants(sqlite_conn, vendor_id=4, scrape_start_time='2025-01-01')

        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('BS-OLD',))
        assert cursor.fetchone()[0] == 'stale'

        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('BN-OLD',))
        assert cursor.fetchone()[0] == 'active'  # BoxNutra unaffected

    def test_already_inactive_not_double_counted(self, sqlite_conn):
        """Already inactive variants not counted in stale_variants."""
        from bulksupplements_scraper import mark_stale_variants

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO vendoringredients (vendor_id, variant_id, sku, last_seen_at, status)
            VALUES (4, 1, 'ALREADY-INACTIVE', '2020-01-01', 'inactive')
        ''')
        sqlite_conn.commit()

        stale_variants = mark_stale_variants(sqlite_conn, vendor_id=4,
                                          scrape_start_time='2025-01-01')
        assert len(stale_variants) == 0  # Already inactive, not counted

    def test_multiple_stale_variants(self, sqlite_conn):
        """Multiple variants marked stale in single call."""
        from bulksupplements_scraper import mark_stale_variants

        cursor = sqlite_conn.cursor()
        for i in range(5):
            cursor.execute('''
                INSERT INTO vendoringredients (vendor_id, variant_id, sku, last_seen_at, status)
                VALUES (4, ?, ?, '2020-01-01', 'active')
            ''', (i, f'OLD-SKU-{i}'))
        sqlite_conn.commit()

        stale_variants = mark_stale_variants(sqlite_conn, vendor_id=4,
                                          scrape_start_time='2025-01-01')
        assert len(stale_variants) == 5


class TestMarkMissingVariantsForProduct:
    """Test per-product variant staleness (variant removed but product exists)."""

    def test_marks_missing_sku_inactive_boxnutra(self, sqlite_conn):
        """BoxNutra: SKUs not in seen_skus list marked stale."""
        from boxnutra_scraper import mark_missing_variants_for_product

        cursor = sqlite_conn.cursor()
        # Insert 3 SKUs for same variant
        for sku in ['PROD-100G', 'PROD-500G', 'PROD-1KG']:
            cursor.execute('''
                INSERT INTO vendoringredients
                (vendor_id, variant_id, sku, status)
                VALUES (25, 100, ?, 'active')
            ''', (sku,))
        sqlite_conn.commit()

        # Current scrape only sees 2 SKUs
        seen_skus = ['PROD-100G', 'PROD-500G']
        mark_missing_variants_for_product(sqlite_conn, vendor_id=25, variant_id=100,
                                          seen_skus=seen_skus,
                                          scrape_time=datetime.now().isoformat())

        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('PROD-1KG',))
        assert cursor.fetchone()[0] == 'stale'

        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('PROD-100G',))
        assert cursor.fetchone()[0] == 'active'

    def test_marks_missing_sku_inactive_trafapharma(self, sqlite_conn):
        """TrafaPharma: SKUs not in seen_skus list marked inactive."""
        from trafapharma_scraper import mark_missing_variants_for_product

        cursor = sqlite_conn.cursor()
        # Insert 3 size variants for same product
        for size_id in ['1', '2', '3']:
            cursor.execute('''
                INSERT INTO vendoringredients
                (vendor_id, variant_id, sku, status)
                VALUES (26, 200, ?, 'active')
            ''', (f'889-{size_id}',))
        sqlite_conn.commit()

        # Current scrape only sees 2 sizes
        seen_skus = ['889-1', '889-2']
        mark_missing_variants_for_product(sqlite_conn, vendor_id=26, variant_id=200,
                                          seen_skus=seen_skus,
                                          scrape_time=datetime.now().isoformat())

        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('889-3',))
        assert cursor.fetchone()[0] == 'stale'

        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('889-1',))
        assert cursor.fetchone()[0] == 'active'

    def test_empty_seen_skus_no_change(self, sqlite_conn):
        """Empty seen_skus list causes no updates (edge case protection)."""
        from boxnutra_scraper import mark_missing_variants_for_product

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO vendoringredients (vendor_id, variant_id, sku, status)
            VALUES (25, 100, 'EXISTING', 'active')
        ''')
        sqlite_conn.commit()

        result = mark_missing_variants_for_product(sqlite_conn, vendor_id=25, variant_id=100,
                                                   seen_skus=[],
                                                   scrape_time=datetime.now().isoformat())

        assert result == 0  # No updates
        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('EXISTING',))
        assert cursor.fetchone()[0] == 'active'  # Unchanged

    def test_scoped_to_variant_id(self, sqlite_conn):
        """Only affects SKUs for specified variant_id."""
        from boxnutra_scraper import mark_missing_variants_for_product

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO vendoringredients (vendor_id, variant_id, sku, status)
            VALUES
                (25, 100, 'PROD-A-100G', 'active'),
                (25, 200, 'PROD-B-100G', 'active')
        ''')
        sqlite_conn.commit()

        # Mark missing for variant 100 only
        mark_missing_variants_for_product(sqlite_conn, vendor_id=25, variant_id=100,
                                          seen_skus=['DIFFERENT-SKU'],  # Doesn't match
                                          scrape_time=datetime.now().isoformat())

        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('PROD-A-100G',))
        assert cursor.fetchone()[0] == 'stale'

        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('PROD-B-100G',))
        assert cursor.fetchone()[0] == 'active'  # Different variant_id, unaffected

    def test_all_skus_seen_no_change(self, sqlite_conn):
        """When all existing SKUs are in seen_skus, nothing marked inactive."""
        from boxnutra_scraper import mark_missing_variants_for_product

        cursor = sqlite_conn.cursor()
        for sku in ['SKU-A', 'SKU-B', 'SKU-C']:
            cursor.execute('''
                INSERT INTO vendoringredients (vendor_id, variant_id, sku, status)
                VALUES (25, 100, ?, 'active')
            ''', (sku,))
        sqlite_conn.commit()

        # All SKUs seen
        result = mark_missing_variants_for_product(
            sqlite_conn, vendor_id=25, variant_id=100,
            seen_skus=['SKU-A', 'SKU-B', 'SKU-C'],
            scrape_time=datetime.now().isoformat()
        )

        assert result == 0

        cursor.execute('SELECT COUNT(*) FROM vendoringredients WHERE status = ?', ('inactive',))
        assert cursor.fetchone()[0] == 0


class TestStalenessIntegration:
    """Integration tests combining upsert with staleness tracking."""

    def test_full_scrape_staleness_flow(self, sqlite_conn):
        """Simulate full scrape with some products disappearing."""
        from bulksupplements_scraper import upsert_vendor_ingredient, mark_stale_variants

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (4, 'https://test.com', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        # First scrape: 3 products
        scrape1_time = '2025-01-01T00:00:00'
        for sku in ['PROD-A', 'PROD-B', 'PROD-C']:
            cursor.execute('''
                INSERT INTO vendoringredients
                (vendor_id, variant_id, sku, last_seen_at, status)
                VALUES (4, 1, ?, ?, 'active')
            ''', (sku, scrape1_time))
        sqlite_conn.commit()

        # Second scrape: only 2 products seen
        scrape2_time = '2025-01-02T00:00:00'
        for sku in ['PROD-A', 'PROD-B']:
            upsert_vendor_ingredient(sqlite_conn, 4, 1, sku, f'Product {sku}', source_id)
        sqlite_conn.commit()

        # Mark stale
        stale_variants = mark_stale_variants(sqlite_conn, vendor_id=4,
                                          scrape_start_time=scrape2_time)

        assert len(stale_variants) == 1  # PROD-C marked stale

        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('PROD-C',))
        assert cursor.fetchone()[0] == 'stale'

        cursor.execute('SELECT status FROM vendoringredients WHERE sku = ?', ('PROD-A',))
        assert cursor.fetchone()[0] == 'active'
