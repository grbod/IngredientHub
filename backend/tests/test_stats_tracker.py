"""
Tests for StatsTracker functionality across all scrapers.
Verifies consistent statistics tracking and reporting.
"""
import pytest
from datetime import datetime, timedelta


class TestStatsTrackerInitialization:
    """Test StatsTracker initialization for each scraper."""

    def test_bulksupplements_stats_tracker_init(self):
        """BulkSupplements StatsTracker initializes with correct defaults."""
        from bulksupplements_scraper import StatsTracker

        stats = StatsTracker(vendor_id=4, is_full_scrape=True, max_products_limit=None)

        assert stats.vendor_id == 4
        assert stats.is_full_scrape is True
        assert stats.max_products_limit is None
        assert stats.products_discovered == 0
        assert stats.products_processed == 0
        assert stats.products_skipped == 0
        assert stats.products_failed == 0
        assert stats.variants_new == 0
        assert stats.variants_updated == 0
        assert stats.variants_unchanged == 0
        assert stats.variants_stale == 0
        assert stats.variants_reactivated == 0
        assert stats.alerts == []
        assert stats.run_id is None

    def test_boxnutra_stats_tracker_init(self):
        """BoxNutra StatsTracker initializes with correct defaults."""
        from boxnutra_scraper import StatsTracker

        stats = StatsTracker(vendor_id=25, is_full_scrape=False, max_products_limit=50)

        assert stats.vendor_id == 25
        assert stats.is_full_scrape is False
        assert stats.max_products_limit == 50

    def test_trafapharma_stats_tracker_init(self):
        """TrafaPharma StatsTracker initializes with correct defaults."""
        from trafapharma_scraper import StatsTracker

        stats = StatsTracker(vendor_id=26, is_full_scrape=True)

        assert stats.vendor_id == 26
        assert stats.is_full_scrape is True

    def test_io_stats_tracker_init(self):
        """IO StatsTracker initializes with correct defaults."""
        from IO_scraper import StatsTracker

        stats = StatsTracker(vendor_id=1, is_full_scrape=True)

        assert stats.vendor_id == 1
        assert stats.is_full_scrape is True


class TestRecordNewProduct:
    """Test recording new products."""

    def test_bulksupplements_record_new_product(self):
        """BulkSupplements tracks new products."""
        from bulksupplements_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=4)
        stats.record_new_product('SKU-001', 'Test Product', vendor_ingredient_id=123)

        assert stats.variants_new == 1
        assert len(stats.alerts) == 1
        assert stats.alerts[0].alert_type == AlertType.NEW_PRODUCT
        assert stats.alerts[0].sku == 'SKU-001'
        assert stats.alerts[0].product_name == 'Test Product'
        assert stats.alerts[0].vendor_ingredient_id == 123

    def test_boxnutra_record_new_product(self):
        """BoxNutra tracks new products."""
        from boxnutra_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=25)
        stats.record_new_product('BN-SKU', 'BoxNutra Product', vendor_ingredient_id=456)

        assert stats.variants_new == 1
        assert len(stats.alerts) == 1
        assert stats.alerts[0].alert_type == AlertType.NEW_PRODUCT

    def test_trafapharma_record_new_product(self):
        """TrafaPharma tracks new products."""
        from trafapharma_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=26)
        stats.record_new_product('RM2078-1kg', 'Vitamin D3', vendor_ingredient_id=789)

        assert stats.variants_new == 1
        assert len(stats.alerts) == 1
        assert stats.alerts[0].alert_type == AlertType.NEW_PRODUCT

    def test_io_record_new_product(self):
        """IO tracks new products."""
        from IO_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=1)
        stats.record_new_product('59410-100-10312-11455', 'Astragalus P.E.', vendor_ingredient_id=999)

        assert stats.variants_new == 1
        assert len(stats.alerts) == 1
        assert stats.alerts[0].alert_type == AlertType.NEW_PRODUCT


class TestRecordReactivated:
    """Test recording reactivated products."""

    def test_bulksupplements_record_reactivated(self):
        """BulkSupplements tracks reactivated products."""
        from bulksupplements_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=4)
        stats.record_reactivated('SKU-001', 'Test Product', stale_since='2024-01-01', vendor_ingredient_id=123)

        assert stats.variants_reactivated == 1
        assert len(stats.alerts) == 1
        assert stats.alerts[0].alert_type == AlertType.REACTIVATED
        assert stats.alerts[0].old_value == '2024-01-01'
        assert 'was stale since' in stats.alerts[0].message

    def test_boxnutra_record_reactivated_no_stale_since(self):
        """BoxNutra handles reactivation without stale_since date."""
        from boxnutra_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=25)
        stats.record_reactivated('BN-SKU', 'Product', stale_since=None)

        assert stats.variants_reactivated == 1
        assert 'was stale since' not in stats.alerts[0].message


class TestRecordPriceChange:
    """Test recording price changes with 30% threshold."""

    def test_bulksupplements_major_price_decrease(self):
        """BulkSupplements alerts on >30% price decrease."""
        from bulksupplements_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=4)
        # 50% decrease: $100 -> $50
        stats.record_price_change('SKU-001', 'Product', old_price=100.0, new_price=50.0)

        assert len(stats.alerts) == 1
        assert stats.alerts[0].alert_type == AlertType.PRICE_DECREASE_MAJOR
        assert stats.alerts[0].change_percent == -50.0
        assert '$100.00' in stats.alerts[0].old_value
        assert '$50.00' in stats.alerts[0].new_value

    def test_boxnutra_major_price_increase(self):
        """BoxNutra alerts on >30% price increase."""
        from boxnutra_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=25)
        # 50% increase: $100 -> $150
        stats.record_price_change('BN-SKU', 'Product', old_price=100.0, new_price=150.0)

        assert len(stats.alerts) == 1
        assert stats.alerts[0].alert_type == AlertType.PRICE_INCREASE_MAJOR
        assert stats.alerts[0].change_percent == 50.0

    def test_trafapharma_no_alert_for_small_change(self):
        """TrafaPharma doesn't alert on <30% price change."""
        from trafapharma_scraper import StatsTracker

        stats = StatsTracker(vendor_id=26)
        # 20% increase: $100 -> $120
        stats.record_price_change('RM2078-1kg', 'Product', old_price=100.0, new_price=120.0)

        assert len(stats.alerts) == 0

    def test_io_no_alert_for_zero_old_price(self):
        """IO doesn't alert when old_price is zero (division by zero protection)."""
        from IO_scraper import StatsTracker

        stats = StatsTracker(vendor_id=1)
        stats.record_price_change('SKU', 'Product', old_price=0.0, new_price=100.0)

        assert len(stats.alerts) == 0

    def test_price_change_exactly_30_percent(self):
        """Test boundary condition at exactly 30% change."""
        from bulksupplements_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=4)
        # Exactly 30% increase: $100 -> $130
        stats.record_price_change('SKU', 'Product', old_price=100.0, new_price=130.0)

        assert len(stats.alerts) == 1
        assert stats.alerts[0].alert_type == AlertType.PRICE_INCREASE_MAJOR

    def test_price_change_just_under_30_percent(self):
        """Test boundary: just under 30% doesn't trigger alert."""
        from bulksupplements_scraper import StatsTracker

        stats = StatsTracker(vendor_id=4)
        # 29% increase: $100 -> $129
        stats.record_price_change('SKU', 'Product', old_price=100.0, new_price=129.0)

        assert len(stats.alerts) == 0


class TestRecordStockChange:
    """Test recording stock status changes."""

    def test_bulksupplements_stock_out_alert(self):
        """BulkSupplements alerts on stock out (in_stock -> out_of_stock)."""
        from bulksupplements_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=4)
        stats.record_stock_change('SKU-001', 'Product', was_in_stock=True, is_in_stock=False)

        assert len(stats.alerts) == 1
        assert stats.alerts[0].alert_type == AlertType.STOCK_OUT
        assert stats.alerts[0].old_value == 'in_stock'
        assert stats.alerts[0].new_value == 'out_of_stock'

    def test_boxnutra_no_alert_for_restock(self):
        """BoxNutra doesn't alert on restock (out_of_stock -> in_stock)."""
        from boxnutra_scraper import StatsTracker

        stats = StatsTracker(vendor_id=25)
        stats.record_stock_change('BN-SKU', 'Product', was_in_stock=False, is_in_stock=True)

        assert len(stats.alerts) == 0

    def test_trafapharma_no_alert_same_status(self):
        """TrafaPharma doesn't alert when status unchanged."""
        from trafapharma_scraper import StatsTracker

        stats = StatsTracker(vendor_id=26)
        stats.record_stock_change('RM2078-1kg', 'Product', was_in_stock=True, is_in_stock=True)

        assert len(stats.alerts) == 0


class TestRecordStale:
    """Test recording stale variants."""

    def test_bulksupplements_record_stale(self):
        """BulkSupplements tracks stale variants."""
        from bulksupplements_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=4)
        stats.record_stale('SKU-001', 'Old Product', last_seen_at='2024-01-01', vendor_ingredient_id=123)

        assert stats.variants_stale == 1
        assert len(stats.alerts) == 1
        assert stats.alerts[0].alert_type == AlertType.STALE_VARIANT
        assert 'last seen: 2024-01-01' in stats.alerts[0].message

    def test_boxnutra_record_stale_unknown_last_seen(self):
        """BoxNutra handles stale with unknown last_seen_at."""
        from boxnutra_scraper import StatsTracker

        stats = StatsTracker(vendor_id=25)
        stats.record_stale('BN-SKU', 'Product', last_seen_at=None)

        assert stats.variants_stale == 1
        assert 'last seen: unknown' in stats.alerts[0].message


class TestRecordFailure:
    """Test recording scraping failures."""

    def test_bulksupplements_http_error(self):
        """BulkSupplements tracks HTTP errors."""
        from bulksupplements_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=4)
        stats.record_failure('product-slug', 'HTTP', 'Connection timeout')

        assert stats.products_failed == 1
        assert len(stats.alerts) == 1
        assert stats.alerts[0].alert_type == AlertType.HTTP_ERROR
        assert '[HTTP]' in stats.alerts[0].message

    def test_boxnutra_db_error(self):
        """BoxNutra tracks DB errors."""
        from boxnutra_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=25)
        stats.record_failure('product-slug', 'DB', 'Constraint violation')

        assert stats.products_failed == 1
        assert stats.alerts[0].alert_type == AlertType.DB_ERROR
        assert '[DB]' in stats.alerts[0].message


class TestRecordUpdatedUnchanged:
    """Test recording updated and unchanged variants."""

    def test_bulksupplements_record_updated(self):
        """BulkSupplements tracks updated variants."""
        from bulksupplements_scraper import StatsTracker

        stats = StatsTracker(vendor_id=4)
        stats.record_updated()
        stats.record_updated()

        assert stats.variants_updated == 2

    def test_boxnutra_record_unchanged(self):
        """BoxNutra tracks unchanged variants."""
        from boxnutra_scraper import StatsTracker

        stats = StatsTracker(vendor_id=25)
        stats.record_unchanged()
        stats.record_unchanged()
        stats.record_unchanged()

        assert stats.variants_unchanged == 3


class TestAlertCounts:
    """Test alert counting and filtering."""

    def test_bulksupplements_get_alert_counts(self):
        """BulkSupplements correctly counts alerts by type."""
        from bulksupplements_scraper import StatsTracker

        stats = StatsTracker(vendor_id=4)
        stats.record_new_product('SKU-1', 'Product 1')
        stats.record_new_product('SKU-2', 'Product 2')
        stats.record_stale('SKU-3', 'Product 3')
        stats.record_price_change('SKU-4', 'Product 4', 100.0, 50.0)  # Major decrease

        counts = stats.get_alert_counts()

        assert counts['new_product'] == 2
        assert counts['stale_variant'] == 1
        assert counts['price_decrease_major'] == 1

    def test_boxnutra_get_alerts_by_type(self):
        """BoxNutra correctly filters alerts by type."""
        from boxnutra_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=25)
        stats.record_new_product('SKU-1', 'Product 1')
        stats.record_new_product('SKU-2', 'Product 2')
        stats.record_stale('SKU-3', 'Product 3')

        new_alerts = stats.get_alerts_by_type(AlertType.NEW_PRODUCT)
        stale_alerts = stats.get_alerts_by_type(AlertType.STALE_VARIANT)

        assert len(new_alerts) == 2
        assert len(stale_alerts) == 1


class TestAlertSeverity:
    """Test alert severity assignments."""

    def test_bulksupplements_severity_levels(self):
        """BulkSupplements assigns correct severity levels."""
        from bulksupplements_scraper import StatsTracker, AlertSeverity

        stats = StatsTracker(vendor_id=4)
        stats.record_new_product('SKU-1', 'Product')  # INFO
        stats.record_stale('SKU-2', 'Product')  # WARNING
        stats.record_price_change('SKU-3', 'Product', 100.0, 50.0)  # CRITICAL (decrease)
        stats.record_failure('slug', 'DB', 'Error')  # CRITICAL

        severities = [a.severity for a in stats.alerts]
        assert AlertSeverity.INFO in severities
        assert AlertSeverity.WARNING in severities
        assert AlertSeverity.CRITICAL in severities


class TestCheckpointSerialization:
    """Test checkpoint serialization/deserialization."""

    def test_bulksupplements_to_checkpoint_dict(self):
        """BulkSupplements serializes stats to checkpoint dict."""
        from bulksupplements_scraper import StatsTracker

        stats = StatsTracker(vendor_id=4, is_full_scrape=False, max_products_limit=100)
        stats.products_discovered = 500
        stats.products_processed = 50
        stats.variants_new = 10
        stats.variants_updated = 30

        checkpoint = stats.to_checkpoint_dict()

        assert checkpoint['vendor_id'] == 4
        assert checkpoint['is_full_scrape'] is False
        assert checkpoint['max_products_limit'] == 100
        assert checkpoint['products_discovered'] == 500
        assert checkpoint['products_processed'] == 50
        assert checkpoint['variants_new'] == 10
        assert checkpoint['variants_updated'] == 30
        assert 'started_at' in checkpoint

    def test_boxnutra_from_checkpoint_dict(self):
        """BoxNutra deserializes stats from checkpoint dict."""
        from boxnutra_scraper import StatsTracker

        checkpoint = {
            'vendor_id': 25,
            'is_full_scrape': True,
            'max_products_limit': None,
            'started_at': '2024-01-01T10:00:00',
            'products_discovered': 200,
            'products_processed': 100,
            'products_skipped': 5,
            'products_failed': 2,
            'variants_new': 20,
            'variants_updated': 50,
            'variants_unchanged': 30,
            'variants_stale': 3,
            'variants_reactivated': 1
        }

        stats = StatsTracker.from_checkpoint_dict(checkpoint)

        assert stats.vendor_id == 25
        assert stats.products_discovered == 200
        assert stats.products_processed == 100
        assert stats.variants_new == 20
        assert stats.variants_reactivated == 1


class TestUpsertResultIntegration:
    """Test UpsertResult integration with database functions."""

    def test_bulksupplements_upsert_returns_result(self, sqlite_conn):
        """BulkSupplements upsert_vendor_ingredient returns UpsertResult."""
        from bulksupplements_scraper import upsert_vendor_ingredient, UpsertResult

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (4, 'https://test.com', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        result = upsert_vendor_ingredient(sqlite_conn, 4, 100, 'TEST-SKU', 'Test Product', source_id)

        assert isinstance(result, UpsertResult)
        assert result.vendor_ingredient_id > 0
        assert result.is_new is True
        assert result.was_stale is False

    def test_boxnutra_upsert_returns_result(self, sqlite_conn):
        """BoxNutra upsert_vendor_ingredient returns UpsertResult."""
        from boxnutra_scraper import upsert_vendor_ingredient, UpsertResult

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (25, 'https://boxnutra.com/test', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        result = upsert_vendor_ingredient(sqlite_conn, 25, 200, 'BN-SKU', 'BoxNutra Product', source_id)

        assert isinstance(result, UpsertResult)
        assert result.is_new is True

    def test_trafapharma_upsert_returns_result(self, sqlite_conn):
        """TrafaPharma upsert_vendor_ingredient returns UpsertResult."""
        from trafapharma_scraper import upsert_vendor_ingredient, UpsertResult

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (26, 'https://trafapharma.com/test', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        result = upsert_vendor_ingredient(sqlite_conn, 26, 300, 'RM2078-1kg', 'Vitamin D3', source_id)

        assert isinstance(result, UpsertResult)
        assert result.is_new is True

    def test_io_upsert_returns_result(self, sqlite_conn):
        """IO upsert_vendor_ingredient returns UpsertResult."""
        from IO_scraper import upsert_vendor_ingredient, UpsertResult

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (1, 'https://ingredientsonline.com/test', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        result = upsert_vendor_ingredient(sqlite_conn, 1, 400, '59410-100', 'Astragalus', source_id)

        assert isinstance(result, UpsertResult)
        assert result.is_new is True


class TestUpsertReactivation:
    """Test UpsertResult tracks reactivation from stale status."""

    def test_bulksupplements_reactivation_detection(self, sqlite_conn):
        """BulkSupplements detects reactivation from stale status."""
        from bulksupplements_scraper import upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (4, 'https://test.com', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid

        # First insert
        result1 = upsert_vendor_ingredient(sqlite_conn, 4, 100, 'REACT-SKU', 'Product', source_id)
        assert result1.is_new is True
        assert result1.was_stale is False

        # Mark as stale
        cursor.execute('''
            UPDATE vendoringredients SET status = 'stale', stale_since = ?
            WHERE vendor_ingredient_id = ?
        ''', (datetime.now().isoformat(), result1.vendor_ingredient_id))
        sqlite_conn.commit()

        # Upsert again - should detect reactivation
        result2 = upsert_vendor_ingredient(sqlite_conn, 4, 100, 'REACT-SKU', 'Product Updated', source_id)

        assert result2.is_new is False
        assert result2.was_stale is True
        assert result2.vendor_ingredient_id == result1.vendor_ingredient_id
        assert 'stale_since' in result2.changed_fields

    def test_boxnutra_reactivation_clears_stale_since(self, sqlite_conn):
        """BoxNutra reactivation clears stale_since field."""
        from boxnutra_scraper import upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (25, 'https://boxnutra.com/test', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid

        # First insert
        result1 = upsert_vendor_ingredient(sqlite_conn, 25, 200, 'BN-REACT', 'Product', source_id)

        # Mark as stale with timestamp
        stale_time = datetime.now().isoformat()
        cursor.execute('''
            UPDATE vendoringredients SET status = 'stale', stale_since = ?
            WHERE vendor_ingredient_id = ?
        ''', (stale_time, result1.vendor_ingredient_id))
        sqlite_conn.commit()

        # Verify stale_since is set
        cursor.execute('SELECT stale_since FROM vendoringredients WHERE vendor_ingredient_id = ?',
                       (result1.vendor_ingredient_id,))
        assert cursor.fetchone()[0] is not None

        # Reactivate
        upsert_vendor_ingredient(sqlite_conn, 25, 200, 'BN-REACT', 'Product', source_id)
        sqlite_conn.commit()

        # Verify stale_since is cleared and status is active
        cursor.execute('SELECT status, stale_since FROM vendoringredients WHERE vendor_ingredient_id = ?',
                       (result1.vendor_ingredient_id,))
        row = cursor.fetchone()
        assert row[0] == 'active'
        assert row[1] is None


class TestMarkStaleVariantsWithStats:
    """Test mark_stale_variants integration with StatsTracker."""

    def test_bulksupplements_mark_stale_updates_stats(self, sqlite_conn):
        """BulkSupplements mark_stale_variants updates stats."""
        from bulksupplements_scraper import mark_stale_variants, StatsTracker

        cursor = sqlite_conn.cursor()
        old_time = (datetime.now() - timedelta(days=1)).isoformat()

        # Insert vendor ingredient with old last_seen_at
        cursor.execute('''
            INSERT INTO vendoringredients (vendor_id, variant_id, sku, raw_product_name, status, last_seen_at)
            VALUES (4, 100, 'OLD-SKU', 'Old Product', 'active', ?)
        ''', (old_time,))
        sqlite_conn.commit()

        stats = StatsTracker(vendor_id=4)
        scrape_start = datetime.now().isoformat()

        stale_variants = mark_stale_variants(sqlite_conn, 4, scrape_start, stats)

        assert len(stale_variants) == 1
        assert stale_variants[0]['sku'] == 'OLD-SKU'
        assert stats.variants_stale == 1

    def test_boxnutra_mark_stale_returns_list(self, sqlite_conn):
        """BoxNutra mark_stale_variants returns list of dicts."""
        from boxnutra_scraper import mark_stale_variants

        cursor = sqlite_conn.cursor()
        old_time = (datetime.now() - timedelta(days=1)).isoformat()

        # Insert multiple old variants
        cursor.execute('''
            INSERT INTO vendoringredients (vendor_id, variant_id, sku, raw_product_name, status, last_seen_at)
            VALUES (25, 200, 'OLD-1', 'Old Product 1', 'active', ?)
        ''', (old_time,))
        cursor.execute('''
            INSERT INTO vendoringredients (vendor_id, variant_id, sku, raw_product_name, status, last_seen_at)
            VALUES (25, 201, 'OLD-2', 'Old Product 2', 'active', ?)
        ''', (old_time,))
        sqlite_conn.commit()

        scrape_start = datetime.now().isoformat()
        stale_variants = mark_stale_variants(sqlite_conn, 25, scrape_start)

        assert isinstance(stale_variants, list)
        assert len(stale_variants) == 2
        skus = [v['sku'] for v in stale_variants]
        assert 'OLD-1' in skus
        assert 'OLD-2' in skus

    def test_trafapharma_mark_stale_sets_stale_since(self, sqlite_conn):
        """TrafaPharma mark_stale_variants sets stale_since timestamp."""
        from trafapharma_scraper import mark_stale_variants

        cursor = sqlite_conn.cursor()
        old_time = (datetime.now() - timedelta(days=1)).isoformat()

        cursor.execute('''
            INSERT INTO vendoringredients (vendor_id, variant_id, sku, raw_product_name, status, last_seen_at)
            VALUES (26, 300, 'TP-OLD', 'Old TrafaPharma', 'active', ?)
        ''', (old_time,))
        sqlite_conn.commit()

        scrape_start = datetime.now().isoformat()
        mark_stale_variants(sqlite_conn, 26, scrape_start)
        sqlite_conn.commit()

        cursor.execute('SELECT status, stale_since FROM vendoringredients WHERE sku = ?', ('TP-OLD',))
        row = cursor.fetchone()

        assert row[0] == 'stale'
        assert row[1] is not None

    def test_io_mark_stale_with_stats(self, sqlite_conn):
        """IO mark_stale_variants works with stats tracking."""
        from IO_scraper import mark_stale_variants, StatsTracker

        cursor = sqlite_conn.cursor()
        old_time = (datetime.now() - timedelta(days=1)).isoformat()

        cursor.execute('''
            INSERT INTO vendoringredients (vendor_id, variant_id, sku, raw_product_name, status, last_seen_at)
            VALUES (1, 400, 'IO-OLD', 'Old IO Product', 'active', ?)
        ''', (old_time,))
        sqlite_conn.commit()

        stats = StatsTracker(vendor_id=1)
        scrape_start = datetime.now().isoformat()

        stale_variants = mark_stale_variants(sqlite_conn, 1, scrape_start, stats)

        assert len(stale_variants) == 1
        assert stats.variants_stale == 1


class TestSaveToRelationalWithStats:
    """Test save_to_relational_tables with StatsTracker integration."""

    def test_boxnutra_save_tracks_new_variants(self, sqlite_conn):
        """BoxNutra save_to_relational_tables tracks new variants in stats."""
        from boxnutra_scraper import save_to_relational_tables, StatsTracker

        stats = StatsTracker(vendor_id=25)

        rows = [{
            'product_id': 12345,
            'product_title': 'Test Product',
            'variant_id': 100001,
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
        }]

        save_to_relational_tables(sqlite_conn, rows, stats)
        sqlite_conn.commit()

        assert stats.variants_new == 1

    def test_trafapharma_save_tracks_new_variants(self, sqlite_conn):
        """TrafaPharma save_to_relational_tables tracks new variants in stats."""
        from trafapharma_scraper import save_to_relational_tables, StatsTracker

        stats = StatsTracker(vendor_id=26)

        rows = [{
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
        }]

        save_to_relational_tables(sqlite_conn, rows, stats)
        sqlite_conn.commit()

        assert stats.variants_new == 1


class TestGetExistingPriceAndStock:
    """Test helper functions for getting existing price and stock."""

    def test_bulksupplements_get_existing_price(self, sqlite_conn):
        """BulkSupplements get_existing_price returns correct value."""
        from bulksupplements_scraper import get_existing_price

        cursor = sqlite_conn.cursor()

        # Create vendor ingredient and price tier
        cursor.execute('''
            INSERT INTO vendoringredients (vendor_id, variant_id, sku, raw_product_name, status)
            VALUES (4, 100, 'PRICE-SKU', 'Product', 'active')
        ''')
        vi_id = cursor.lastrowid

        cursor.execute('''
            INSERT INTO pricetiers (vendor_ingredient_id, price, effective_date)
            VALUES (?, 99.99, ?)
        ''', (vi_id, datetime.now().isoformat()))
        sqlite_conn.commit()

        price = get_existing_price(sqlite_conn, vi_id)

        assert price == 99.99

    def test_boxnutra_get_existing_price_no_tiers(self, sqlite_conn):
        """BoxNutra get_existing_price returns None when no tiers exist."""
        from boxnutra_scraper import get_existing_price

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO vendoringredients (vendor_id, variant_id, sku, raw_product_name, status)
            VALUES (25, 200, 'NO-PRICE-SKU', 'Product', 'active')
        ''')
        vi_id = cursor.lastrowid
        sqlite_conn.commit()

        price = get_existing_price(sqlite_conn, vi_id)

        assert price is None

    def test_trafapharma_get_existing_stock_status(self, sqlite_conn):
        """TrafaPharma get_existing_stock_status returns correct value."""
        from trafapharma_scraper import get_existing_stock_status

        cursor = sqlite_conn.cursor()

        cursor.execute('''
            INSERT INTO vendoringredients (vendor_id, variant_id, sku, raw_product_name, status)
            VALUES (26, 300, 'STOCK-SKU', 'Product', 'active')
        ''')
        vi_id = cursor.lastrowid

        cursor.execute('''
            INSERT INTO vendorinventory (vendor_ingredient_id, stock_status)
            VALUES (?, 'in_stock')
        ''', (vi_id,))
        sqlite_conn.commit()

        status = get_existing_stock_status(sqlite_conn, vi_id)

        assert status == 'in_stock'


class TestPrintReport:
    """Test StatsTracker print_report doesn't crash."""

    def test_bulksupplements_print_report(self, capsys):
        """BulkSupplements print_report outputs correctly."""
        from bulksupplements_scraper import StatsTracker

        stats = StatsTracker(vendor_id=4, is_full_scrape=True)
        stats.products_discovered = 100
        stats.products_processed = 95
        stats.products_skipped = 3
        stats.products_failed = 2
        stats.variants_new = 10
        stats.variants_updated = 50
        stats.variants_unchanged = 35

        stats.record_new_product('SKU-1', 'New Product')
        stats.record_price_change('SKU-2', 'Price Drop', 100.0, 50.0)

        stats.print_report()

        captured = capsys.readouterr()
        assert 'SCRAPE STATISTICS REPORT' in captured.out
        assert 'Discovered:' in captured.out
        assert 'VARIANTS' in captured.out
        assert 'MAJOR PRICE CHANGES' in captured.out

    def test_boxnutra_print_report_empty_stats(self, capsys):
        """BoxNutra print_report works with empty stats."""
        from boxnutra_scraper import StatsTracker

        stats = StatsTracker(vendor_id=25)
        stats.print_report()

        captured = capsys.readouterr()
        assert 'SCRAPE STATISTICS REPORT' in captured.out


class TestRecordParseAndMissingRequired:
    """Test recording parse failures and missing required fields."""

    def test_bulksupplements_record_parse_failure(self):
        """BulkSupplements tracks parse failures."""
        from bulksupplements_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=4)
        stats.record_parse_failure('SKU-001', 'Product', 'price', 'invalid_price_value')

        assert len(stats.alerts) == 1
        assert stats.alerts[0].alert_type == AlertType.PARSE_FAILURE
        assert 'price' in stats.alerts[0].message

    def test_boxnutra_record_missing_required(self):
        """BoxNutra tracks missing required fields."""
        from boxnutra_scraper import StatsTracker, AlertType

        stats = StatsTracker(vendor_id=25)
        stats.record_missing_required('BN-SKU', 'Product', 'variant_sku')

        assert len(stats.alerts) == 1
        assert stats.alerts[0].alert_type == AlertType.MISSING_REQUIRED
        assert 'variant_sku' in stats.alerts[0].message
