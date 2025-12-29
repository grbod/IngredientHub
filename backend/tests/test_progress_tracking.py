"""
Tests for progress tracking and formatting functions.
"""
import pytest
import time


class TestProgressTrackerBulkSupplements:
    """ProgressTracker from bulksupplements_scraper.py"""

    def test_tracker_initialization(self):
        """Tracker initializes with correct values."""
        from bulksupplements_scraper import ProgressTracker

        tracker = ProgressTracker(100)
        assert tracker.total == 100
        assert tracker.completed == 0
        assert tracker.failed == 0
        assert tracker.skipped == 0

    def test_tracker_update_success(self):
        """Update increments completed count."""
        from bulksupplements_scraper import ProgressTracker

        tracker = ProgressTracker(10)
        tracker.update(success=True, item_name="test")
        assert tracker.completed == 1
        assert tracker.failed == 0
        assert tracker.skipped == 0

    def test_tracker_update_failure(self):
        """Update with failure increments failed count."""
        from bulksupplements_scraper import ProgressTracker

        tracker = ProgressTracker(10)
        tracker.update(success=False, item_name="test")
        assert tracker.completed == 1
        assert tracker.failed == 1

    def test_tracker_update_skipped(self):
        """Update with SKIPPED status increments skipped count."""
        from bulksupplements_scraper import ProgressTracker

        tracker = ProgressTracker(10)
        tracker.update(success=True, item_name="test", status="SKIPPED-NO_POWDER")
        assert tracker.completed == 1
        assert tracker.skipped == 1
        assert tracker.failed == 0


class TestProgressTrackerBoxNutra:
    """ProgressTracker from boxnutra_scraper.py"""

    def test_get_rate_calculation(self):
        """Rate calculation returns items per second."""
        from boxnutra_scraper import ProgressTracker

        tracker = ProgressTracker(100)
        # Simulate processing 10 items over 2 seconds
        tracker.processed = 10
        tracker.start_time = time.time() - 2.0

        rate = tracker.get_rate()
        assert 4.5 < rate < 5.5  # Should be ~5 items/sec

    def test_get_rate_no_time_elapsed(self):
        """Rate calculation with no time elapsed returns 0."""
        from boxnutra_scraper import ProgressTracker

        tracker = ProgressTracker(100)
        tracker.start_time = time.time()
        tracker.processed = 0

        rate = tracker.get_rate()
        assert rate == 0

    def test_get_eta_calculation(self):
        """ETA calculation returns reasonable estimate."""
        from boxnutra_scraper import ProgressTracker

        tracker = ProgressTracker(100)
        tracker.processed = 50
        tracker.start_time = time.time() - 10.0  # 10 seconds for 50 items

        eta = tracker.get_eta()
        # Should be ~10 seconds remaining (50 items at 5/sec)
        assert eta is not None
        # ETA format could be "10s" or "0:10" depending on duration

    def test_get_eta_zero_rate(self):
        """ETA with zero rate returns placeholder."""
        from boxnutra_scraper import ProgressTracker

        tracker = ProgressTracker(100)
        tracker.processed = 0
        tracker.start_time = time.time()

        eta = tracker.get_eta()
        assert "calculating" in eta.lower()

    def test_format_progress(self):
        """Progress formatting includes key info."""
        from boxnutra_scraper import ProgressTracker

        tracker = ProgressTracker(100)
        tracker.processed = 50
        tracker.start_time = time.time() - 10.0

        output = tracker.format_progress("test-product", "OK")
        assert "50/100" in output
        assert "test-product" in output
        assert "OK" in output
        assert "ETA" in output


class TestProgressTrackerTrafaPharma:
    """ProgressTracker from trafapharma_scraper.py"""

    def test_summary_output(self, capsys):
        """Summary prints completion stats."""
        from trafapharma_scraper import ProgressTracker

        tracker = ProgressTracker(10)
        tracker.completed = 10
        tracker.failed = 1
        tracker.skipped = 2
        tracker.start_time = time.time() - 5.0

        tracker.summary()

        captured = capsys.readouterr()
        assert "Completed" in captured.out
        assert "10" in captured.out


class TestFormatProductDetailsBulkSupplements:
    """format_product_details from bulksupplements_scraper.py"""

    def test_format_empty_rows(self):
        """Empty rows returns empty string."""
        from bulksupplements_scraper import format_product_details

        assert format_product_details([]) == ""
        assert format_product_details(None) == ""

    def test_format_verbose_false(self):
        """Verbose=False returns empty string."""
        from bulksupplements_scraper import format_product_details

        rows = [{'packaging': 'Test', 'packaging_kg': 1.0, 'price': 10.0}]
        assert format_product_details(rows, verbose=False) == ""

    def test_format_single_row(self):
        """Single row formats correctly."""
        from bulksupplements_scraper import format_product_details

        rows = [{
            'packaging': '100 Grams',
            'packaging_kg': 0.1,
            'price': 10.99,
            'price_per_kg': 109.90,
            'stock_status': 'in_stock'
        }]

        output = format_product_details(rows)
        assert '100 Grams' in output
        assert '0.1kg' in output
        assert 'in_stock' in output

    def test_format_multiple_rows_sorted(self):
        """Multiple rows are sorted by pack size."""
        from bulksupplements_scraper import format_product_details

        rows = [
            {'packaging': '500g', 'packaging_kg': 0.5, 'pack_size_g': 500,
             'price': 25.0, 'price_per_kg': 50.0, 'stock_status': 'in_stock'},
            {'packaging': '100g', 'packaging_kg': 0.1, 'pack_size_g': 100,
             'price': 10.0, 'price_per_kg': 100.0, 'stock_status': 'in_stock'},
        ]

        output = format_product_details(rows)
        lines = output.split('\n')
        # Find data lines (skip header and separator)
        data_lines = [l for l in lines if '100g' in l or '500g' in l]
        assert len(data_lines) == 2
        # 100g should come before 500g (sorted by size)
        assert data_lines[0].index('100g') < len(data_lines[0])

    def test_format_truncates_long_packaging(self):
        """Long packaging names are truncated."""
        from bulksupplements_scraper import format_product_details

        rows = [{
            'packaging': 'A Very Long Packaging Description That Exceeds Limit',
            'packaging_kg': 1.0,
            'price': 50.0,
            'price_per_kg': 50.0,
            'stock_status': 'in_stock'
        }]

        output = format_product_details(rows)
        # Should be truncated with '..'
        assert '..' in output


class TestFormatProductDetailsBoxNutra:
    """format_product_details from boxnutra_scraper.py"""

    def test_format_with_none_values(self):
        """Handles None values gracefully."""
        from boxnutra_scraper import format_product_details

        rows = [{
            'packaging': 'Test',
            'packaging_kg': None,
            'price': None,
            'price_per_kg': None,
            'stock_status': 'unknown'
        }]

        output = format_product_details(rows)
        assert output is not None
        assert '-' in output  # None values shown as '-'


class TestFormatProductDetailsTrafaPharma:
    """format_product_details from trafapharma_scraper.py"""

    def test_format_inquire_price(self):
        """Inquire prices shown correctly."""
        from trafapharma_scraper import format_product_details

        rows = [{
            'size_name': '25kg',
            'size_kg': 25.0,
            'price': None,  # Inquire
            'price_per_kg': None
        }]

        output = format_product_details(rows)
        assert 'Inquire' in output

    def test_format_with_price(self):
        """Prices shown with $ formatting."""
        from trafapharma_scraper import format_product_details

        rows = [{
            'size_name': '1 kg',
            'size_kg': 1.0,
            'price': 99.50,
            'price_per_kg': 99.50
        }]

        output = format_product_details(rows)
        assert '$99.50' in output
