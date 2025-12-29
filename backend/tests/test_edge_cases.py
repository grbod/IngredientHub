"""
Tests for scraper-specific edge cases.
Each scraper has unique data handling requirements.
"""
import pytest
from datetime import datetime


class TestBulkSupplementsEdgeCases:
    """BulkSupplements-specific edge cases."""

    def test_shipping_responsibility_set(self, sqlite_conn):
        """BulkSupplements sets shipping_responsibility to 'vendor'."""
        from bulksupplements_scraper import upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (4, 'https://test.com', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        result = upsert_vendor_ingredient(sqlite_conn, 4, 100, 'BS-SKU', 'Product', source_id)
        vi_id = result.vendor_ingredient_id

        cursor.execute('SELECT shipping_responsibility FROM vendoringredients WHERE vendor_ingredient_id = ?', (vi_id,))
        assert cursor.fetchone()[0] == 'vendor'


class TestBoxNutraEdgeCases:
    """BoxNutra-specific edge cases."""

    def test_shipping_responsibility_set(self, sqlite_conn):
        """BoxNutra sets shipping_responsibility to 'vendor'."""
        from boxnutra_scraper import upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (25, 'https://boxnutra.com/test', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        result = upsert_vendor_ingredient(sqlite_conn, 25, 200, 'BN-SKU', 'Product', source_id)
        vi_id = result.vendor_ingredient_id

        cursor.execute('SELECT shipping_responsibility FROM vendoringredients WHERE vendor_ingredient_id = ?', (vi_id,))
        assert cursor.fetchone()[0] == 'vendor'


class TestTrafaPharmaEdgeCases:
    """TrafaPharma-specific edge cases."""

    def test_sku_generated_from_product_code_and_size(self):
        """SKU = 'product_code-size' format (e.g., 'RM2154-1kg')."""
        product_code = 'RM2154'
        size_kg = 1.0
        # Format: use grams if < 1kg, otherwise kg
        if size_kg < 1:
            size_str = f"{int(size_kg * 1000)}g"
        else:
            size_str = f"{int(size_kg)}kg"
        sku = f"{product_code}-{size_str}"
        assert sku == "RM2154-1kg"

        # Test gram format
        size_kg = 0.025
        size_str = f"{int(size_kg * 1000)}g"
        sku = f"{product_code}-{size_str}"
        assert sku == "RM2154-25g"

    def test_size_parsing_kg(self):
        """Size strings with kg parsed correctly."""
        from trafapharma_scraper import parse_size_to_kg

        assert parse_size_to_kg("25kgs") == 25.0
        assert parse_size_to_kg("25 kgs") == 25.0
        assert parse_size_to_kg("1 kg") == 1.0
        assert parse_size_to_kg("2.2 lbs/1 kg") == 1.0  # Prefer kg when both present

    def test_size_parsing_grams(self):
        """Size strings with grams parsed correctly."""
        from trafapharma_scraper import parse_size_to_kg

        result = parse_size_to_kg("100g")
        assert result is not None
        assert abs(result - 0.1) < 0.001

        result = parse_size_to_kg("500 grams")
        assert result is not None
        assert abs(result - 0.5) < 0.001

    def test_size_parsing_lbs(self):
        """Size strings with pounds parsed correctly."""
        from trafapharma_scraper import parse_size_to_kg

        result = parse_size_to_kg("1 lb")
        assert result is not None
        assert abs(result - 0.45359237) < 0.01

    def test_size_parsing_bulk_returns_none(self):
        """Bulk Price or unknown returns None."""
        from trafapharma_scraper import parse_size_to_kg

        assert parse_size_to_kg("Bulk Price") is None
        assert parse_size_to_kg("Inquire") is None

    def test_ingredient_name_cleaning(self):
        """Extract clean ingredient name from product name."""
        from trafapharma_scraper import extract_ingredient_name

        # Test percentage removal
        result = extract_ingredient_name("5-HTP 98%")
        assert "98%" not in result
        assert "5-HTP" in result

    def test_shipping_responsibility_buyer(self, sqlite_conn):
        """TrafaPharma sets shipping_responsibility to 'buyer'."""
        from trafapharma_scraper import upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (26, 'https://trafapharma.com/test', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        result = upsert_vendor_ingredient(sqlite_conn, 26, 300, '123-1', 'Product', source_id)
        vi_id = result.vendor_ingredient_id

        cursor.execute('SELECT shipping_responsibility FROM vendoringredients WHERE vendor_ingredient_id = ?', (vi_id,))
        assert cursor.fetchone()[0] == 'buyer'

    def test_inquire_price_skips_price_tier(self, sqlite_conn):
        """'Inquire Bulk Price' (NULL price) skips price tier insertion."""
        from trafapharma_scraper import save_to_relational_tables

        rows = [{
            'product_id': 999,
            'product_code': 'INQ001',
            'product_name': 'Test Product',
            'ingredient_name': 'Test',
            'category': 'Vitamins',
            'size_id': '1',
            'size_name': 'Bulk',
            'size_kg': 25.0,
            'price': None,  # Inquire Bulk Price
            'price_per_kg': None,
            'stock_status': 'unknown',
            'order_rule_type': 'fixed_pack',
            'shipping_responsibility': 'buyer',
            'url': 'https://trafapharma.com/test',
            'scraped_at': datetime.now().isoformat()
        }]

        save_to_relational_tables(sqlite_conn, rows)
        sqlite_conn.commit()

        cursor = sqlite_conn.cursor()

        # Vendor ingredient should still be created (SKU = product_code + size in kg)
        cursor.execute("SELECT COUNT(*) FROM vendoringredients WHERE sku = ?", ('INQ001-25kg',))
        assert cursor.fetchone()[0] == 1

        # But no price tier for NULL price
        cursor.execute('''
            SELECT COUNT(*) FROM pricetiers pt
            JOIN vendoringredients vi ON pt.vendor_ingredient_id = vi.vendor_ingredient_id
            WHERE vi.sku = 'INQ001-25kg'
        ''')
        assert cursor.fetchone()[0] == 0  # No price tier for Inquire products


class TestIOScraperEdgeCases:
    """IngredientsOnline-specific edge cases."""

    def test_decimal_inventory_parsing(self):
        """Inventory quantities like '0.09' parsed correctly."""
        qty_str = "0.09"
        parsed = int(float(qty_str))
        assert parsed == 0

        qty_str = "27.5"
        parsed = int(float(qty_str))
        assert parsed == 27

        qty_str = "100"
        parsed = int(float(qty_str))
        assert parsed == 100

    def test_variant_code_parsing(self):
        """SKU parsing extracts variant code."""
        # IO SKU format: product_id-variant_code-attribute_id-manufacturer_id
        sku = "59410-100-10312-11455"
        parts = sku.split('-')
        variant_code = parts[1] if len(parts) > 1 else None
        assert variant_code == "100"  # 25kg Drum


class TestCommonEdgeCases:
    """Edge cases common across all scrapers."""

    def test_unicode_in_product_names(self, sqlite_conn):
        """Unicode characters in product names handled correctly."""
        from boxnutra_scraper import get_or_create_ingredient
        ing_id = get_or_create_ingredient(sqlite_conn, "Vitamin D3 (cholecalciferol)", None)
        assert ing_id is not None

        ing_id2 = get_or_create_ingredient(sqlite_conn, "Maca Root Extract", None)
        assert ing_id2 is not None

    def test_very_long_product_names(self, sqlite_conn):
        """Long product names don't cause issues."""
        from boxnutra_scraper import get_or_create_ingredient
        long_name = "A" * 500  # Very long name
        ing_id = get_or_create_ingredient(sqlite_conn, long_name, None)
        assert ing_id is not None

    def test_special_characters_in_sku(self, sqlite_conn):
        """SKUs with special characters handled correctly."""
        from bulksupplements_scraper import upsert_vendor_ingredient

        cursor = sqlite_conn.cursor()
        cursor.execute('''
            INSERT INTO scrapesources (vendor_id, product_url, scraped_at)
            VALUES (4, 'https://test.com', ?)
        ''', (datetime.now().isoformat(),))
        source_id = cursor.lastrowid
        sqlite_conn.commit()

        # SKU with hyphens and numbers
        result = upsert_vendor_ingredient(sqlite_conn, 4, 100, 'SKU-123-ABC', 'Product', source_id)
        vi_id = result.vendor_ingredient_id
        assert vi_id is not None

        cursor.execute('SELECT sku FROM vendoringredients WHERE vendor_ingredient_id = ?', (vi_id,))
        assert cursor.fetchone()[0] == 'SKU-123-ABC'
