"""
Tests for get_or_create functions across all scrapers.
Tests idempotent create operations that return existing records or create new ones.
"""
import pytest


class TestGetOrCreateCategory:
    """Test get_or_create_category across all scrapers."""

    def test_creates_new_category_boxnutra(self, sqlite_conn):
        """BoxNutra: First call creates category, returns category_id."""
        from boxnutra_scraper import get_or_create_category
        cat_id = get_or_create_category(sqlite_conn, 'Vitamins')
        assert cat_id is not None
        assert isinstance(cat_id, int)

        # Verify in DB
        cursor = sqlite_conn.cursor()
        cursor.execute('SELECT name FROM categories WHERE category_id = ?', (cat_id,))
        assert cursor.fetchone()[0] == 'Vitamins'

    def test_creates_new_category_trafapharma(self, sqlite_conn):
        """TrafaPharma: First call creates category, returns category_id."""
        from trafapharma_scraper import get_or_create_category
        cat_id = get_or_create_category(sqlite_conn, 'Botanicals')
        assert cat_id is not None

        cursor = sqlite_conn.cursor()
        cursor.execute('SELECT name FROM categories WHERE category_id = ?', (cat_id,))
        assert cursor.fetchone()[0] == 'Botanicals'

    def test_returns_existing_category(self, sqlite_conn):
        """Second call returns same ID, no duplicate."""
        from boxnutra_scraper import get_or_create_category
        id1 = get_or_create_category(sqlite_conn, 'Minerals')
        id2 = get_or_create_category(sqlite_conn, 'Minerals')
        assert id1 == id2

        # Verify only one row exists
        cursor = sqlite_conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM categories WHERE name = ?', ('Minerals',))
        assert cursor.fetchone()[0] == 1

    def test_null_name_returns_none(self, sqlite_conn):
        """NULL/empty names return None, don't insert."""
        from boxnutra_scraper import get_or_create_category
        assert get_or_create_category(sqlite_conn, None) is None

    def test_empty_string_returns_none(self, sqlite_conn):
        """Empty string returns None."""
        from boxnutra_scraper import get_or_create_category
        result = get_or_create_category(sqlite_conn, '')
        assert result is None

    def test_case_sensitivity(self, sqlite_conn):
        """Categories are case-sensitive (SQLite default)."""
        from boxnutra_scraper import get_or_create_category
        id1 = get_or_create_category(sqlite_conn, 'vitamins')
        id2 = get_or_create_category(sqlite_conn, 'Vitamins')
        # SQLite is case-sensitive by default
        assert id1 != id2


class TestGetOrCreateIngredient:
    """Test get_or_create_ingredient across all scrapers."""

    def test_creates_with_category(self, sqlite_conn):
        """Creates ingredient linked to category."""
        from boxnutra_scraper import get_or_create_category, get_or_create_ingredient
        cat_id = get_or_create_category(sqlite_conn, 'Amino Acids')
        ing_id = get_or_create_ingredient(sqlite_conn, 'L-Glutamine', cat_id)

        cursor = sqlite_conn.cursor()
        cursor.execute('SELECT name, category_id FROM ingredients WHERE ingredient_id = ?', (ing_id,))
        row = cursor.fetchone()
        assert row[0] == 'L-Glutamine'
        assert row[1] == cat_id

    def test_creates_without_category(self, sqlite_conn):
        """Creates ingredient with NULL category_id."""
        from boxnutra_scraper import get_or_create_ingredient
        ing_id = get_or_create_ingredient(sqlite_conn, 'Creatine', None)

        cursor = sqlite_conn.cursor()
        cursor.execute('SELECT category_id FROM ingredients WHERE ingredient_id = ?', (ing_id,))
        assert cursor.fetchone()[0] is None

    def test_duplicate_name_returns_existing(self, sqlite_conn):
        """Same name returns same ID regardless of category change."""
        from boxnutra_scraper import get_or_create_ingredient, get_or_create_category
        cat1 = get_or_create_category(sqlite_conn, 'Cat1')
        cat2 = get_or_create_category(sqlite_conn, 'Cat2')

        id1 = get_or_create_ingredient(sqlite_conn, 'Beta-Alanine', cat1)
        id2 = get_or_create_ingredient(sqlite_conn, 'Beta-Alanine', cat2)  # Different category

        assert id1 == id2  # Same ingredient, category not updated

    def test_multiple_unique_ingredients(self, sqlite_conn):
        """Multiple unique ingredients created correctly."""
        from boxnutra_scraper import get_or_create_ingredient
        ing1 = get_or_create_ingredient(sqlite_conn, 'Ingredient A', None)
        ing2 = get_or_create_ingredient(sqlite_conn, 'Ingredient B', None)
        ing3 = get_or_create_ingredient(sqlite_conn, 'Ingredient C', None)

        assert len({ing1, ing2, ing3}) == 3  # All different IDs


class TestGetOrCreateManufacturer:
    """Test manufacturer creation patterns."""

    def test_creates_new_manufacturer(self, sqlite_conn):
        """Creates manufacturer record."""
        from boxnutra_scraper import get_or_create_manufacturer
        mfr_id = get_or_create_manufacturer(sqlite_conn, 'BoxNutra')
        assert mfr_id is not None

        cursor = sqlite_conn.cursor()
        cursor.execute('SELECT name FROM manufacturers WHERE manufacturer_id = ?', (mfr_id,))
        assert cursor.fetchone()[0] == 'BoxNutra'

    def test_unknown_manufacturer_for_trafapharma(self, sqlite_conn):
        """TrafaPharma always uses 'Unknown' manufacturer."""
        from trafapharma_scraper import get_or_create_manufacturer
        mfr_id = get_or_create_manufacturer(sqlite_conn, 'Unknown')

        cursor = sqlite_conn.cursor()
        cursor.execute('SELECT name FROM manufacturers WHERE manufacturer_id = ?', (mfr_id,))
        assert cursor.fetchone()[0] == 'Unknown'

    def test_returns_existing_manufacturer(self, sqlite_conn):
        """Second call returns same ID."""
        from boxnutra_scraper import get_or_create_manufacturer
        id1 = get_or_create_manufacturer(sqlite_conn, 'TestMfr')
        id2 = get_or_create_manufacturer(sqlite_conn, 'TestMfr')
        assert id1 == id2


class TestGetOrCreateVariant:
    """Test ingredient variant creation with composite key."""

    def test_creates_variant_with_composite_key(self, sqlite_conn):
        """Variant uniqueness is (ingredient_id, manufacturer_id, variant_name)."""
        from boxnutra_scraper import (get_or_create_ingredient,
                                       get_or_create_manufacturer,
                                       get_or_create_variant)
        ing_id = get_or_create_ingredient(sqlite_conn, 'Vitamin C', None)
        mfr_id = get_or_create_manufacturer(sqlite_conn, 'BulkSupplements')

        var_id = get_or_create_variant(sqlite_conn, ing_id, mfr_id, 'Ascorbic Acid Powder')
        assert var_id is not None

        cursor = sqlite_conn.cursor()
        cursor.execute('SELECT variant_name FROM ingredientvariants WHERE variant_id = ?', (var_id,))
        assert cursor.fetchone()[0] == 'Ascorbic Acid Powder'

    def test_same_variant_different_manufacturer(self, sqlite_conn):
        """Same variant name, different manufacturer = different variant."""
        from boxnutra_scraper import (get_or_create_ingredient,
                                       get_or_create_manufacturer,
                                       get_or_create_variant)
        ing_id = get_or_create_ingredient(sqlite_conn, 'Magnesium', None)
        mfr1 = get_or_create_manufacturer(sqlite_conn, 'Vendor1')
        mfr2 = get_or_create_manufacturer(sqlite_conn, 'Vendor2')

        var1 = get_or_create_variant(sqlite_conn, ing_id, mfr1, 'Mag Glycinate')
        var2 = get_or_create_variant(sqlite_conn, ing_id, mfr2, 'Mag Glycinate')

        assert var1 != var2

    def test_same_variant_same_keys_returns_existing(self, sqlite_conn):
        """Same composite key returns same variant_id."""
        from boxnutra_scraper import (get_or_create_ingredient,
                                       get_or_create_manufacturer,
                                       get_or_create_variant)
        ing_id = get_or_create_ingredient(sqlite_conn, 'Zinc', None)
        mfr_id = get_or_create_manufacturer(sqlite_conn, 'TestMfr')

        var1 = get_or_create_variant(sqlite_conn, ing_id, mfr_id, 'Zinc Picolinate')
        var2 = get_or_create_variant(sqlite_conn, ing_id, mfr_id, 'Zinc Picolinate')

        assert var1 == var2
