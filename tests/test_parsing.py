"""
Tests for parsing functions across all scrapers.
These are pure functions with no database dependencies.
"""
import pytest


class TestBulkSupplementsParsing:
    """Parsing functions from bulksupplements_scraper.py"""

    def test_parse_pack_size_g_grams(self):
        """Parse gram-based pack sizes."""
        from bulksupplements_scraper import parse_pack_size_g

        assert parse_pack_size_g("100 Grams (3.5 oz)") == 100
        assert parse_pack_size_g("250 Grams (8.8 oz)") == 250
        assert parse_pack_size_g("500 Grams") == 500
        assert parse_pack_size_g("1000 grams") == 1000

    def test_parse_pack_size_g_kilograms(self):
        """Parse kilogram-based pack sizes."""
        from bulksupplements_scraper import parse_pack_size_g

        assert parse_pack_size_g("1 Kilogram (2.2 lbs)") == 1000
        assert parse_pack_size_g("5 Kilograms (11 lbs)") == 5000
        assert parse_pack_size_g("25 Kilograms (55 lbs)") == 25000

    def test_parse_pack_size_g_empty_or_invalid(self):
        """Handle empty or invalid inputs."""
        from bulksupplements_scraper import parse_pack_size_g

        assert parse_pack_size_g("") == 0
        assert parse_pack_size_g(None) == 0
        assert parse_pack_size_g("Capsules 100ct") == 0
        assert parse_pack_size_g("No number here") == 0

    def test_parse_pack_size_g_decimal(self):
        """Parse decimal gram values."""
        from bulksupplements_scraper import parse_pack_size_g

        assert parse_pack_size_g("2.5 Kilograms") == 2500

    def test_parse_packaging_kg(self):
        """Convert grams to kg."""
        from bulksupplements_scraper import parse_packaging_kg

        assert parse_packaging_kg(100) == 0.1
        assert parse_packaging_kg(250) == 0.25
        assert parse_packaging_kg(1000) == 1.0
        assert parse_packaging_kg(25000) == 25.0

    def test_parse_packaging_kg_edge_cases(self):
        """Edge cases for packaging kg conversion."""
        from bulksupplements_scraper import parse_packaging_kg

        assert parse_packaging_kg(0) is None
        assert parse_packaging_kg(None) is None
        assert parse_packaging_kg(-100) is None

    def test_calculate_price_per_kg(self):
        """Calculate price per kg from price and grams."""
        from bulksupplements_scraper import calculate_price_per_kg

        # $10 for 100g = $100/kg
        assert calculate_price_per_kg(10.0, 100) == 100.0
        # $50 for 1000g = $50/kg
        assert calculate_price_per_kg(50.0, 1000) == 50.0
        # $100 for 500g = $200/kg
        assert calculate_price_per_kg(100.0, 500) == 200.0

    def test_calculate_price_per_kg_edge_cases(self):
        """Edge cases for price per kg calculation."""
        from bulksupplements_scraper import calculate_price_per_kg

        assert calculate_price_per_kg(10.0, 0) == 0
        assert calculate_price_per_kg(10.0, -100) == 0
        assert calculate_price_per_kg(0, 100) == 0

    def test_convert_stock_status(self):
        """Convert boolean to stock status string."""
        from bulksupplements_scraper import convert_stock_status

        assert convert_stock_status(True) == 'in_stock'
        assert convert_stock_status(False) == 'out_of_stock'
        assert convert_stock_status(None) == 'unknown'


class TestBoxNutraParsing:
    """Parsing functions from boxnutra_scraper.py"""

    def test_calculate_price_per_kg(self):
        """Calculate price per kg from price and grams."""
        from boxnutra_scraper import calculate_price_per_kg

        # $10 for 100g = $100/kg
        assert calculate_price_per_kg(10.0, 100) == 100.0
        # $25 for 250g = $100/kg
        assert calculate_price_per_kg(25.0, 250) == 100.0

    def test_calculate_price_per_kg_edge_cases(self):
        """Edge cases for price per kg calculation."""
        from boxnutra_scraper import calculate_price_per_kg

        assert calculate_price_per_kg(10.0, 0) == 0
        assert calculate_price_per_kg(10.0, -100) == 0

    def test_convert_stock_status(self):
        """Convert boolean to stock status string."""
        from boxnutra_scraper import convert_stock_status

        assert convert_stock_status(True) == 'in_stock'
        assert convert_stock_status(False) == 'out_of_stock'
        assert convert_stock_status(None) == 'unknown'


class TestTrafaPharmaParsing:
    """Parsing functions from trafapharma_scraper.py"""

    def test_parse_price_basic(self):
        """Parse basic price strings."""
        from trafapharma_scraper import parse_price

        assert parse_price("$24.99") == 24.99
        assert parse_price("$ 795.00") == 795.00
        assert parse_price("$100") == 100.0

    def test_parse_price_with_commas(self):
        """Parse prices with thousand separators."""
        from trafapharma_scraper import parse_price

        assert parse_price("$ 1,195.00") == 1195.00
        assert parse_price("$10,000.00") == 10000.00

    def test_parse_price_inquire(self):
        """Inquire prices return None."""
        from trafapharma_scraper import parse_price

        assert parse_price("Inquire Bulk Price") is None
        assert parse_price("Bulk Price") is None
        assert parse_price("inquire") is None

    def test_parse_price_empty_or_invalid(self):
        """Empty or invalid inputs return None."""
        from trafapharma_scraper import parse_price

        assert parse_price("") is None
        assert parse_price(None) is None
        assert parse_price("not a price") is None

    def test_parse_size_to_kg_kilograms(self):
        """Parse kg-based sizes."""
        from trafapharma_scraper import parse_size_to_kg

        assert parse_size_to_kg("25kgs") == 25.0
        assert parse_size_to_kg("25 kgs") == 25.0
        assert parse_size_to_kg("1 kg") == 1.0
        assert parse_size_to_kg("2.5 kg") == 2.5

    def test_parse_size_to_kg_grams(self):
        """Parse gram-based sizes."""
        from trafapharma_scraper import parse_size_to_kg

        result = parse_size_to_kg("100g")
        assert result is not None
        assert abs(result - 0.1) < 0.001

        result = parse_size_to_kg("500 grams")
        assert result is not None
        assert abs(result - 0.5) < 0.001

        result = parse_size_to_kg("10g")
        assert result is not None
        assert abs(result - 0.01) < 0.001

    def test_parse_size_to_kg_pounds(self):
        """Parse pound-based sizes."""
        from trafapharma_scraper import parse_size_to_kg

        result = parse_size_to_kg("1 lb")
        assert result is not None
        assert abs(result - 0.45359237) < 0.01

        result = parse_size_to_kg("2.2 lbs")
        assert result is not None
        assert abs(result - 0.998) < 0.01

    def test_parse_size_to_kg_combined(self):
        """Parse sizes with both lbs and kg (prefer kg)."""
        from trafapharma_scraper import parse_size_to_kg

        # When both are present, kg should be preferred
        assert parse_size_to_kg("2.2 lbs/1 kg") == 1.0

    def test_parse_size_to_kg_invalid(self):
        """Invalid sizes return None."""
        from trafapharma_scraper import parse_size_to_kg

        assert parse_size_to_kg("Bulk Price") is None
        assert parse_size_to_kg("bulk") is None
        assert parse_size_to_kg("Select Size") is None
        assert parse_size_to_kg("") is None
        assert parse_size_to_kg(None) is None

    def test_extract_ingredient_name_percentage(self):
        """Remove percentage specifications."""
        from trafapharma_scraper import extract_ingredient_name

        assert "98%" not in extract_ingredient_name("5-HTP 98%")
        assert "5-HTP" in extract_ingredient_name("5-HTP 98%")
        assert "50%" not in extract_ingredient_name("Green Tea Extract 50% EGCG")

    def test_extract_ingredient_name_iu_specifications(self):
        """Remove IU/g specifications."""
        from trafapharma_scraper import extract_ingredient_name

        result = extract_ingredient_name("Vitamin D3 100,000 IU/g")
        assert "100,000" not in result
        assert "IU/g" not in result
        assert "Vitamin D3" in result

    def test_extract_ingredient_name_pe_and_ratios(self):
        """Remove P.E. and ratio specifications."""
        from trafapharma_scraper import extract_ingredient_name

        result = extract_ingredient_name("Ashwagandha Root P.E. 5% Withanolides")
        assert "P.E." not in result
        assert "5%" not in result
        assert "Ashwagandha" in result

        result = extract_ingredient_name("Ginseng 10:1 Extract")
        assert "10:1" not in result
        assert "Ginseng" in result

    def test_extract_ingredient_name_grade_specs(self):
        """Remove USP/NF/FCC grade specifications."""
        from trafapharma_scraper import extract_ingredient_name

        result = extract_ingredient_name("Ascorbic Acid USP Grade")
        assert "USP" not in result
        assert "Ascorbic Acid" in result

    def test_extract_ingredient_name_empty(self):
        """Empty input returns empty string."""
        from trafapharma_scraper import extract_ingredient_name

        assert extract_ingredient_name("") == ""
        assert extract_ingredient_name(None) == ""

    def test_calculate_price_per_kg(self):
        """Calculate price per kg."""
        from trafapharma_scraper import calculate_price_per_kg

        assert calculate_price_per_kg(100.0, 1.0) == 100.0
        assert calculate_price_per_kg(250.0, 25.0) == 10.0

    def test_calculate_price_per_kg_edge_cases(self):
        """Edge cases for price per kg calculation."""
        from trafapharma_scraper import calculate_price_per_kg

        assert calculate_price_per_kg(None, 1.0) is None
        assert calculate_price_per_kg(100.0, None) is None
        assert calculate_price_per_kg(100.0, 0) is None
        assert calculate_price_per_kg(100.0, -1.0) is None

    def test_extract_product_id_from_url_wishlist(self):
        """Extract product ID from wishlist URLs."""
        from trafapharma_scraper import extract_product_id_from_url

        assert extract_product_id_from_url("/cart/add_to_wishlist/889") == 889
        assert extract_product_id_from_url("/cart/add_to_wishlist/123") == 123
        assert extract_product_id_from_url("https://trafapharma.com/cart/add_to_wishlist/456") == 456

    def test_extract_product_id_from_url_enquiry(self):
        """Extract product ID from enquiry URLs."""
        from trafapharma_scraper import extract_product_id_from_url

        assert extract_product_id_from_url("/products/enquiry_now/716") == 716
        assert extract_product_id_from_url("https://trafapharma.com/products/enquiry_now/999") == 999

    def test_extract_product_id_from_url_invalid(self):
        """Invalid URLs return None."""
        from trafapharma_scraper import extract_product_id_from_url

        assert extract_product_id_from_url("/products/some-product") is None
        assert extract_product_id_from_url("https://trafapharma.com/vitamins") is None
        assert extract_product_id_from_url("") is None
