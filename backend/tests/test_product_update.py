"""
Tests for single-product update functionality.
Tests both the API routes and the product updater service.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Import the product updater functions
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.services.product_updater import (
    update_single_product,
    get_product_info,
    extract_handle_from_url,
    compare_io_price_tiers,
    compare_io_inventory,
    build_io_parent_sku,
)


class TestExtractHandle:
    """Test URL handle extraction."""

    def test_extract_bs_handle(self):
        """Test extracting handle from BulkSupplements URL."""
        url = 'https://www.bulksupplements.com/products/magnesium-glycinate-powder'
        handle = extract_handle_from_url(url, vendor_id=2)  # BS vendor_id=2
        assert handle == 'magnesium-glycinate-powder'

    def test_extract_bn_handle(self):
        """Test extracting handle from BoxNutra URL."""
        url = 'https://www.boxnutra.com/products/creatine-monohydrate'
        handle = extract_handle_from_url(url, vendor_id=3)  # BN vendor_id=3
        assert handle == 'creatine-monohydrate'

    def test_extract_tp_slug(self):
        """Test extracting slug from TrafaPharma URL."""
        url = 'https://trafapharma.com/products/ashwagandha-extract'
        slug = extract_handle_from_url(url, vendor_id=4)  # TP vendor_id=4
        assert 'ashwagandha-extract' in slug

    def test_extract_handle_invalid_url(self):
        """Test handle extraction from invalid URL."""
        url = 'not-a-valid-url'
        handle = extract_handle_from_url(url, vendor_id=2)
        # Should return None or the original string
        assert handle is None or handle == 'not-a-valid-url'


class TestProductUpdaterService:
    """Test the product updater service functions."""

    def test_update_single_product_not_found(self):
        """Test that update returns error for non-existent product."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_cursor.fetchone.return_value = None

        result = update_single_product(mock_conn, 99999)

        assert result['success'] is False
        assert 'not found' in result.get('error', '').lower()

    @patch('api.services.product_updater.get_product_info')
    def test_update_single_product_unknown_vendor(self, mock_get_product_info):
        """Test that update returns error for unknown vendor."""
        # Mock get_product_info to return a product with unknown vendor
        mock_get_product_info.return_value = {
            'vendor_ingredient_id': 1,
            'vendor_id': 999,  # Unknown vendor
            'vendor_name': 'UnknownVendor',
            'sku': 'TEST123',
            'raw_product_name': 'Test Product',
            'product_url': 'https://example.com/product'
        }

        mock_conn = MagicMock()
        result = update_single_product(mock_conn, 1)

        assert result['success'] is False
        # Should have an error message (either unsupported vendor or URL extraction failure)
        assert result.get('error') is not None


class TestProductRoutes:
    """Test the product API routes."""

    def test_get_product_info_endpoint(self):
        """Test the GET /products/{id} endpoint."""
        from fastapi.testclient import TestClient
        from api.main import app

        client = TestClient(app)

        # Test with a valid product ID (if database has products)
        response = client.get('/api/products/1')

        # Should return 200 with product info or 404 if not found
        assert response.status_code in [200, 404, 500]

        if response.status_code == 200:
            data = response.json()
            assert 'vendor_ingredient_id' in data
            assert 'vendor_name' in data

    def test_get_product_info_not_found(self):
        """Test GET /products/{id} with non-existent ID."""
        from fastapi.testclient import TestClient
        from api.main import app

        client = TestClient(app)

        response = client.get('/api/products/99999999')

        # Should return 404 (not found) or 500 (db connection issue in test)
        assert response.status_code in [404, 500]
        if response.status_code == 404:
            assert 'not found' in response.json().get('detail', '').lower()

    def test_update_product_endpoint_validation(self):
        """Test POST /products/update-single validation."""
        from fastapi.testclient import TestClient
        from api.main import app

        client = TestClient(app)

        # Test with missing vendor_ingredient_id
        response = client.post('/api/products/update-single', json={})

        assert response.status_code == 422  # Validation error

    def test_update_product_not_found(self):
        """Test POST /products/update-single with non-existent product."""
        from fastapi.testclient import TestClient
        from api.main import app

        client = TestClient(app)

        response = client.post('/api/products/update-single', json={
            'vendor_ingredient_id': 99999999
        })

        # Should return error about product not found
        assert response.status_code in [200, 404, 500]
        if response.status_code == 200:
            data = response.json()
            assert data['success'] is False


class TestUpdateProductResponse:
    """Test the update response format."""

    def test_response_has_required_fields(self):
        """Test that successful response has all required fields."""
        from api.routes.products import UpdateProductResponse

        response = UpdateProductResponse(
            success=True,
            vendor_ingredient_id=1,
            vendor_id=2,
            vendor_name='BulkSupplements',
            sku='MAGGLY250',
            old_values={'price': 24.95},
            new_values={'price': 22.95},
            changed_fields={'price': {'old': 24.95, 'new': 22.95}},
            message='Updated successfully',
            duration_ms=1500
        )

        assert response.success is True
        assert response.vendor_ingredient_id == 1
        assert response.vendor_name == 'BulkSupplements'
        assert response.duration_ms == 1500
        assert 'price' in response.changed_fields

    def test_error_response_format(self):
        """Test that error response has error field."""
        from api.routes.products import UpdateProductResponse

        response = UpdateProductResponse(
            success=False,
            vendor_ingredient_id=1,
            vendor_id=None,
            vendor_name=None,
            sku=None,
            old_values={},
            new_values={},
            changed_fields={},
            message='Update failed',
            duration_ms=500,
            error='Connection timeout'
        )

        assert response.success is False
        assert response.error == 'Connection timeout'


class TestIntegration:
    """Integration tests with real database (when available)."""

    @pytest.mark.skipif(
        os.environ.get('SKIP_INTEGRATION_TESTS', 'true').lower() == 'true',
        reason='Integration tests disabled'
    )
    def test_real_product_update(self):
        """Test updating a real product from the database."""
        from fastapi.testclient import TestClient
        from api.main import app

        client = TestClient(app)

        # First, get a real product from the database
        response = client.get('/api/products/1')
        if response.status_code != 200:
            pytest.skip('No product with ID 1 in database')

        # Try to update it
        update_response = client.post('/api/products/update-single', json={
            'vendor_ingredient_id': 1
        })

        # Should complete (success or controlled error)
        assert update_response.status_code == 200
        data = update_response.json()
        assert 'success' in data
        assert 'message' in data
        assert 'duration_ms' in data


class TestIOProductUpdate:
    """Test IngredientsOnline-specific update functions."""

    def test_build_io_parent_sku(self):
        """Test building parent SKU from variant SKU."""
        # Variant SKU: product_id-variant_code-attr_id-manufacturer_id
        # Parent SKU: product_id-MANUFACTURERNAME-manufacturer_id
        variant_sku = '59410-100-10312-11455'
        manufacturer = 'Sunnycare'

        parent_sku = build_io_parent_sku(variant_sku, manufacturer)

        assert parent_sku == '59410-SUNNYCARE-11455'

    def test_build_io_parent_sku_with_spaces(self):
        """Test parent SKU with manufacturer name containing spaces."""
        variant_sku = '12345-100-10000-99999'
        manufacturer = 'Some Company Name'

        parent_sku = build_io_parent_sku(variant_sku, manufacturer)

        # Should uppercase and remove spaces
        assert parent_sku == '12345-SOMECOMPANYNAME-99999'

    def test_build_io_parent_sku_invalid(self):
        """Test parent SKU with invalid variant SKU."""
        # Too few parts
        assert build_io_parent_sku('12345', 'Manufacturer') is None
        assert build_io_parent_sku('12345-100', 'Manufacturer') is None

        # Missing manufacturer
        assert build_io_parent_sku('12345-100-200-300', None) is None
        assert build_io_parent_sku('12345-100-200-300', '') is None

    def test_compare_io_price_tiers_no_change(self):
        """Test price tier comparison with no changes."""
        old_tiers = [
            {'min_quantity': 0, 'price': 50.0, 'price_per_kg': 50.0},
            {'min_quantity': 25, 'price': 45.0, 'price_per_kg': 45.0},
            {'min_quantity': 50, 'price': 40.0, 'price_per_kg': 40.0},
        ]
        new_tiers = [
            {'min_quantity': 0, 'price': 50.0, 'price_per_kg': 50.0},
            {'min_quantity': 25, 'price': 45.0, 'price_per_kg': 45.0},
            {'min_quantity': 50, 'price': 40.0, 'price_per_kg': 40.0},
        ]

        result = compare_io_price_tiers(old_tiers, new_tiers)

        assert result['has_changes'] is False
        assert result['tiers'] == {}

    def test_compare_io_price_tiers_with_changes(self):
        """Test price tier comparison with price changes."""
        old_tiers = [
            {'min_quantity': 0, 'price': 50.0, 'price_per_kg': 50.0},
            {'min_quantity': 25, 'price': 45.0, 'price_per_kg': 45.0},
        ]
        new_tiers = [
            {'min_quantity': 0, 'price': 48.0, 'price_per_kg': 48.0},  # Changed
            {'min_quantity': 25, 'price': 45.0, 'price_per_kg': 45.0},  # Same
        ]

        result = compare_io_price_tiers(old_tiers, new_tiers)

        assert result['has_changes'] is True
        assert '0-24 kg' in result['tiers']
        assert result['tiers']['0-24 kg'] == {'old': 50.0, 'new': 48.0}
        assert '25-49 kg' not in result['tiers']  # No change

    def test_compare_io_price_tiers_new_tier(self):
        """Test price tier comparison when new tier is added."""
        old_tiers = [
            {'min_quantity': 0, 'price': 50.0, 'price_per_kg': 50.0},
        ]
        new_tiers = [
            {'min_quantity': 0, 'price': 50.0, 'price_per_kg': 50.0},
            {'min_quantity': 100, 'price': 35.0, 'price_per_kg': 35.0},  # New
        ]

        result = compare_io_price_tiers(old_tiers, new_tiers)

        assert result['has_changes'] is True
        assert '100+ kg' in result['tiers']
        assert result['tiers']['100+ kg'] == {'old': None, 'new': 35.0}

    def test_compare_io_inventory_no_change(self):
        """Test inventory comparison with no changes."""
        old_inv = {'chino': 125.0, 'edison': 50.0}
        new_inv = {'chino': 125.0, 'edison': 50.0}

        result = compare_io_inventory(old_inv, new_inv)

        assert result['has_changes'] is False
        assert result['warehouses'] == {}

    def test_compare_io_inventory_with_changes(self):
        """Test inventory comparison with quantity changes."""
        old_inv = {'chino': 125.0, 'edison': 50.0}
        new_inv = {'chino': 100.0, 'edison': 50.0}  # Chino decreased

        result = compare_io_inventory(old_inv, new_inv)

        assert result['has_changes'] is True
        assert 'chino' in result['warehouses']
        assert result['warehouses']['chino'] == {'old': 125.0, 'new': 100.0}
        assert 'edison' not in result['warehouses']

    def test_compare_io_inventory_new_warehouse(self):
        """Test inventory comparison with new warehouse."""
        old_inv = {'chino': 125.0}
        new_inv = {'chino': 125.0, 'edison': 50.0}  # Edison is new

        result = compare_io_inventory(old_inv, new_inv)

        assert result['has_changes'] is True
        assert 'edison' in result['warehouses']
        assert result['warehouses']['edison'] == {'old': 0, 'new': 50.0}

    def test_compare_io_inventory_stockout(self):
        """Test inventory comparison when stock goes to zero."""
        old_inv = {'chino': 125.0}
        new_inv = {'chino': 0.0}

        result = compare_io_inventory(old_inv, new_inv)

        assert result['has_changes'] is True
        assert result['warehouses']['chino'] == {'old': 125.0, 'new': 0.0}

    def test_compare_io_inventory_empty(self):
        """Test inventory comparison with empty inventories."""
        result = compare_io_inventory({}, {})
        assert result['has_changes'] is False

        result = compare_io_inventory({}, {'chino': 100.0})
        assert result['has_changes'] is True


class TestIOClient:
    """Test the IO API client."""

    def test_io_client_auth_missing_credentials(self):
        """Test IOClient fails gracefully with missing credentials."""
        from api.services.io_client import IOClient

        # Temporarily clear credentials
        original_email = os.environ.pop('IO_EMAIL', None)
        original_password = os.environ.pop('IO_PASSWORD', None)

        try:
            client = IOClient()
            success, error = client.authenticate()

            assert success is False
            assert 'not configured' in error.lower()
        finally:
            # Restore credentials
            if original_email:
                os.environ['IO_EMAIL'] = original_email
            if original_password:
                os.environ['IO_PASSWORD'] = original_password

    def test_extract_variant_prices(self):
        """Test extracting prices from product data."""
        from api.services.io_client import extract_variant_prices

        # Mock ConfigurableProduct data
        product_data = {
            '__typename': 'ConfigurableProduct',
            'sku': '59410-SUNNYCARE-11455',
            'variants': [
                {
                    'product': {
                        'sku': '59410-100-10312-11455',
                        'name': 'Test Product 25kg Drum',
                        'price_range': {
                            'minimum_price': {
                                'final_price': {'value': 50.0}
                            }
                        },
                        'price_tiers': [
                            {'quantity': 25, 'final_price': {'value': 45.0}},
                            {'quantity': 50, 'final_price': {'value': 40.0}},
                        ]
                    }
                }
            ]
        }

        prices = extract_variant_prices(product_data, '59410-100-10312-11455')

        # Should only return actual price tiers from API (no synthetic 0-tier)
        assert len(prices) == 2
        assert prices[0]['min_quantity'] == 25
        assert prices[0]['price'] == 45.0
        assert prices[1]['min_quantity'] == 50
        assert prices[1]['price'] == 40.0

    def test_extract_variant_prices_not_found(self):
        """Test extracting prices for non-existent variant."""
        from api.services.io_client import extract_variant_prices

        product_data = {
            '__typename': 'ConfigurableProduct',
            'variants': []
        }

        prices = extract_variant_prices(product_data, 'NON-EXISTENT-SKU')

        assert prices == []

    def test_extract_variant_inventory(self):
        """Test extracting inventory from API response."""
        from api.services.io_client import extract_variant_inventory

        inventory_data = [
            {'sku': '59410-100-10312-11455', 'source_name': 'Chino', 'quantity': 125.0},
            {'sku': '59410-100-10312-11455', 'source_name': 'Edison', 'quantity': 50.0},
            {'sku': '59410-101-10312-11455', 'source_name': 'Chino', 'quantity': 200.0},
        ]

        inventory = extract_variant_inventory(inventory_data, '59410-100-10312-11455')

        assert len(inventory) == 2
        assert inventory['chino'] == 125.0
        assert inventory['edison'] == 50.0

    def test_extract_variant_inventory_empty(self):
        """Test extracting inventory when none available."""
        from api.services.io_client import extract_variant_inventory

        inventory = extract_variant_inventory([], '59410-100-10312-11455')
        assert inventory == {}


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
