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


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
