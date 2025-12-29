"""
Tests for product filtering logic.
Covers skip logic for non-ingredient products and third-party vendors.
"""
import pytest


class TestBoxNutraProductFiltering:
    """Product filtering logic from boxnutra_scraper.py"""

    def test_should_skip_shipping_insurance(self):
        """Skip shipping insurance products."""
        from boxnutra_scraper import should_skip_product

        should_skip, reason = should_skip_product(
            "Shipping Insurance", "BoxNutra", "https://boxnutra.com/shipping-insurance"
        )
        assert should_skip is True
        assert "shipping insurance" in reason.lower()

    def test_should_skip_shipping_protection(self):
        """Skip shipping protection products."""
        from boxnutra_scraper import should_skip_product

        should_skip, reason = should_skip_product(
            "Shipping Protection Plan", "BoxNutra", "https://boxnutra.com/shipping-protection"
        )
        assert should_skip is True
        assert "shipping protection" in reason.lower()

    def test_should_skip_gift_card(self):
        """Skip gift card products."""
        from boxnutra_scraper import should_skip_product

        should_skip, reason = should_skip_product(
            "Gift Card $50", "BoxNutra", "https://boxnutra.com/gift-card"
        )
        assert should_skip is True
        assert "gift card" in reason.lower()

    def test_should_skip_extra_fee(self):
        """Skip extra fee products."""
        from boxnutra_scraper import should_skip_product

        should_skip, reason = should_skip_product(
            "Extra Fee for Rush Order", "BoxNutra", "https://boxnutra.com/extra-fee"
        )
        assert should_skip is True
        assert "extra fee" in reason.lower()

    def test_should_skip_deposit(self):
        """Skip deposit products."""
        from boxnutra_scraper import should_skip_product

        should_skip, reason = should_skip_product(
            "Deposit for Custom Order", "BoxNutra", "https://boxnutra.com/deposit"
        )
        assert should_skip is True
        assert "deposit" in reason.lower()

    def test_should_skip_bottle_caps(self):
        """Skip bottle caps products."""
        from boxnutra_scraper import should_skip_product

        should_skip, reason = should_skip_product(
            "Bottle Caps 100ct", "BoxNutra", "https://boxnutra.com/bottle-caps"
        )
        assert should_skip is True
        assert "bottle caps" in reason.lower()

    def test_should_skip_bottles_case(self):
        """Skip bottles case products."""
        from boxnutra_scraper import should_skip_product

        should_skip, reason = should_skip_product(
            "Bottles Case 24pk", "BoxNutra", "https://boxnutra.com/bottles-case"
        )
        assert should_skip is True
        assert "bottles case" in reason.lower()

    def test_should_skip_white_bottles(self):
        """Skip white bottles products."""
        from boxnutra_scraper import should_skip_product

        should_skip, reason = should_skip_product(
            "White Bottles 100cc", "BoxNutra", "https://boxnutra.com/white-bottles"
        )
        assert should_skip is True
        assert "white bottles" in reason.lower()

    def test_should_skip_third_party_vendor(self):
        """Skip third-party vendor products."""
        from boxnutra_scraper import should_skip_product

        should_skip, reason = should_skip_product(
            "Some Supplement", "OtherVendor", "https://boxnutra.com/some-product"
        )
        assert should_skip is True
        assert "third-party" in reason.lower()

    def test_should_not_skip_boxnutra_ingredient(self):
        """Don't skip regular BoxNutra ingredients."""
        from boxnutra_scraper import should_skip_product

        should_skip, reason = should_skip_product(
            "L-Glutamine Powder", "BoxNutra", "https://boxnutra.com/l-glutamine"
        )
        assert should_skip is False
        assert reason == ""

    def test_should_not_skip_boxnutra_case_insensitive(self):
        """BoxNutra vendor check is case-insensitive."""
        from boxnutra_scraper import should_skip_product

        should_skip, reason = should_skip_product(
            "Creatine Monohydrate", "boxnutra", "https://boxnutra.com/creatine"
        )
        assert should_skip is False

        should_skip, reason = should_skip_product(
            "BCAA Powder", "BOXNUTRA", "https://boxnutra.com/bcaa"
        )
        assert should_skip is False

    def test_should_skip_case_insensitive(self):
        """Skip product checks are case-insensitive."""
        from boxnutra_scraper import should_skip_product

        should_skip, _ = should_skip_product(
            "SHIPPING INSURANCE", "BoxNutra", "https://boxnutra.com/shipping"
        )
        assert should_skip is True

        should_skip, _ = should_skip_product(
            "Gift CARD Premium", "BoxNutra", "https://boxnutra.com/gift"
        )
        assert should_skip is True

    def test_should_skip_empty_vendor(self):
        """Empty vendor should be skipped as third-party."""
        from boxnutra_scraper import should_skip_product

        should_skip, reason = should_skip_product(
            "Some Product", "", "https://boxnutra.com/product"
        )
        assert should_skip is True
        assert "third-party" in reason.lower()

    def test_should_skip_none_vendor(self):
        """None vendor should be skipped as third-party."""
        from boxnutra_scraper import should_skip_product

        should_skip, reason = should_skip_product(
            "Some Product", None, "https://boxnutra.com/product"
        )
        assert should_skip is True
        assert "third-party" in reason.lower()


class TestBoxNutraSkippedLogging:
    """Test the skipped product logging."""

    def test_log_skipped_product(self):
        """Skipped products are logged correctly."""
        from boxnutra_scraper import log_skipped_product, skipped_products

        # Clear any existing entries
        skipped_products.clear()

        log_skipped_product(
            "Test Product",
            "TestVendor",
            "https://boxnutra.com/test",
            "test reason"
        )

        assert len(skipped_products) == 1
        assert skipped_products[0]['title'] == "Test Product"
        assert skipped_products[0]['vendor'] == "TestVendor"
        assert skipped_products[0]['reason'] == "test reason"
        assert 'timestamp' in skipped_products[0]

        # Clean up
        skipped_products.clear()
