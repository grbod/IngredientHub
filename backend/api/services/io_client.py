"""
IngredientsOnline API Client for single-product refresh.

Provides authentication and data fetching from IO's GraphQL API.
Extracts key functions from IO_scraper.py for reuse in product_updater.py.
"""

import os
import time
import random
import requests
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass


# API Configuration
GRAPHQL_URL = "https://pwaktx64p8stvio.ingredientsonline.com/graphql"
MAX_RETRIES = 3
RETRY_DELAY = 2
MAX_RETRY_DELAY = 16


@dataclass
class IOProductData:
    """Structured product data from IO API."""
    sku: str
    name: str
    price_tiers: List[Dict]  # [{min_qty: 0, price: 50.0}, {min_qty: 25, price: 45.0}, ...]
    inventory: Dict[str, float]  # {warehouse: quantity_kg}


class IOClient:
    """
    Client for IngredientsOnline GraphQL API.

    Handles authentication and product/inventory fetching.
    """

    def __init__(self):
        self.token: Optional[str] = None
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })

    def authenticate(self) -> Tuple[bool, str]:
        """
        Authenticate with IO API using credentials from environment.

        Returns:
            (success: bool, error_message: str)
        """
        email = os.getenv('IO_EMAIL')
        password = os.getenv('IO_PASSWORD')

        if not email or not password:
            return False, "IO credentials not configured (IO_EMAIL/IO_PASSWORD)"

        query = '''
        mutation {
          generateCustomerToken(email: "%s", password: "%s") {
            token
          }
        }
        ''' % (email, password)

        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.post(
                    GRAPHQL_URL,
                    json={'query': query},
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()

                if 'errors' in data:
                    error_msg = data['errors'][0].get('message', 'Unknown auth error')
                    return False, f"IO auth failed: {error_msg}"

                self.token = data['data']['generateCustomerToken']['token']
                self.session.headers['Authorization'] = f'Bearer {self.token}'
                return True, ""

            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    delay = min(RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                    delay = delay * (0.5 + random.random())
                    time.sleep(delay)
                else:
                    return False, f"IO auth network error: {str(e)}"

        return False, "IO auth failed after retries"

    def _graphql_request(self, query: str, variables: Dict = None) -> Tuple[Optional[Dict], str]:
        """
        Make authenticated GraphQL request.

        Returns:
            (data: dict or None, error_message: str)
        """
        if not self.token:
            return None, "Not authenticated"

        payload = {'query': query}
        if variables:
            payload['variables'] = variables

        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.post(
                    GRAPHQL_URL,
                    json=payload,
                    timeout=60
                )

                if response.status_code in (401, 403):
                    return None, "IO token expired or invalid"

                response.raise_for_status()
                data = response.json()

                if 'errors' in data:
                    error_msg = data['errors'][0].get('message', 'GraphQL error')
                    # Check for auth errors
                    if 'not authorized' in error_msg.lower() or 'token' in error_msg.lower():
                        return None, f"IO auth error: {error_msg}"
                    # Non-auth error, still return data if present
                    if 'data' in data:
                        return data, ""
                    return None, error_msg

                return data, ""

            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    delay = min(RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
                    delay = delay * (0.5 + random.random())
                    time.sleep(delay)
                else:
                    return None, f"IO API error: {str(e)}"

        return None, "IO API request failed after retries"

    def fetch_product_by_sku(self, parent_sku: str) -> Tuple[Optional[Dict], str]:
        """
        Fetch product with pricing data by parent SKU.

        Args:
            parent_sku: The parent product SKU (e.g., "59410-SUNNYCARE-11455")

        Returns:
            (product_data: dict or None, error_message: str)
        """
        # Query for product by SKU - includes variants with price tiers
        query = '''
        {
          products(filter: {sku: {eq: "%s"}}, pageSize: 1) {
            items {
              __typename
              name
              sku
              url_key
              url_rewrites {
                url
              }
              price_range {
                minimum_price {
                  regular_price { value currency }
                  final_price { value currency }
                  discount { percent_off amount_off }
                }
              }
              ... on ConfigurableProduct {
                variants {
                  product {
                    sku
                    name
                    price_range {
                      minimum_price {
                        regular_price { value currency }
                        final_price { value currency }
                        discount { percent_off amount_off }
                      }
                    }
                    price_tiers {
                      quantity
                      final_price { value currency }
                      discount { percent_off }
                    }
                  }
                  attributes {
                    code
                    label
                  }
                }
              }
              ... on SimpleProduct {
                price_range {
                  minimum_price {
                    regular_price { value currency }
                    final_price { value currency }
                    discount { percent_off amount_off }
                  }
                }
                price_tiers {
                  quantity
                  final_price { value currency }
                  discount { percent_off }
                }
              }
            }
            total_count
          }
        }
        ''' % parent_sku

        data, error = self._graphql_request(query)
        if error:
            return None, error

        items = data.get('data', {}).get('products', {}).get('items', [])
        if not items:
            return None, f"Product not found: {parent_sku}"

        return items[0], ""

    def fetch_inventory(self, parent_sku: str) -> Tuple[List[Dict], str]:
        """
        Fetch inventory data for a product.

        Args:
            parent_sku: The parent product SKU

        Returns:
            (inventory_list: list of dicts, error_message: str)
            Each dict has: {sku, source_code, source_name, quantity}
        """
        query = """
        query getInventory($sku: String) {
          inventory(sku: $sku) {
            inventorydetail {
              backorder
              leadtime
              next_stocking
              quantity
              sku
              source_code
              source_name
            }
          }
        }
        """

        data, error = self._graphql_request(query, variables={'sku': parent_sku})
        if error:
            return [], error

        inventory = data.get('data', {}).get('inventory')
        if inventory is None:
            return [], "No inventory data available"

        details = inventory.get('inventorydetail', [])
        return details, ""

    def fetch_product_with_inventory(self, parent_sku: str) -> Tuple[Optional[Dict], str]:
        """
        Fetch both product pricing and inventory data.

        Returns combined data structure:
        {
            'product': {...},
            'inventory': [{sku, warehouse, quantity}, ...]
        }
        """
        # Fetch product data
        product, error = self.fetch_product_by_sku(parent_sku)
        if error:
            return None, error

        # Fetch inventory
        inventory, inv_error = self.fetch_inventory(parent_sku)
        # Don't fail if inventory fetch fails, just return empty
        if inv_error:
            inventory = []

        return {
            'product': product,
            'inventory': inventory
        }, ""


def extract_variant_prices(product_data: Dict, variant_sku: str) -> List[Dict]:
    """
    Extract price tiers for a specific variant SKU from product data.

    Returns:
        List of {min_quantity: float, price: float, price_per_kg: float}
    """
    tiers = []

    # Check if this is a configurable product with variants
    if product_data.get('__typename') == 'ConfigurableProduct':
        variants = product_data.get('variants', [])
        for variant in variants:
            var_product = variant.get('product', {})
            if var_product.get('sku') == variant_sku:
                # Extract volume tiers from API (these have actual min quantities like 25, 100, 500 kg)
                price_tiers = var_product.get('price_tiers', [])
                for tier in price_tiers:
                    qty = tier.get('quantity', 0)
                    price = tier.get('final_price', {}).get('value', 0)
                    if price > 0:
                        tiers.append({
                            'min_quantity': qty,
                            'price': price,
                            'price_per_kg': price
                        })

                # Fallback: if no volume tiers, use base price with min_quantity=25 (typical IO minimum)
                if not tiers:
                    price_range = var_product.get('price_range', {})
                    min_price = price_range.get('minimum_price', {})
                    base_price = min_price.get('final_price', {}).get('value', 0)
                    if base_price > 0:
                        tiers.append({
                            'min_quantity': 25,  # IO minimum order is typically 25kg
                            'price': base_price,
                            'price_per_kg': base_price
                        })
                break

    # Handle SimpleProduct
    elif product_data.get('__typename') == 'SimpleProduct':
        if product_data.get('sku') == variant_sku:
            # Extract volume tiers from API
            price_tiers = product_data.get('price_tiers', [])
            for tier in price_tiers:
                qty = tier.get('quantity', 0)
                price = tier.get('final_price', {}).get('value', 0)
                if price > 0:
                    tiers.append({
                        'min_quantity': qty,
                        'price': price,
                        'price_per_kg': price
                    })

            # Fallback: if no volume tiers, use base price with min_quantity=25
            if not tiers:
                price_range = product_data.get('price_range', {})
                min_price = price_range.get('minimum_price', {})
                base_price = min_price.get('final_price', {}).get('value', 0)
                if base_price > 0:
                    tiers.append({
                        'min_quantity': 25,  # IO minimum order is typically 25kg
                        'price': base_price,
                        'price_per_kg': base_price
                    })

    # Sort by min_quantity
    tiers.sort(key=lambda x: x['min_quantity'])
    return tiers


def extract_variant_inventory(inventory_data: List[Dict], variant_sku: str) -> Dict[str, float]:
    """
    Extract warehouse inventory for a specific variant SKU.

    Returns:
        Dict mapping warehouse name to quantity in kg.
        Example: {'chino': 125.0, 'edison': 50.0}
    """
    inventory = {}

    for item in inventory_data:
        if item.get('sku') == variant_sku:
            # Use source_name if available, otherwise source_code
            warehouse = item.get('source_name') or item.get('source_code') or 'unknown'
            warehouse = warehouse.lower()
            quantity = float(item.get('quantity', 0))

            # Keep highest quantity if multiple entries for same warehouse
            if warehouse not in inventory or quantity > inventory[warehouse]:
                inventory[warehouse] = quantity

    return inventory


def get_product_url(product_data: Dict) -> Optional[str]:
    """
    Build canonical product URL from GraphQL response.

    Returns:
        Full product URL or None if URL cannot be determined.
    """
    base_url = "https://www.ingredientsonline.com"

    # Try url_rewrites first (most reliable)
    url_rewrites = product_data.get('url_rewrites', [])
    if url_rewrites and len(url_rewrites) > 0:
        url_path = url_rewrites[0].get('url', '')
        if url_path:
            if not url_path.startswith('/'):
                url_path = '/' + url_path
            if not url_path.endswith('/'):
                url_path = url_path + '/'
            return f"{base_url}{url_path}"

    # Fallback to url_key
    url_key = product_data.get('url_key', '')
    if url_key:
        return f"{base_url}/{url_key}/"

    return None


def get_all_variant_skus(product_data: Dict) -> List[str]:
    """
    Get all variant SKUs from a product.

    Returns:
        List of variant SKU strings.
    """
    skus = []

    if product_data.get('__typename') == 'ConfigurableProduct':
        variants = product_data.get('variants', [])
        for variant in variants:
            var_product = variant.get('product', {})
            sku = var_product.get('sku')
            if sku:
                skus.append(sku)
    elif product_data.get('__typename') == 'SimpleProduct':
        sku = product_data.get('sku')
        if sku:
            skus.append(sku)

    return skus


# =============================================================================
# LAZY PLAYWRIGHT FALLBACK
# Only initialized if API fails - NOT loaded at module import time
# =============================================================================

# Module-level state for Playwright (lazy initialized)
_pw_browser = None
_pw_page = None
_pw_context = None
_pw_authenticated = False


def _init_playwright_lazy() -> bool:
    """
    Lazy-initialize Playwright browser for fallback inventory scraping.
    Only called when API fails. Uses headed browser (headless=False) to avoid bot detection.

    Returns True if authentication successful.
    """
    global _pw_browser, _pw_page, _pw_context, _pw_authenticated

    # Already initialized?
    if _pw_authenticated and _pw_page:
        try:
            _pw_page.url  # Check if browser still alive
            return True
        except:
            _pw_authenticated = False

    email = os.environ.get('IO_EMAIL')
    password = os.environ.get('IO_PASSWORD')

    if not email or not password:
        print("  [Playwright] No credentials available", flush=True)
        return False

    try:
        # Lazy import - only load Playwright when needed
        from playwright.sync_api import sync_playwright
        import time as _time

        print("  [Playwright] Initializing browser for fallback...", flush=True)

        playwright = sync_playwright().start()

        # Headed browser to avoid bot detection
        _pw_browser = playwright.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox'
            ]
        )

        _pw_context = _pw_browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        )

        _pw_page = _pw_context.new_page()
        _pw_page.set_default_timeout(60000)

        # Navigate to login page
        login_url = "https://www.ingredientsonline.com/customer/account/login"
        _pw_page.goto(login_url + "/", wait_until="domcontentloaded", timeout=60000)

        # Fill login form
        try:
            email_input = _pw_page.get_by_label("Email", exact=False)
            email_input.fill(email)
        except:
            _pw_page.locator('input[name="login[username]"]').fill(email)

        try:
            password_input = _pw_page.get_by_label("Password", exact=False)
            password_input.fill(password)
        except:
            _pw_page.locator('input[name="login[password]"]').fill(password)

        # Click login button
        for selector in ['button[type="submit"]', 'button:has-text("Sign In")', '.action.login.primary']:
            try:
                loc = _pw_page.locator(selector).first
                if loc.is_visible():
                    loc.click()
                    break
            except:
                continue

        _pw_page.wait_for_load_state('domcontentloaded')
        _time.sleep(2)

        # Verify login by checking for prices
        _pw_page.goto("https://www.ingredientsonline.com/products/?in_stock[filter]=1,1&size=10",
                     wait_until='domcontentloaded', timeout=60000)

        content = _pw_page.content()
        if '$' in content and 'log in to see pricing' not in content.lower():
            _pw_authenticated = True
            print("  [Playwright] Authenticated successfully", flush=True)
            return True
        else:
            print("  [Playwright] Authentication failed", flush=True)
            return False

    except Exception as e:
        print(f"  [Playwright] Init error: {e}", flush=True)
        return False


def fetch_inventory_playwright_fallback(product_url: str) -> Tuple[List[Dict], str]:
    """
    Fallback: Scrape inventory data from product page HTML using Playwright.
    Only called when API fails. Lazy-initializes browser on first call.

    Args:
        product_url: Full URL to the product page

    Returns:
        Tuple of (inventory_list, error_message)
        inventory_list contains dicts with 'sku', 'source_code', 'quantity'
    """
    import re
    import time as _time

    global _pw_page, _pw_authenticated

    # Lazy initialize browser if needed
    if not _pw_authenticated:
        if not _init_playwright_lazy():
            return [], "Playwright initialization failed"

    if not _pw_page:
        return [], "Playwright page not available"

    try:
        print(f"  [Playwright] Fetching inventory from {product_url}", flush=True)

        _pw_page.goto(product_url, timeout=30000, wait_until='domcontentloaded')

        # Wait for inventory table
        try:
            _pw_page.wait_for_selector('.inventory-table', timeout=10000)
        except:
            try:
                _pw_page.wait_for_selector('text=WAREHOUSE', timeout=5000)
            except:
                _time.sleep(3)

        content = _pw_page.content()
        inventory_list = []

        # Parse warehouse data from HTML
        warehouse_patterns = [
            (r'Chino,?\s*CA', 'chino'),
            (r'Edison,?\s*NJ', 'nj'),
            (r'Southwest', 'sw'),
        ]

        for pattern, source_code in warehouse_patterns:
            # Look for location followed by quantity in table cells
            match = re.search(
                rf'{pattern}.*?</(?:span|label|td)>.*?(?:class="table-item"[^>]*>|<td[^>]*>)\s*(\d+)\s*</td>',
                content, re.IGNORECASE | re.DOTALL
            )
            if match:
                qty = int(match.group(1)) if match.group(1) else 0
                if qty > 0:
                    inventory_list.append({
                        'sku': '',  # Will be filled in by caller
                        'source_code': source_code,
                        'source_name': source_code,
                        'quantity': qty
                    })

        if inventory_list:
            print(f"  [Playwright] Found {len(inventory_list)} warehouse entries", flush=True)
        else:
            print("  [Playwright] No inventory data found in HTML", flush=True)

        return inventory_list, ""

    except Exception as e:
        error_msg = f"Playwright scrape error: {e}"
        print(f"  [Playwright] {error_msg}", flush=True)
        return [], error_msg


def cleanup_playwright():
    """Clean up Playwright browser resources."""
    global _pw_browser, _pw_page, _pw_context, _pw_authenticated

    try:
        if _pw_page:
            _pw_page.close()
        if _pw_context:
            _pw_context.close()
        if _pw_browser:
            _pw_browser.close()
    except:
        pass

    _pw_browser = None
    _pw_page = None
    _pw_context = None
    _pw_authenticated = False
