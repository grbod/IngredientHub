#!/usr/bin/env python3
"""
IngredientsOnline.com Pricing Scraper

Automatically discovers and scrapes pricing/inventory data for all in-stock products.
Credentials are read from environment variables IO_EMAIL and IO_PASSWORD.
Output is saved to a timestamped CSV file with checkpoint support for long runs.
"""

import os
import sys
import json
import time
import re
import argparse
import random
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import pandas as pd
import requests
from playwright.sync_api import sync_playwright, Page, Browser
from bs4 import BeautifulSoup


# =============================================================================
# Configuration
# =============================================================================

GRAPHQL_URL = "https://pwaktx64p8stvio.ingredientsonline.com/graphql"
LOGIN_URL = "https://www.ingredientsonline.com/login"
URLS_FILE = "urls.txt"
PAGE_TIMEOUT = 30000  # 30 seconds (using domcontentloaded, not networkidle)

# URL Discovery Configuration
PRODUCTS_BASE_URL = "https://www.ingredientsonline.com/products/"
DISCOVERY_DELAY = 0.5  # Rate limiting between discovery page requests

# Retry configuration (exponential backoff)
MAX_RETRIES = 10
INITIAL_RETRY_DELAY = 2  # Start with 2 seconds
MAX_RETRY_DELAY = 120    # Cap at 2 minutes

# Checkpoint configuration
CHECKPOINT_INTERVAL = 100  # Save progress every N products
CHECKPOINT_FILE = ".scrape_checkpoint.json"

# Default discovery settings
DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_PAGES = None  # None = all pages


def load_env_file():
    """Load environment variables from .env file if it exists."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()


def get_credentials() -> tuple[str, str]:
    """Get credentials from .env file or environment variables."""
    # Load .env file first
    load_env_file()

    email = os.environ.get("IO_EMAIL")
    password = os.environ.get("IO_PASSWORD")

    if not email or not password:
        print("Error: Missing credentials.")
        print("Please create a .env file with:")
        print("  IO_EMAIL=your-email@example.com")
        print("  IO_PASSWORD=your-password")
        print("Or set environment variables.")
        sys.exit(1)

    return email, password


# =============================================================================
# Retry Logic with Exponential Backoff
# =============================================================================

def retry_with_backoff(func, *args, max_retries=MAX_RETRIES, initial_delay=INITIAL_RETRY_DELAY,
                       max_delay=MAX_RETRY_DELAY, **kwargs):
    """
    Retry a function with exponential backoff.
    Delays: 2s, 4s, 8s, 16s, 32s, 64s, 120s, 120s, 120s, 120s
    """
    last_exception = None
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            if attempt == max_retries - 1:
                print(f"  All {max_retries} attempts failed. Last error: {e}")
                raise
            delay = min(initial_delay * (2 ** attempt), max_delay)
            print(f"  Attempt {attempt + 1}/{max_retries} failed: {e}")
            print(f"  Retrying in {delay}s...")
            time.sleep(delay)
    raise last_exception


# =============================================================================
# Interactive Prompts for Testing
# =============================================================================

def prompt_discovery_settings() -> Tuple[int, Optional[int]]:
    """
    Prompt user for discovery settings (page size and max pages).
    Returns (page_size, max_pages) where max_pages=None means all pages.
    """
    print("\n" + "=" * 50)
    print("Discovery Settings")
    print("=" * 50)
    print("Configure how many products to discover.")
    print("Press Enter to use defaults (all products).\n")

    # Page size
    while True:
        try:
            size_input = input(f"Products per page (size) [{DEFAULT_PAGE_SIZE}]: ").strip()
            if not size_input:
                page_size = DEFAULT_PAGE_SIZE
            else:
                page_size = int(size_input)
                if page_size < 1 or page_size > 100:
                    print("  Please enter a number between 1 and 100")
                    continue
            break
        except ValueError:
            print("  Please enter a valid number")

    # Max pages
    while True:
        try:
            pages_input = input("Max pages to scrape [all]: ").strip()
            if not pages_input:
                max_pages = None
            else:
                max_pages = int(pages_input)
                if max_pages < 1:
                    print("  Please enter a positive number")
                    continue
            break
        except ValueError:
            print("  Please enter a valid number")

    # Summary
    if max_pages:
        print(f"\n→ Will discover up to {page_size * max_pages} products ({page_size}/page × {max_pages} pages)")
    else:
        print(f"\n→ Will discover ALL products ({page_size}/page)")

    return page_size, max_pages


# =============================================================================
# URL Discovery Functions
# =============================================================================

def extract_total_count(soup: BeautifulSoup) -> int:
    """
    Extract total product count from 'Showing X products out of Y' text.
    """
    text = soup.get_text()

    # Look for "Showing X products out of Y" pattern
    match = re.search(r'Showing\s+\d+\s+products?\s+out\s+of\s+(\d+)', text, re.IGNORECASE)
    if match:
        return int(match.group(1))

    # Fallback patterns
    match = re.search(r'(\d+)\s+products?\s+found', text, re.IGNORECASE)
    if match:
        return int(match.group(1))

    return 0


def parse_listing_page(html: str) -> Tuple[List[str], int]:
    """
    Parse products listing HTML to extract product URLs.
    Returns tuple of (list of product URLs, total product count).
    Product URLs have format: /<category>/<product-slug>/
    """
    soup = BeautifulSoup(html, 'html.parser')

    # Extract total product count
    total_count = extract_total_count(soup)

    # Find all product links
    product_urls = set()
    base_url = "https://www.ingredientsonline.com"

    # Non-product paths to skip
    skip_patterns = [
        '/account', '/login', '/cart', '/rma', '/products', '/checkout',
        '/wishlist', '/compare', '/search', '/customer', '/contact',
        '/about', '/faq', '/help', '/privacy', '/terms', '/returns',
        '/factory', '/brand', '/supplier', '/manufacturer', '/vendor'
    ]

    for link in soup.find_all('a', href=True):
        href = link['href']

        # Product URLs have format: /<category>/<product-slug>
        # Must start with / and have at least 2 path segments
        if not href.startswith('/'):
            continue

        # Clean the href
        clean_href = href.split('?')[0].rstrip('/')

        # Count path segments (e.g., /botanicals/acai-powder has 2)
        segments = [s for s in clean_href.split('/') if s]
        if len(segments) < 2:
            continue

        # Skip known non-product paths
        if any(skip in href.lower() for skip in skip_patterns):
            continue

        # Skip if it looks like a category page (single segment or ends with /)
        # Valid product URLs have category + product-slug
        full_url = base_url + clean_href
        product_urls.add(full_url)

    return list(product_urls), total_count


def fetch_listing_page_playwright(page: Page, page_num: int, page_size: int = 100) -> Tuple[List[str], int]:
    """
    Fetch a single products listing page using Playwright (for JS-rendered content).
    Returns tuple of (list of product URLs, total product count).
    """
    params = f"?in_stock[filter]=1,1&sort=name_asc&page={page_num}&size={page_size}"
    url = PRODUCTS_BASE_URL + params

    page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)

    # Wait for products to load - look for product links or "Showing X products" text
    try:
        page.wait_for_selector('a[href*="/botanicals/"], a[href*="/amino-acids/"], a[href*="/food-additives/"], a[href*="/ingredients/"]', timeout=10000)
    except:
        # Fallback: wait for "Showing" text which indicates products loaded
        try:
            page.wait_for_selector('text=/Showing.*products/', timeout=5000)
        except:
            pass

    # Extra buffer for remaining JS
    time.sleep(1)

    # Debug: check current URL for redirects
    current_url = page.url
    if 'login' in current_url.lower():
        print(f"    WARNING: Redirected to login page - session may have expired")

    # Get page content after JS execution
    html = page.content()

    # Parse the page
    urls, total = parse_listing_page(html)

    # Debug output
    print(f"    DEBUG: Parsed {len(urls)} product URLs, total count: {total}")
    if urls[:3]:
        print(f"    DEBUG: Sample URLs: {urls[:3]}")

    # If no URLs found, check what we're seeing
    if not urls:
        soup = BeautifulSoup(html, 'html.parser')
        all_links = [a['href'] for a in soup.find_all('a', href=True)]
        print(f"    DEBUG: Page has {len(all_links)} total links")
        # Show some sample hrefs to debug
        sample = [h for h in all_links if '/' in h][:5]
        print(f"    DEBUG: Sample hrefs: {sample}")

    return urls, total


def discover_product_urls(browser_page: Page, page_size: int = 100, max_pages: Optional[int] = None) -> List[str]:
    """
    Discover all in-stock product URLs from the products listing page using Playwright.
    Returns list of unique product URLs.
    """
    all_urls = set()
    page_num = 1
    total_products = None
    consecutive_failures = 0
    max_consecutive_failures = 3

    print("\n" + "-" * 40)
    print("Discovering product URLs...")
    print("-" * 40)

    while True:
        # Apply max_pages limit
        if max_pages and page_num > max_pages:
            print(f"Reached max pages limit ({max_pages})")
            break

        try:
            print(f"  Fetching page {page_num}...")
            page_urls, total = fetch_listing_page_playwright(browser_page, page_num, page_size)

            # Reset failure counter on success
            consecutive_failures = 0

            if total_products is None and total > 0:
                total_products = total
                expected_pages = (total + page_size - 1) // page_size
                print(f"Found {total} total in-stock products across ~{expected_pages} pages")

            if not page_urls:
                print(f"No more products found on page {page_num}")
                break

            all_urls.update(page_urls)
            print(f"  Page {page_num}: Found {len(page_urls)} URLs (Total unique: {len(all_urls)})")

            # Check if we've likely collected all products
            if total_products and len(all_urls) >= total_products:
                break

            page_num += 1
            time.sleep(DISCOVERY_DELAY)

        except Exception as e:
            consecutive_failures += 1
            print(f"Error on page {page_num}: {e}")
            if consecutive_failures >= max_consecutive_failures:
                print(f"  {max_consecutive_failures} consecutive failures, stopping discovery")
                break
            print(f"  Retrying in 2s... (attempt {consecutive_failures}/{max_consecutive_failures})")
            time.sleep(2)
            continue

    urls_list = sorted(list(all_urls))
    print(f"\nDiscovered {len(urls_list)} unique product URLs")

    return urls_list


# =============================================================================
# Checkpoint Functions
# =============================================================================

def save_checkpoint(processed_urls: List[str], all_data: List[Dict],
                    all_urls: List[str], output_file: str = None) -> None:
    """
    Save scraping progress to checkpoint file.
    """
    checkpoint = {
        'processed_urls': processed_urls,
        'all_urls': all_urls,
        'data_count': len(all_data),
        'output_file': output_file,
        'timestamp': datetime.now().isoformat()
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)

    # Also save intermediate data
    if all_data and output_file:
        df = pd.DataFrame(all_data)
        df.to_csv(output_file, index=False)


def load_checkpoint() -> Optional[Dict]:
    """
    Load checkpoint if it exists.
    Returns checkpoint dict or None.
    """
    if not os.path.exists(CHECKPOINT_FILE):
        return None

    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
        return checkpoint
    except Exception as e:
        print(f"Error loading checkpoint: {e}")
        return None


def clear_checkpoint() -> None:
    """Remove checkpoint file after successful completion."""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("Cleared checkpoint file")


# =============================================================================
# URL Loading (for static file fallback)
# =============================================================================

def load_urls(filepath: str) -> List[str]:
    """Load URLs from file, one per line."""
    if not os.path.exists(filepath):
        print(f"Error: URLs file not found: {filepath}")
        sys.exit(1)

    with open(filepath, "r") as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    if not urls:
        print(f"Error: No URLs found in {filepath}")
        sys.exit(1)

    print(f"Loaded {len(urls)} URLs from {filepath}")
    return urls


def get_inventory(sku: str) -> List[Dict]:
    """
    Fetch inventory data from GraphQL API.
    Returns list of warehouse inventory details.
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

    try:
        response = requests.post(
            GRAPHQL_URL,
            json={"query": query, "variables": {"sku": sku}},
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()

        inventory_details = data.get("data", {}).get("inventory", {}).get("inventorydetail", [])
        return inventory_details
    except Exception as e:
        print(f"    Warning: Could not fetch inventory for {sku}: {e}")
        return []


def dismiss_cookie_popup(page: Page) -> None:
    """Dismiss HubSpot cookie banner if it appears (fallback)."""
    try:
        accept_btn = page.locator('#hs-eu-confirmation-button')
        if accept_btn.is_visible(timeout=2000):
            accept_btn.click()
            page.locator('#hs-eu-cookie-confirmation').wait_for(state='hidden', timeout=2000)
            print("Dismissed cookie popup")
    except:
        pass


def detect_and_wait_for_captcha(page: Page, max_wait: int = 120) -> bool:
    """
    Detect Cloudflare CAPTCHA and wait for user to solve it manually.
    Returns True if CAPTCHA was detected and solved, False if no CAPTCHA.
    """
    try:
        content = page.content().lower()

        # Check for Cloudflare challenge indicators
        captcha_indicators = [
            'verify you are human',
            'cloudflare',
            'security challenge',
            'checking your browser',
            'just a moment',
            'ray id',
        ]

        is_captcha = any(indicator in content for indicator in captcha_indicators)

        if is_captcha:
            print("\n" + "=" * 50)
            print("⚠️  CAPTCHA DETECTED - Please solve it manually in the browser")
            print("=" * 50)

            # Wait for CAPTCHA to be solved (page will change)
            start_time = time.time()
            while time.time() - start_time < max_wait:
                time.sleep(2)
                new_content = page.content().lower()

                # Check if CAPTCHA is gone
                still_captcha = any(indicator in new_content for indicator in captcha_indicators)
                if not still_captcha:
                    print("✓ CAPTCHA solved! Continuing...")
                    time.sleep(1)  # Brief pause after solving
                    return True

                elapsed = int(time.time() - start_time)
                if elapsed % 10 == 0:  # Print every 10 seconds
                    print(f"  Waiting for CAPTCHA solution... ({elapsed}s / {max_wait}s)")

            print("⚠️  CAPTCHA timeout - continuing anyway")
            return True

    except Exception as e:
        pass

    return False


def check_login_error(page: Page) -> bool:
    """Check if there's a login error popup/message on the page."""
    # Check page content for the specific error messages
    try:
        content = page.content().lower()
        error_phrases = [
            'account sign-in was incorrect',
            'account is disabled temporarily',
            'disabled temporarily',
            'try again later',
            'login error',
            'account may be disabled',
        ]
        for phrase in error_phrases:
            if phrase in content:
                return True
    except:
        pass

    # Check for error dialog/modal
    error_selectors = [
        'text="Error"',
        ':has-text("sign-in was incorrect")',
        ':has-text("disabled temporarily")',
        ':has-text("try again later")',
    ]
    for selector in error_selectors:
        try:
            if page.locator(selector).count() > 0:
                return True
        except:
            continue

    return False


def dismiss_error_dialog(page: Page) -> None:
    """Try to dismiss any error dialog by clicking X or close button."""
    close_selectors = [
        'button:has-text("×")',
        'button:has-text("X")',
        'button:has-text("Close")',
        '[aria-label="Close"]',
        '.close',
        '.modal-close',
        'button.close',
    ]
    for selector in close_selectors:
        try:
            if page.locator(selector).count() > 0:
                page.locator(selector).first.click()
                time.sleep(1)
                return
        except:
            continue

    # Try pressing Escape to close modal
    try:
        page.keyboard.press('Escape')
        time.sleep(1)
    except:
        pass


def login_attempt(page: Page, email: str, password: str) -> bool:
    """
    Single login attempt using reliable Playwright patterns.
    Returns True if successful, False otherwise.
    """
    print(f"Navigating to login page: {LOGIN_URL}")
    page.goto(LOGIN_URL + "/", wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)

    # Wait for page to fully load
    page.wait_for_load_state("domcontentloaded")
    time.sleep(2)

    # Verify we're on the login page by checking for key elements
    try:
        customer_login = page.locator('text=Customer Login')
        customer_login.wait_for(state="visible", timeout=10000)
        print("Found 'Customer Login' heading")
    except:
        print("Warning: Could not find 'Customer Login' heading")

    try:
        registered = page.locator('text=Registered Customers')
        registered.wait_for(state="visible", timeout=5000)
        print("Found 'Registered Customers' section")
    except:
        print("Warning: Could not find 'Registered Customers' section")

    time.sleep(1)

    # Fill email using getByLabel with fallbacks (click first, then fill)
    email_filled = False
    try:
        # Try getByLabel first
        email_input = page.get_by_label("Email", exact=False)
        if email_input.count() > 0:
            email_input.click()
            time.sleep(0.3)
            email_input.fill(email)
            email_filled = True
            print("Filled email using getByLabel")
    except:
        pass

    if not email_filled:
        # Fallback selectors
        email_selectors = [
            'input[placeholder*="email" i]',
            '.login-form input[type="email"]',
            'input[type="email"]',
            'input[id="email"]',
        ]
        for selector in email_selectors:
            try:
                loc = page.locator(selector)
                if loc.count() > 0:
                    loc.click()
                    time.sleep(0.3)
                    loc.fill(email)
                    email_filled = True
                    print(f"Filled email using selector: {selector}")
                    break
            except:
                continue

    if not email_filled:
        print("Error: Could not find email input field")
        return False

    time.sleep(0.5)

    # Fill password using getByLabel with fallbacks (click first, then fill)
    password_filled = False
    try:
        password_input = page.get_by_label("Password", exact=False)
        if password_input.count() > 0:
            password_input.click()
            time.sleep(0.3)
            password_input.fill(password)
            password_filled = True
            print("Filled password using getByLabel")
    except:
        pass

    if not password_filled:
        password_selectors = [
            'input[placeholder*="password" i]',
            '.login-form input[type="password"]',
            'input[type="password"]',
        ]
        for selector in password_selectors:
            try:
                loc = page.locator(selector)
                if loc.count() > 0:
                    loc.click()
                    time.sleep(0.3)
                    loc.fill(password)
                    password_filled = True
                    print(f"Filled password using selector: {selector}")
                    break
            except:
                continue

    if not password_filled:
        print("Error: Could not find password input field")
        return False

    time.sleep(0.5)

    # Click submit button
    submit_selectors = [
        'button[type="submit"]',
        'button:has-text("Sign In")',
        'button:has-text("Login")',
        'input[type="submit"]',
    ]

    submitted = False
    for selector in submit_selectors:
        try:
            loc = page.locator(selector)
            if loc.count() > 0:
                loc.click()
                submitted = True
                print(f"Clicked submit using selector: {selector}")
                break
        except:
            continue

    if not submitted:
        print("Error: Could not find submit button")
        return False

    # Wait for navigation/login to complete
    print("Waiting for login to complete...")
    time.sleep(3)
    try:
        page.wait_for_load_state("domcontentloaded", timeout=15000)
    except:
        pass

    # Check for login error popup
    if check_login_error(page):
        print("Login error detected (account temporarily disabled)")
        dismiss_error_dialog(page)
        return False

    # Dismiss cookie popup if present
    dismiss_cookie_popup(page)

    # Check URL - but don't trust it completely
    current_url = page.url
    print(f"Current URL after login: {current_url}")

    # Check for error one more time
    if check_login_error(page):
        return False

    # Don't claim success here - let verify_login_on_catalog confirm
    return True


def verify_login_on_catalog(page: Page) -> bool:
    """
    Verify login is working by checking the catalog page.
    If we see 'Log in to see pricing' it means we're not logged in.
    """
    try:
        # Navigate to catalog to check
        page.goto(PRODUCTS_BASE_URL + "?in_stock[filter]=1,1&size=10", wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
        time.sleep(3)

        content = page.content().lower()
        if 'log in to see pricing' in content or 'login to see pricing' in content:
            print("❌ Not logged in - seeing 'Log in to see pricing' on catalog")
            return False

        # Also check if we can see actual prices (numbers with $)
        if '$' in content:
            print("✓ Login verified - can see pricing on catalog")
            return True

        return True  # Assume OK if no obvious login prompts
    except Exception as e:
        print(f"Error verifying login: {e}")
        return True  # Proceed anyway


def login(page: Page, email: str, password: str, max_attempts: int = 5) -> bool:
    """
    Log into ingredientsonline.com with retry logic.
    Uses longer delays since failed attempts can temporarily disable account.
    Returns True if login successful, False otherwise.
    """
    # Delays: 30s, 60s, 90s, 120s (account gets temporarily disabled on failures)
    retry_delays = [30, 60, 90, 120]

    for attempt in range(max_attempts):
        if attempt > 0:
            delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
            print(f"\n⏳ Account temporarily disabled. Waiting {delay}s before retry...")
            print(f"   (Attempt {attempt + 1}/{max_attempts})")
            time.sleep(delay)

        if login_attempt(page, email, password):
            # Verify login actually worked by checking catalog
            if verify_login_on_catalog(page):
                return True
            else:
                print("Login appeared successful but catalog shows not logged in")
                continue

        print(f"Login attempt {attempt + 1} failed")

    print(f"All {max_attempts} login attempts failed")
    return False


def extract_next_data(page: Page) -> Optional[Dict]:
    """
    Extract the __NEXT_DATA__ JSON from the page.
    This contains all the server-side rendered data including pricing.
    """
    try:
        # Get the page content
        content = page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # Find the __NEXT_DATA__ script tag
        next_data_script = soup.find('script', id='__NEXT_DATA__')

        if not next_data_script:
            print("Warning: __NEXT_DATA__ script tag not found")
            return None

        # Parse the JSON
        data = json.loads(next_data_script.string)
        return data

    except json.JSONDecodeError as e:
        print(f"Error parsing __NEXT_DATA__ JSON: {e}")
        return None
    except Exception as e:
        print(f"Error extracting __NEXT_DATA__: {e}")
        return None


def parse_pricing(data: Dict, url: str) -> List[Dict]:
    """
    Parse pricing tiers from __NEXT_DATA__ and fetch inventory.
    Returns a list of dictionaries, one per price tier.
    """
    rows = []
    timestamp = datetime.now().isoformat()

    try:
        # Navigate to the SEO data containing product info
        seo_data = data.get('props', {}).get('pageProps', {}).get('seo', {})

        if not seo_data:
            print(f"Warning: No SEO data found for {url}")
            return rows

        product_name = seo_data.get('name', 'Unknown')
        product_sku = seo_data.get('sku', 'Unknown')  # Magento-style SKU for inventory API

        # Fetch inventory data using the product SKU
        print(f"    Fetching inventory for SKU: {product_sku}")
        inventory_data = get_inventory(product_sku)

        # Build inventory summary by warehouse
        inventory_by_warehouse = {}
        for inv in inventory_data:
            source = inv.get('source_name') or inv.get('source_code') or 'Unknown'
            if not source:
                continue
            qty = inv.get('quantity', 0)
            leadtime = inv.get('leadtime', '')
            next_stock = inv.get('next_stocking', '')
            try:
                qty_int = int(qty) if qty else 0
            except:
                qty_int = 0
            if qty_int > 0 or leadtime or next_stock:
                inventory_by_warehouse[source] = {
                    'quantity': qty,
                    'leadtime_weeks': leadtime,
                    'next_stocking': next_stock
                }

        if inventory_by_warehouse:
            print(f"    Found inventory at: {', '.join(str(k) for k in inventory_by_warehouse.keys())}")

        # Extract variants and their price tiers
        variants = seo_data.get('variants', [])

        if not variants:
            print(f"Warning: No variants found for {product_name}")
            return rows

        for variant in variants:
            product_data = variant.get('product', {})
            variant_sku = product_data.get('sku', 'Unknown')
            variant_name = product_data.get('name', product_name)

            price_tiers = product_data.get('price_tiers', [])

            # Base row data including inventory
            base_row = {
                'product_name': product_name,
                'product_sku': product_sku,
                'variant_sku': variant_sku,
                'variant_name': variant_name,
                'url': url,
                'scraped_at': timestamp
            }

            # Add inventory columns
            for warehouse, inv_info in inventory_by_warehouse.items():
                safe_name = warehouse.replace(' ', '_').replace(',', '')
                base_row[f'inv_{safe_name}_qty'] = inv_info['quantity']
                base_row[f'inv_{safe_name}_leadtime'] = inv_info['leadtime_weeks']
                base_row[f'inv_{safe_name}_eta'] = inv_info['next_stocking']

            if not price_tiers:
                # Try to get base price if no tiers
                price_range = seo_data.get('price_range', {})
                min_price = price_range.get('minimum_price', {}).get('final_price', {})
                if min_price:
                    row = base_row.copy()
                    row.update({
                        'tier_quantity': 1,
                        'price': min_price.get('value', 0),
                        'currency': min_price.get('currency', 'USD'),
                        'discount_percent': 0,
                    })
                    rows.append(row)
                continue

            for tier in price_tiers:
                tier_quantity = tier.get('quantity', 0)
                final_price = tier.get('final_price', {})
                discount = tier.get('discount', {})

                row = base_row.copy()
                row.update({
                    'tier_quantity': tier_quantity,
                    'price': final_price.get('value', 0),
                    'currency': final_price.get('currency', 'USD'),
                    'discount_percent': discount.get('percent_off', 0),
                })
                rows.append(row)

        print(f"  Extracted {len(rows)} price tiers for {product_name}")
        return rows

    except Exception as e:
        print(f"Error parsing pricing data: {e}")
        return rows


def scrape_product(page: Page, url: str) -> List[Dict]:
    """
    Scrape a single product page with exponential backoff retry.
    Returns list of pricing tier dictionaries.
    """
    for attempt in range(MAX_RETRIES):
        try:
            print(f"  Navigating to: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)

            # Wait for page to stabilize and JS to execute
            time.sleep(2)

            # Check for CAPTCHA and wait for manual solution if needed
            if detect_and_wait_for_captcha(page):
                # After CAPTCHA, reload the page
                page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
                time.sleep(2)

            # Dismiss cookie popup if it appears
            dismiss_cookie_popup(page)

            time.sleep(1)

            # Extract and parse data
            data = extract_next_data(page)

            if data:
                return parse_pricing(data, url)
            else:
                # Check if it's a CAPTCHA page (no data because blocked)
                if detect_and_wait_for_captcha(page):
                    continue  # Retry after CAPTCHA solved
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES}: No data extracted")

        except Exception as e:
            print(f"  Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")

        # Exponential backoff before retry
        if attempt < MAX_RETRIES - 1:
            delay = min(INITIAL_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
            print(f"  Retrying in {delay}s...")
            time.sleep(delay)

    print(f"  Failed to scrape {url} after {MAX_RETRIES} attempts")
    return []


def save_to_csv(data: List[Dict], output_dir: str = ".") -> str:
    """
    Save scraped data to a timestamped CSV file.
    Returns the filepath of the created file.
    """
    if not data:
        print("No data to save")
        return ""

    # Create DataFrame
    df = pd.DataFrame(data)

    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"pricing_data_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    # Save to CSV
    df.to_csv(filepath, index=False)
    print(f"\nSaved {len(data)} rows to: {filepath}")

    return filepath


def main():
    """Main entry point for the scraper."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='IngredientsOnline.com Pricing Scraper - Discovers and scrapes all in-stock products'
    )
    parser.add_argument('--resume', action='store_true',
                        help='Resume from checkpoint if available')
    parser.add_argument('--urls-file', type=str, default=None,
                        help='Use static URLs file instead of discovery (e.g., urls.txt)')
    args = parser.parse_args()

    print("=" * 60)
    print("IngredientsOnline.com Pricing Scraper")
    print("=" * 60)

    # Get credentials
    email, password = get_credentials()

    # Check for existing checkpoint
    checkpoint = load_checkpoint()
    processed_urls = []
    all_data = []
    urls = []

    if checkpoint and args.resume:
        print(f"\nFound checkpoint from {checkpoint['timestamp']}")
        print(f"  Processed: {len(checkpoint['processed_urls'])} products")
        print(f"  Remaining: {len(checkpoint['all_urls']) - len(checkpoint['processed_urls'])} products")

        resume = input("\nResume from checkpoint? [Y/n]: ").strip().lower()
        if resume != 'n':
            processed_urls = checkpoint['processed_urls']
            urls = checkpoint['all_urls']
            # Load existing data if output file exists
            if checkpoint.get('output_file') and os.path.exists(checkpoint['output_file']):
                df = pd.read_csv(checkpoint['output_file'])
                all_data = df.to_dict('records')
                print(f"  Loaded {len(all_data)} existing data rows")
        else:
            clear_checkpoint()
            checkpoint = None

    elif checkpoint and not args.resume:
        print(f"\nNote: Checkpoint exists from {checkpoint['timestamp']}")
        print("  Use --resume to continue, or it will start fresh")
        clear = input("Clear checkpoint and start fresh? [y/N]: ").strip().lower()
        if clear == 'y':
            clear_checkpoint()
        checkpoint = None

    # Get discovery settings if not resuming and not using static file
    page_size = DEFAULT_PAGE_SIZE
    max_pages = DEFAULT_MAX_PAGES
    use_discovery = False

    if not urls:  # Not resuming with existing URLs
        if args.urls_file:
            # Static file mode
            urls = load_urls(args.urls_file)
        else:
            # Discovery mode (default) - will run after browser launch
            use_discovery = True
            page_size, max_pages = prompt_discovery_settings()

    # Generate output filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    output_file = f"pricing_data_{timestamp}.csv"
    if checkpoint and checkpoint.get('output_file'):
        output_file = checkpoint['output_file']

    # Start Playwright browser
    with sync_playwright() as p:
        # Launch browser in headed mode with stealth options to avoid bot detection
        print("\nLaunching browser (headed mode with stealth options)...")
        browser = p.chromium.launch(
            headless=False,
            slow_mo=100,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-infobars',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
            ]
        )

        # Create a new context with realistic settings
        context = browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='America/New_York',
        )

        # Pre-set HubSpot consent cookie to skip the cookie banner entirely
        context.add_cookies([
            {
                "name": "__hs_notify_banner_dismiss",
                "value": "true",
                "domain": ".ingredientsonline.com",
                "path": "/"
            }
        ])
        print("Pre-set cookie consent to skip banner")

        page = context.new_page()

        # Inject JavaScript to hide automation markers (stealth)
        page.add_init_script("""
            // Remove webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });

            // Override plugins to look like a real browser
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });

            // Override languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });

            // Hide automation chrome
            window.chrome = {
                runtime: {}
            };

            // Override permissions query
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
        """)

        # Login first
        print("\n" + "-" * 40)
        print("Step 1: Logging in...")
        print("-" * 40)

        if not login(page, email, password):
            print("Login failed. Exiting.")
            browser.close()
            sys.exit(1)

        # Discover URLs if in discovery mode (after login)
        if use_discovery:
            urls = discover_product_urls(page, page_size, max_pages)

            if not urls:
                print("No product URLs discovered. Exiting.")
                browser.close()
                sys.exit(1)

        # Filter out already processed URLs if resuming
        if processed_urls:
            remaining_urls = [u for u in urls if u not in processed_urls]
            print(f"\nResuming: {len(remaining_urls)} products remaining")
        else:
            remaining_urls = urls

        # Scrape each URL
        print("\n" + "-" * 40)
        print("Step 2: Scraping product pages...")
        print("-" * 40)

        total_urls = len(urls)
        failed_urls = []
        start_offset = len(processed_urls)  # Fixed at loop start for correct counting

        for i, url in enumerate(remaining_urls, 1):
            current = start_offset + i
            pct = (current / total_urls) * 100
            print(f"\n[{current}/{total_urls}] ({pct:.1f}%) Processing:")

            try:
                rows = scrape_product(page, url)
                if rows:
                    all_data.extend(rows)
                    processed_urls.append(url)
                else:
                    failed_urls.append(url)
            except Exception as e:
                print(f"  Error scraping {url}: {e}")
                failed_urls.append(url)

            # Save checkpoint every CHECKPOINT_INTERVAL products
            if current % CHECKPOINT_INTERVAL == 0:
                print(f"\n--- Checkpoint: {current}/{total_urls} products, {len(all_data)} rows ---")
                save_checkpoint(processed_urls, all_data, urls, output_file)

            # Randomized delay between requests to avoid bot detection
            if i < len(remaining_urls):
                delay = random.uniform(2, 5)
                time.sleep(delay)

        # Close browser
        print("\nClosing browser...")
        browser.close()

    # Save results
    print("\n" + "-" * 40)
    print("Step 3: Saving results...")
    print("-" * 40)

    if all_data:
        filepath = save_to_csv(all_data)
        clear_checkpoint()

        # Print summary
        print("\n" + "=" * 60)
        print("SCRAPING COMPLETE")
        print("=" * 60)
        print(f"Total products scraped: {len(processed_urls)}")
        print(f"Total price tiers extracted: {len(all_data)}")
        print(f"Output file: {filepath}")

        if failed_urls:
            print(f"\nFailed to scrape {len(failed_urls)} products:")
            for url in failed_urls[:10]:
                print(f"  - {url}")
            if len(failed_urls) > 10:
                print(f"  ... and {len(failed_urls) - 10} more")

        # Preview the data
        print("\nData preview:")
        df = pd.DataFrame(all_data)
        # Show key columns (inventory columns vary by product)
        preview_cols = ['product_name', 'tier_quantity', 'price', 'discount_percent']
        # Add any inventory columns that exist
        inv_cols = [c for c in df.columns if c.startswith('inv_') and c.endswith('_qty')]
        preview_cols.extend(inv_cols[:2])  # Show up to 2 inventory columns
        available_cols = [c for c in preview_cols if c in df.columns]
        print(df[available_cols].head(10).to_string())
    else:
        print("\nNo data was extracted. Please check:")
        print("  1. Login credentials are correct")
        print("  2. Product URLs are valid")
        print("  3. You have access to view pricing on these products")


if __name__ == "__main__":
    main()
