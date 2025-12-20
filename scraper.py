#!/usr/bin/env python3
"""
IngredientsOnline.com Pricing Scraper

Scrapes pricing tier data from product pages after authenticating.
Credentials are read from environment variables IO_EMAIL and IO_PASSWORD.
URLs are read from urls.txt (one per line).
Output is saved to a timestamped CSV file.
"""

import os
import sys
import json
import time
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
import requests
from playwright.sync_api import sync_playwright, Page, Browser
from bs4 import BeautifulSoup


# Configuration
GRAPHQL_URL = "https://pwaktx64p8stvio.ingredientsonline.com/graphql"
LOGIN_URL = "https://www.ingredientsonline.com/login"
URLS_FILE = "urls.txt"
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
PAGE_TIMEOUT = 60000  # 60 seconds for slow-loading pages


def get_credentials() -> tuple[str, str]:
    """Get credentials from environment variables."""
    email = os.environ.get("IO_EMAIL")
    password = os.environ.get("IO_PASSWORD")

    if not email or not password:
        print("Error: Missing credentials.")
        print("Please set environment variables:")
        print("  export IO_EMAIL='your-email@example.com'")
        print("  export IO_PASSWORD='your-password'")
        sys.exit(1)

    return email, password


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


def login(page: Page, email: str, password: str) -> bool:
    """
    Log into ingredientsonline.com.
    Returns True if login successful, False otherwise.
    """
    print(f"Navigating to login page: {LOGIN_URL}")
    page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)

    # Wait for page to stabilize
    time.sleep(3)

    # Wait for the login form to be visible
    try:
        page.wait_for_selector('input[type="email"], input[name="email"], input[id="email"]', timeout=15000)
    except:
        # Try alternative selectors
        pass

    # Fill email - try multiple possible selectors
    email_selectors = [
        'input[type="email"]',
        'input[name="email"]',
        'input[id="email"]',
        'input[placeholder*="email" i]',
        'input[placeholder*="Email" i]',
    ]

    email_filled = False
    for selector in email_selectors:
        try:
            if page.locator(selector).count() > 0:
                page.fill(selector, email)
                email_filled = True
                print(f"Filled email using selector: {selector}")
                break
        except:
            continue

    if not email_filled:
        print("Error: Could not find email input field")
        return False

    # Fill password - try multiple possible selectors
    password_selectors = [
        'input[type="password"]',
        'input[name="password"]',
        'input[id="password"]',
    ]

    password_filled = False
    for selector in password_selectors:
        try:
            if page.locator(selector).count() > 0:
                page.fill(selector, password)
                password_filled = True
                print(f"Filled password using selector: {selector}")
                break
        except:
            continue

    if not password_filled:
        print("Error: Could not find password input field")
        return False

    # Click submit button - try multiple selectors
    submit_selectors = [
        'button[type="submit"]',
        'button:has-text("Sign In")',
        'button:has-text("Login")',
        'button:has-text("Log In")',
        'input[type="submit"]',
    ]

    submitted = False
    for selector in submit_selectors:
        try:
            if page.locator(selector).count() > 0:
                page.click(selector)
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
    time.sleep(5)
    try:
        page.wait_for_load_state("domcontentloaded", timeout=PAGE_TIMEOUT)
    except:
        pass

    # Dismiss cookie popup if present
    dismiss_cookie_popup(page)

    # Check if login was successful by looking for indicators
    # Usually after login, the URL changes or certain elements appear
    current_url = page.url
    if "login" not in current_url.lower() or "account" in current_url.lower():
        print(f"Login appears successful. Current URL: {current_url}")
        return True

    # Additional check - look for account-related elements
    try:
        # If we can find elements that only appear when logged in
        if page.locator('text="My Account"').count() > 0 or \
           page.locator('text="Sign Out"').count() > 0 or \
           page.locator('text="Logout"').count() > 0:
            print("Login successful - found authenticated user elements")
            return True
    except:
        pass

    print("Login may have failed - proceeding anyway to check data access")
    return True  # Proceed and see if we get authenticated data


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
    Scrape a single product page.
    Returns list of pricing tier dictionaries.
    """
    for attempt in range(MAX_RETRIES):
        try:
            print(f"  Navigating to: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)

            # Wait for page to stabilize and JS to execute
            time.sleep(2)

            # Dismiss cookie popup if it appears
            dismiss_cookie_popup(page)

            time.sleep(2)

            # Extract and parse data
            data = extract_next_data(page)

            if data:
                return parse_pricing(data, url)
            else:
                print(f"  Attempt {attempt + 1}: No data extracted")

        except Exception as e:
            print(f"  Attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)

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
    print("=" * 60)
    print("IngredientsOnline.com Pricing Scraper")
    print("=" * 60)

    # Get credentials and URLs
    email, password = get_credentials()
    urls = load_urls(URLS_FILE)

    all_data = []

    # Start Playwright browser
    with sync_playwright() as p:
        # Launch browser in headed mode for debugging
        print("\nLaunching browser (headed mode)...")
        browser = p.chromium.launch(headless=False, slow_mo=100)

        # Create a new context with a realistic viewport
        context = browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
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

        # Login
        print("\n" + "-" * 40)
        print("Step 1: Logging in...")
        print("-" * 40)

        if not login(page, email, password):
            print("Login failed. Exiting.")
            browser.close()
            sys.exit(1)

        # Scrape each URL
        print("\n" + "-" * 40)
        print("Step 2: Scraping product pages...")
        print("-" * 40)

        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] Processing:")
            rows = scrape_product(page, url)
            all_data.extend(rows)

            # Small delay between requests to be polite
            if i < len(urls):
                time.sleep(1)

        # Close browser
        print("\nClosing browser...")
        browser.close()

    # Save results
    print("\n" + "-" * 40)
    print("Step 3: Saving results...")
    print("-" * 40)

    if all_data:
        filepath = save_to_csv(all_data)

        # Print summary
        print("\n" + "=" * 60)
        print("SCRAPING COMPLETE")
        print("=" * 60)
        print(f"Total products scraped: {len(urls)}")
        print(f"Total price tiers extracted: {len(all_data)}")
        print(f"Output file: {filepath}")

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
        print("  2. URLs in urls.txt are valid product pages")
        print("  3. You have access to view pricing on these products")


if __name__ == "__main__":
    main()
