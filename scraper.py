#!/usr/bin/env python3
"""
IngredientsOnline.com Pricing Scraper (GraphQL API Version)

Scrapes pricing and inventory data using the GraphQL API.
No browser required - pure HTTP requests.

Credentials are read from environment variables IO_EMAIL and IO_PASSWORD.
Output is saved to a timestamped CSV file.
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
import requests


# =============================================================================
# Configuration
# =============================================================================

GRAPHQL_URL = "https://pwaktx64p8stvio.ingredientsonline.com/graphql"
BASE_URL = "https://www.ingredientsonline.com"

# Pagination settings
DEFAULT_PAGE_SIZE = 50  # Products per GraphQL query
REQUEST_DELAY = 0.5     # Seconds between requests (be polite to the API)

# Retry configuration
MAX_RETRIES = 5
RETRY_DELAY = 2


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
    load_env_file()

    email = os.environ.get("IO_EMAIL")
    password = os.environ.get("IO_PASSWORD")

    if not email or not password:
        print("Error: Missing credentials.")
        print("Please create a .env file with:")
        print("  IO_EMAIL=your-email@example.com")
        print("  IO_PASSWORD=your-password")
        sys.exit(1)

    return email, password


# =============================================================================
# GraphQL API Functions
# =============================================================================

def get_auth_token(email: str, password: str) -> str:
    """
    Authenticate via GraphQL and get JWT token.
    """
    query = '''
    mutation {
      generateCustomerToken(email: "%s", password: "%s") {
        token
      }
    }
    ''' % (email, password)

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                GRAPHQL_URL,
                json={'query': query},
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            if 'errors' in data:
                error_msg = data['errors'][0].get('message', 'Unknown error')
                print(f"Authentication error: {error_msg}")
                sys.exit(1)

            token = data['data']['generateCustomerToken']['token']
            return token

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"Auth attempt {attempt + 1} failed: {e}, retrying...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Authentication failed after {MAX_RETRIES} attempts: {e}")
                sys.exit(1)


def graphql_request(query: str, token: str, variables: Dict = None) -> Dict:
    """
    Make an authenticated GraphQL request.
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    payload = {'query': query}
    if variables:
        payload['variables'] = variables

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                GRAPHQL_URL,
                json=payload,
                headers=headers,
                timeout=60
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"  Request failed: {e}, retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                raise


def get_total_product_count(token: str, in_stock_only: bool = True) -> int:
    """
    Get total number of products available.
    """
    if in_stock_only:
        query = '''
        {
          products(filter: {in_stock: {eq: "1"}}, pageSize: 1) {
            total_count
          }
        }
        '''
    else:
        query = '''
        {
          products(filter: {}, pageSize: 1) {
            total_count
          }
        }
        '''
    data = graphql_request(query, token)
    return data['data']['products']['total_count']


def fetch_products_page(token: str, page: int, page_size: int, in_stock_only: bool = True) -> List[Dict]:
    """
    Fetch a page of products with pricing data, sorted alphabetically by name.
    """
    stock_filter = 'in_stock: {eq: "1"}' if in_stock_only else ''
    query = '''
    {
      products(filter: {%s}, pageSize: %d, currentPage: %d, sort: {name: ASC}) {''' % (stock_filter, page_size, page)

    query += '''
        items {
          __typename
          name
          sku
          url_key
          url_rewrites {
            url
          }
          ... on ConfigurableProduct {
            variants {
              product {
                sku
                name
                price_tiers {
                  quantity
                  final_price { value currency }
                  discount { percent_off }
                }
              }
            }
          }
          ... on SimpleProduct {
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
    '''

    data = graphql_request(query, token)
    return data['data']['products']['items']


def get_inventory(sku: str) -> List[Dict]:
    """
    Fetch inventory data from GraphQL API (no auth required).
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
        return []


# =============================================================================
# Data Processing
# =============================================================================

def get_product_url(product: Dict) -> str:
    """
    Get the correct product URL from url_rewrites field.
    This provides the canonical URL path for each product.
    """
    url_rewrites = product.get('url_rewrites', [])

    if url_rewrites and len(url_rewrites) > 0:
        # Use the first url_rewrite - this is the canonical URL
        url_path = url_rewrites[0].get('url', '')
        if url_path:
            # Ensure it starts with / and ends with /
            if not url_path.startswith('/'):
                url_path = '/' + url_path
            if not url_path.endswith('/'):
                url_path = url_path + '/'
            return f"{BASE_URL}{url_path}"

    # Fallback to just url_key if no rewrites
    url_key = product.get('url_key', '')
    if url_key:
        return f"{BASE_URL}/{url_key}/"

    return ''


def process_product(product: Dict) -> List[Dict]:
    """
    Process a single product and return rows for CSV.
    One row per price tier per variant.
    """
    rows = []
    timestamp = datetime.now().isoformat()

    product_name = product.get('name', 'Unknown')
    product_sku = product.get('sku', 'Unknown')
    product_url = get_product_url(product)
    product_type = product.get('__typename', 'Unknown')

    # Fetch inventory for this product
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

    # Base row data
    base_row = {
        'product_name': product_name,
        'product_sku': product_sku,
        'url': product_url,
        'scraped_at': timestamp
    }

    # Add inventory columns
    for warehouse, inv_info in inventory_by_warehouse.items():
        safe_name = warehouse.replace(' ', '_').replace(',', '')
        base_row[f'inv_{safe_name}_qty'] = inv_info['quantity']
        base_row[f'inv_{safe_name}_leadtime'] = inv_info['leadtime_weeks']
        base_row[f'inv_{safe_name}_eta'] = inv_info['next_stocking']

    # Handle ConfigurableProduct (has variants)
    if product_type == 'ConfigurableProduct':
        variants = product.get('variants', [])
        if not variants:
            return rows

        for variant in variants:
            variant_product = variant.get('product', {})
            variant_sku = variant_product.get('sku', 'Unknown')
            variant_name = variant_product.get('name', product_name)
            price_tiers = variant_product.get('price_tiers', [])

            if not price_tiers:
                continue

            for tier in price_tiers:
                row = base_row.copy()
                row.update({
                    'variant_sku': variant_sku,
                    'variant_name': variant_name,
                    'tier_quantity': tier.get('quantity', 0),
                    'price': tier.get('final_price', {}).get('value', 0),
                    'currency': tier.get('final_price', {}).get('currency', 'USD'),
                    'discount_percent': tier.get('discount', {}).get('percent_off', 0),
                })
                rows.append(row)

    # Handle SimpleProduct (no variants)
    elif product_type == 'SimpleProduct':
        price_tiers = product.get('price_tiers', [])

        if not price_tiers:
            return rows

        for tier in price_tiers:
            row = base_row.copy()
            row.update({
                'variant_sku': product_sku,
                'variant_name': product_name,
                'tier_quantity': tier.get('quantity', 0),
                'price': tier.get('final_price', {}).get('value', 0),
                'currency': tier.get('final_price', {}).get('currency', 'USD'),
                'discount_percent': tier.get('discount', {}).get('percent_off', 0),
            })
            rows.append(row)

    return rows


def save_to_csv(data: List[Dict], output_dir: str = ".") -> str:
    """
    Save scraped data to a timestamped CSV file.
    Returns the filepath of the created file.
    """
    if not data:
        print("No data to save")
        return ""

    df = pd.DataFrame(data)

    # Reorder columns for better readability
    priority_cols = [
        'product_name', 'product_sku', 'variant_sku', 'variant_name',
        'tier_quantity', 'price', 'currency', 'discount_percent', 'url', 'scraped_at'
    ]
    other_cols = [c for c in df.columns if c not in priority_cols]
    ordered_cols = [c for c in priority_cols if c in df.columns] + other_cols
    df = df[ordered_cols]

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"pricing_data_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False)
    print(f"\nSaved {len(data)} rows to: {filepath}")

    return filepath


# =============================================================================
# Main Scraper
# =============================================================================

def main():
    """Main entry point for the scraper."""
    parser = argparse.ArgumentParser(
        description='IngredientsOnline.com Pricing Scraper (GraphQL API)'
    )
    parser.add_argument('--page-size', type=int, default=DEFAULT_PAGE_SIZE,
                        help=f'Products per page (default: {DEFAULT_PAGE_SIZE})')
    parser.add_argument('--max-products', type=int, default=None,
                        help='Maximum products to scrape (for testing)')
    args = parser.parse_args()

    print("=" * 60)
    print("IngredientsOnline.com Pricing Scraper (GraphQL API)")
    print("=" * 60)

    # Get credentials and authenticate
    email, password = get_credentials()

    print("\nAuthenticating...")
    token = get_auth_token(email, password)
    print("✓ Authentication successful")

    # Get total product count
    print("\nFetching product count...")
    total_count = get_total_product_count(token)
    print(f"Found {total_count} total products")

    # Apply max limit if specified
    if args.max_products:
        total_count = min(total_count, args.max_products)
        print(f"Limited to {total_count} products (--max-products)")

    # Calculate pagination
    page_size = args.page_size
    total_pages = (total_count + page_size - 1) // page_size

    print(f"\nScraping {total_count} products ({page_size}/page, {total_pages} pages)")
    print("-" * 40)

    # Scrape all products
    all_data = []
    products_processed = 0
    start_time = time.time()

    for page in range(1, total_pages + 1):
        print(f"\n[Page {page}/{total_pages}] Fetching products...", flush=True)

        try:
            products = fetch_products_page(token, page, page_size)

            for product in products:
                if args.max_products and products_processed >= args.max_products:
                    break

                products_processed += 1
                pct = (products_processed / total_count) * 100
                print(f"  [{products_processed}/{total_count}] ({pct:.1f}%) {product.get('name', 'Unknown')[:50]}...", flush=True)

                rows = process_product(product)
                if rows:
                    all_data.extend(rows)
                    print(f"    → {len(rows)} price tiers", flush=True)
                else:
                    print(f"    → No pricing data", flush=True)

                # Small delay between inventory fetches
                time.sleep(REQUEST_DELAY)

            if args.max_products and products_processed >= args.max_products:
                break

        except Exception as e:
            print(f"  Error on page {page}: {e}")
            continue

        # Delay between pages
        time.sleep(REQUEST_DELAY)

    # Calculate elapsed time
    elapsed = time.time() - start_time
    rate = products_processed / elapsed if elapsed > 0 else 0

    # Save results
    print("\n" + "-" * 40)
    print("Saving results...")

    if all_data:
        filepath = save_to_csv(all_data)

        print("\n" + "=" * 60)
        print("SCRAPING COMPLETE")
        print("=" * 60)
        print(f"Products scraped: {products_processed}")
        print(f"Price tiers extracted: {len(all_data)}")
        print(f"Time elapsed: {elapsed:.1f}s ({rate:.1f} products/sec)")
        print(f"Output file: {filepath}")

        # Preview
        print("\nData preview:")
        df = pd.DataFrame(all_data)
        preview_cols = ['product_name', 'variant_sku', 'tier_quantity', 'price']
        available = [c for c in preview_cols if c in df.columns]
        print(df[available].head(10).to_string())
    else:
        print("\nNo data was extracted.")


if __name__ == "__main__":
    main()
