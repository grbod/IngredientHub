#!/usr/bin/env python3
"""
Ingredient Database Dashboard

Two-panel Streamlit app for searching ingredients and comparing vendor pricing/inventory.
- Left panel: Search with autocomplete + scrollable list
- Right panel: Detail view with stock status, pricing, and freshness indicator
"""

import os
import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from streamlit_searchbox import st_searchbox

# =============================================================================
# Configuration
# =============================================================================

st.set_page_config(
    page_title="Ingredient Database",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load environment variables
def load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

load_env()

# =============================================================================
# Database Connection
# =============================================================================

def get_connection():
    """Get database connection with reconnection support."""
    db_url = os.environ.get('SUPABASE_DB_URL')
    if not db_url:
        st.error("SUPABASE_DB_URL not found in .env file")
        st.stop()

    # Check if we have an existing connection in session state
    if 'db_conn' not in st.session_state or st.session_state.db_conn is None:
        st.session_state.db_conn = psycopg2.connect(db_url)

    # Test if connection is still alive
    try:
        st.session_state.db_conn.cursor().execute("SELECT 1")
    except:
        # Reconnect
        try:
            st.session_state.db_conn.close()
        except:
            pass
        st.session_state.db_conn = psycopg2.connect(db_url)

    return st.session_state.db_conn

# =============================================================================
# Data Queries
# =============================================================================

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_all_ingredients():
    """Get list of all ingredients for search."""
    conn = get_connection()
    query = """
        SELECT DISTINCT i.ingredient_id, i.name, c.name as category
        FROM ingredients i
        LEFT JOIN categories c ON i.category_id = c.category_id
        ORDER BY i.name
    """
    return pd.read_sql(query, conn)

def search_ingredients(query: str):
    """Search ingredients by name for autocomplete."""
    if not query or len(query) < 1:
        return []

    ingredients_df = get_all_ingredients()
    mask = ingredients_df['name'].str.lower().str.contains(query.lower(), na=False)
    matches = ingredients_df[mask]['name'].tolist()
    return matches[:15]  # Limit to 15 suggestions

@st.cache_data(ttl=60)  # Cache for 1 minute
def get_ingredient_details(ingredient_name: str):
    """Get full details for an ingredient including all vendors, pricing, and inventory."""
    conn = get_connection()

    # Get pricing data with vendor info (only active variants by default)
    pricing_query = """
        SELECT
            i.name as ingredient_name,
            v.name as vendor,
            vi.sku,
            vi.raw_product_name,
            vi.status as variant_status,
            vi.last_seen_at,
            pt.min_quantity as tier_qty,
            pt.price,
            pt.price_per_kg,
            pt.original_price,
            pt.discount_percent,
            pm.name as pricing_model,
            ps.description as packaging,
            ps.quantity as pack_size,
            ss.scraped_at,
            ss.product_url,
            u.name as unit,
            u.conversion_factor as unit_to_kg
        FROM ingredients i
        JOIN ingredientvariants iv ON i.ingredient_id = iv.ingredient_id
        JOIN vendoringredients vi ON iv.variant_id = vi.variant_id
        JOIN vendors v ON vi.vendor_id = v.vendor_id
        LEFT JOIN pricetiers pt ON vi.vendor_ingredient_id = pt.vendor_ingredient_id
        LEFT JOIN pricingmodels pm ON pt.pricing_model_id = pm.model_id
        LEFT JOIN packagingsizes ps ON vi.vendor_ingredient_id = ps.vendor_ingredient_id
        LEFT JOIN scrapesources ss ON vi.current_source_id = ss.source_id
        LEFT JOIN units u ON pt.unit_id = u.unit_id
        WHERE i.name = %s
        AND (vi.status = 'active' OR vi.status IS NULL)
        ORDER BY v.name, pt.min_quantity
    """
    pricing_df = pd.read_sql(pricing_query, conn, params=(ingredient_name,))

    # Get inventory data with warehouse locations
    inventory_query = """
        SELECT
            i.name as ingredient_name,
            v.name as vendor,
            vi.sku,
            l.name as warehouse,
            l.state as warehouse_state,
            il.quantity_available,
            il.stock_status,
            il.lead_time_days,
            il.expected_arrival,
            il.last_updated
        FROM ingredients i
        JOIN ingredientvariants iv ON i.ingredient_id = iv.ingredient_id
        JOIN vendoringredients vi ON iv.variant_id = vi.variant_id
        JOIN vendors v ON vi.vendor_id = v.vendor_id
        LEFT JOIN inventorylocations iloc ON vi.vendor_ingredient_id = iloc.vendor_ingredient_id
        LEFT JOIN inventorylevels il ON iloc.inventory_location_id = il.inventory_location_id
        LEFT JOIN locations l ON iloc.location_id = l.location_id
        WHERE i.name = %s
        AND (vi.status = 'active' OR vi.status IS NULL)
        ORDER BY v.name, l.name
    """
    inventory_df = pd.read_sql(inventory_query, conn, params=(ingredient_name,))

    # Get simple vendor inventory (for BS which doesn't have warehouse locations)
    vendor_inv_query = """
        SELECT
            i.name as ingredient_name,
            v.name as vendor,
            vi.sku,
            vinv.stock_status,
            vinv.last_updated
        FROM ingredients i
        JOIN ingredientvariants iv ON i.ingredient_id = iv.ingredient_id
        JOIN vendoringredients vi ON iv.variant_id = vi.variant_id
        JOIN vendors v ON vi.vendor_id = v.vendor_id
        LEFT JOIN vendorinventory vinv ON vi.vendor_ingredient_id = vinv.vendor_ingredient_id
        WHERE i.name = %s
        AND (vi.status = 'active' OR vi.status IS NULL)
        ORDER BY v.name
    """
    vendor_inv_df = pd.read_sql(vendor_inv_query, conn, params=(ingredient_name,))

    return pricing_df, inventory_df, vendor_inv_df

def get_freshness_status(scraped_at_str: str) -> tuple:
    """
    Calculate data freshness.
    Returns (status, color, days_old)
    """
    if not scraped_at_str:
        return "Unknown", "gray", None

    try:
        scraped_at = datetime.fromisoformat(scraped_at_str.replace('Z', '+00:00'))
        now = datetime.now(scraped_at.tzinfo) if scraped_at.tzinfo else datetime.now()
        age = now - scraped_at
        days_old = age.days

        if days_old == 0:
            return "Fresh (today)", "green", 0
        elif days_old == 1:
            return "Recent (yesterday)", "green", 1
        elif days_old <= 7:
            return f"This week ({days_old}d ago)", "orange", days_old
        else:
            return f"Stale ({days_old}d ago)", "red", days_old
    except:
        return "Unknown", "gray", None

# =============================================================================
# UI Components
# =============================================================================

def render_stock_badge(status: str, qty: float = None, warehouse: str = None):
    """Render a stock status badge (legacy, kept for compatibility)."""
    if status == 'in_stock':
        color = "green"
        icon = "✓"
        label = "In Stock"
    elif status == 'out_of_stock':
        color = "red"
        icon = "✗"
        label = "Out of Stock"
    else:
        color = "gray"
        icon = "?"
        label = "Unknown"

    qty_str = f" ({qty:,.0f} kg)" if qty and qty > 0 else ""
    wh_str = f" @ {warehouse}" if warehouse else ""

    return f":{color}[{icon} {label}{qty_str}{wh_str}]"

def render_stock_card(status: str, vendor: str = None, qty: float = None,
                      warehouse: str = None, state: str = None, lead_time: int = None):
    """Render a styled stock status card."""
    # Determine status class and label
    if status == 'in_stock':
        status_class = 'instock'
        status_label = 'In Stock'
        status_icon = '✓'
    elif status == 'out_of_stock':
        status_class = 'outofstock'
        status_label = 'Out of Stock'
        status_icon = '✗'
    else:
        status_class = 'unknown'
        status_label = 'Unknown'
        status_icon = '?'

    # Build location string
    location = ""
    if warehouse:
        location = f"{warehouse}, {state}" if state else warehouse

    # Build content parts
    parts = [f'<div class="stock-status stock-status-{status_class}">{status_icon} {status_label}</div>']

    if vendor and not warehouse:
        parts.append(f'<div class="stock-location">{vendor}</div>')

    if location:
        parts.append(f'<div class="stock-location">📍 {location}</div>')

    if qty and qty > 0:
        parts.append(f'<div class="stock-quantity">{qty:,.0f} kg available</div>')

    if lead_time:
        parts.append(f'<div class="stock-leadtime">Lead time: {lead_time} days</div>')

    content = "".join(parts)
    return f'<div class="stock-card stock-card-{status_class}">{content}</div>'

def render_price_table(df: pd.DataFrame, inventory_df: pd.DataFrame = None, vendor_inv_df: pd.DataFrame = None):
    """Render pricing table with tiers, pack prices, discounts, and stock status."""
    if df.empty:
        st.info("No pricing data available")
        return

    # Map warehouse names to state abbreviations
    WAREHOUSE_TO_STATE = {
        'chino': 'CA',
        'edison': 'NJ',
        'nj': 'NJ',
        'sw': 'SW',
    }

    def get_inventory_for_sku(sku, vendor):
        """Look up inventory data for a specific SKU. Returns (warehouses_list, status) for IO, or (None, status) for others."""
        if vendor == 'IngredientsOnline':
            if inventory_df is None or inventory_df.empty:
                return [], 'unknown'
            inv_rows = inventory_df[inventory_df['sku'] == sku]
            if inv_rows.empty:
                return [], 'unknown'
            # Get ALL warehouses with stock > 0
            in_stock_rows = inv_rows[inv_rows['quantity_available'] > 0]
            if not in_stock_rows.empty:
                warehouses = []
                for _, row in in_stock_rows.iterrows():
                    qty = row['quantity_available']
                    warehouse = row.get('warehouse', '')
                    state = WAREHOUSE_TO_STATE.get(warehouse.lower(), warehouse) if warehouse else ''
                    warehouses.append((qty, state))
                # Sort by quantity descending
                warehouses.sort(key=lambda x: x[0], reverse=True)
                return warehouses, 'in_stock'
            else:
                return [], 'out_of_stock'
        else:
            if vendor_inv_df is None or vendor_inv_df.empty:
                return None, 'unknown'
            inv_rows = vendor_inv_df[(vendor_inv_df['sku'] == sku) & (vendor_inv_df['vendor'] == vendor)]
            if inv_rows.empty:
                return None, 'unknown'
            status = inv_rows['stock_status'].iloc[0] if pd.notna(inv_rows['stock_status'].iloc[0]) else 'unknown'
            return None, status

    def format_inventory_cell(warehouses_or_qty, status, vendor):
        """Format inventory for display in table cell."""
        if vendor == 'IngredientsOnline':
            if warehouses_or_qty and isinstance(warehouses_or_qty, list) and len(warehouses_or_qty) > 0:
                parts = []
                for qty, state in warehouses_or_qty:
                    if state:
                        parts.append(f"{qty:,.0f}kg ({state})")
                    else:
                        parts.append(f"{qty:,.0f}kg")
                return " & ".join(parts)
            elif status == 'out_of_stock':
                return "Out of Stock"
            else:
                return "-"
        elif status == 'in_stock':
            return "In Stock"
        elif status == 'out_of_stock':
            return "Out of Stock"
        else:
            return "-"

    # Group by vendor - consolidate all SKUs into one table per vendor
    for vendor in df['vendor'].unique():
        vendor_df = df[df['vendor'] == vendor].copy()

        # Build consolidated table rows for all SKUs in this vendor
        table_rows = []

        for sku in vendor_df['sku'].unique():
            sku_df = vendor_df[vendor_df['sku'] == sku].copy()

            # Get packaging info for Size column
            packaging = sku_df['packaging'].iloc[0] if 'packaging' in sku_df.columns and pd.notna(sku_df['packaging'].iloc[0]) else "-"

            # Get pack size in kg (for calculating pack price for IO)
            pack_size = sku_df['pack_size'].iloc[0] if 'pack_size' in sku_df.columns and pd.notna(sku_df['pack_size'].iloc[0]) else None

            # Get conversion factor (default 1.0 for kg)
            conversion = sku_df['unit_to_kg'].iloc[0] if 'unit_to_kg' in sku_df.columns and pd.notna(sku_df['unit_to_kg'].iloc[0]) else 1.0

            # Get inventory for this SKU
            inv_data, inv_status = get_inventory_for_sku(sku, vendor)
            inv_display = format_inventory_cell(inv_data, inv_status, vendor)

            # Get unique price tiers for this SKU
            cols = ['tier_qty', 'price', 'price_per_kg', 'discount_percent']
            available_cols = [c for c in cols if c in sku_df.columns]
            tiers = sku_df[available_cols].drop_duplicates()

            for _, tier in tiers.iterrows():
                # Format quantity in kg
                tier_qty = tier.get('tier_qty')
                if pd.notna(tier_qty):
                    qty_kg = tier_qty * conversion
                    qty_str = f"{qty_kg:,.0f}" if qty_kg >= 1 else f"{qty_kg:,.2f}"
                else:
                    qty_str = "-"

                # Calculate Price/Pack
                # For IO: price_per_kg * pack_size (IO stores per-kg price)
                # For others: use price directly (already pack price)
                if vendor == 'IngredientsOnline' and pack_size and pd.notna(tier.get('price_per_kg')):
                    pack_price = tier['price_per_kg'] * pack_size
                    price_pack_str = f"${pack_price:,.2f}"
                elif pd.notna(tier.get('price')):
                    price_pack_str = f"${tier['price']:,.2f}"
                else:
                    price_pack_str = "-"

                table_rows.append({
                    'Size': packaging,
                    'Min Qty (kg)': qty_str,
                    'Price/Pack': price_pack_str,
                    'Normalized $/kg': f"${tier['price_per_kg']:,.2f}" if pd.notna(tier.get('price_per_kg')) else "-",
                    '_price_per_kg_sort': tier.get('price_per_kg') if pd.notna(tier.get('price_per_kg')) else float('inf'),
                    'Stock': inv_display
                })

        # Create display dataframe and sort by price_per_kg descending (smallest packs first)
        display_df = pd.DataFrame(table_rows)
        display_df = display_df.sort_values('_price_per_kg_sort', ascending=False).drop(columns=['_price_per_kg_sort']).reset_index(drop=True)

        # Style the Stock column with colors
        def color_stock(val):
            if val == "In Stock" or "kg" in str(val):
                return 'color: #22c55e'  # Green
            elif val == "Out of Stock":
                return 'color: #ef4444'  # Red
            return ''

        styled_df = display_df.style.applymap(color_stock, subset=['Stock'])
        st.dataframe(styled_df, hide_index=True, use_container_width=True)

def render_inventory_section(inventory_df: pd.DataFrame, vendor_inv_df: pd.DataFrame):
    """Render inventory status section with styled cards.

    Only shows warehouse-level cards for IngredientsOnline.
    BS/BN/TP inventory is shown per-row in the pricing table instead.
    """

    # Only show warehouse-level inventory cards (IO has this)
    # BS/BN stock status is now shown in the pricing table per-row
    has_warehouse_data = not inventory_df.empty and inventory_df['warehouse'].notna().any()

    cards_html = ""

    if has_warehouse_data:
        for _, row in inventory_df.iterrows():
            if pd.notna(row.get('warehouse')):
                status = row.get('stock_status', 'unknown')
                qty = row.get('quantity_available', 0)
                warehouse = row.get('warehouse', '')
                state = row.get('warehouse_state', '')
                lead_time = row.get('lead_time_days')

                cards_html += render_stock_card(
                    status=status,
                    qty=qty,
                    warehouse=warehouse,
                    state=state,
                    lead_time=lead_time
                )

    if cards_html:
        st.markdown(cards_html, unsafe_allow_html=True)
    else:
        st.info("Stock status shown in pricing table")

# =============================================================================
# Main App
# =============================================================================

def main():
    # Custom CSS for theme, cards, and layout
    st.markdown("""
        <style>
        /* =================================
           CSS Variables - Color Palette
           ================================= */
        :root {
            --stock-green: #22c55e;
            --stock-green-bg: rgba(34, 197, 94, 0.1);
            --stock-green-border: rgba(34, 197, 94, 0.3);
            --stock-red: #ef4444;
            --stock-red-bg: rgba(239, 68, 68, 0.1);
            --stock-red-border: rgba(239, 68, 68, 0.3);
            --stock-gray: #6b7280;
            --stock-gray-bg: rgba(107, 114, 128, 0.1);
            --stock-gray-border: rgba(107, 114, 128, 0.3);
            --accent-blue: #3b82f6;
            --card-bg: #1e1e1e;
            --card-border: #333;
            --text-primary: #ffffff;
            --text-secondary: #9ca3af;
        }

        /* =================================
           Sidebar Styling
           ================================= */
        [data-testid="stSidebar"] {
            min-width: 380px;
            max-width: 380px;
        }
        [data-testid="stSidebar"] > div:first-child {
            min-width: 380px;
            max-width: 380px;
        }
        div[data-testid="stSidebarContent"] {
            padding-top: 1rem;
        }

        /* =================================
           Stock Status Cards
           ================================= */
        .stock-card {
            border-radius: 12px;
            padding: 16px 20px;
            margin-bottom: 12px;
            border-left: 4px solid;
        }
        .stock-card-instock {
            background: var(--stock-green-bg);
            border-left-color: var(--stock-green);
        }
        .stock-card-outofstock {
            background: var(--stock-red-bg);
            border-left-color: var(--stock-red);
        }
        .stock-card-unknown {
            background: var(--stock-gray-bg);
            border-left-color: var(--stock-gray);
        }
        .stock-status {
            font-size: 0.85rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 4px;
        }
        .stock-status-instock { color: var(--stock-green); }
        .stock-status-outofstock { color: var(--stock-red); }
        .stock-status-unknown { color: var(--stock-gray); }

        .stock-location {
            font-size: 1rem;
            color: var(--text-primary);
            margin-bottom: 4px;
            display: flex;
            align-items: center;
            gap: 6px;
        }
        .stock-quantity {
            font-size: 1.25rem;
            font-weight: 700;
            color: var(--text-primary);
            margin: 8px 0;
        }
        .stock-leadtime {
            font-size: 0.85rem;
            color: var(--text-secondary);
        }

        /* =================================
           Vendor Cards
           ================================= */
        .vendor-card {
            background: var(--card-bg);
            border: 1px solid var(--card-border);
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 16px;
        }
        .vendor-name {
            font-size: 1.1rem;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 8px;
        }
        .vendor-packaging {
            font-size: 0.9rem;
            color: var(--text-secondary);
            margin-bottom: 12px;
        }

        /* =================================
           Freshness Badge
           ================================= */
        .freshness-badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 16px;
            font-size: 0.85rem;
            font-weight: 500;
        }
        .freshness-fresh {
            background: var(--stock-green-bg);
            color: var(--stock-green);
            border: 1px solid var(--stock-green-border);
        }
        .freshness-recent {
            background: var(--stock-green-bg);
            color: var(--stock-green);
            border: 1px solid var(--stock-green-border);
        }
        .freshness-stale {
            background: var(--stock-red-bg);
            color: var(--stock-red);
            border: 1px solid var(--stock-red-border);
        }
        .freshness-unknown {
            background: var(--stock-gray-bg);
            color: var(--stock-gray);
            border: 1px solid var(--stock-gray-border);
        }

        /* =================================
           General Polish
           ================================= */
        .section-header {
            font-size: 1.25rem;
            font-weight: 600;
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        /* Dataframe styling */
        .stDataFrame {
            border-radius: 8px;
        }

        /* Ingredient list styling */
        .ingredient-list-container {
            max-height: 450px;
            overflow-y: auto;
            border: 1px solid #333;
            border-radius: 8px;
        }

        /* Style buttons as list items */
        [data-testid="stSidebar"] .stButton > button {
            width: 100%;
            text-align: left !important;
            justify-content: flex-start !important;
            background: #1a1a1a;
            border: none;
            border-bottom: 1px solid #2a2a2a;
            border-radius: 0;
            padding: 8px 12px;
            color: #e0e0e0;
            font-size: 0.85rem;
            font-weight: 400;
        }
        [data-testid="stSidebar"] .stButton > button * {
            text-align: left !important;
            justify-content: flex-start !important;
        }
        [data-testid="stSidebar"] .stButton > button > div {
            width: 100%;
            text-align: left !important;
        }
        [data-testid="stSidebar"] .stButton > button p {
            text-align: left !important;
            width: 100%;
        }
        [data-testid="stSidebar"] .stButton:nth-child(odd) > button {
            background: #1e1e1e;
        }
        [data-testid="stSidebar"] .stButton:nth-child(even) > button {
            background: #252525;
        }
        [data-testid="stSidebar"] .stButton > button:hover {
            background: #2d3748 !important;
            border-color: #2a2a2a;
        }
        [data-testid="stSidebar"] .stButton > button:focus {
            box-shadow: none;
        }

        /* Selected item styling */
        [data-testid="stSidebar"] .stButton > button[kind="primary"],
        [data-testid="stSidebar"] .stButton > button[data-testid="stBaseButton-primary"] {
            background: #1e3a5f !important;
            border-left: 3px solid #3b82f6;
            color: #fff;
        }
        </style>
    """, unsafe_allow_html=True)

    # Sidebar - Search and ingredient list
    with st.sidebar:
        st.title("🧪 Ingredients")

        # Autocomplete search box
        selected_from_search = st_searchbox(
            search_ingredients,
            key="ingredient_search",
            placeholder="Type to search...",
            clear_on_submit=True,
            default=None
        )

        # If something was selected from search, update session state and rerun
        if selected_from_search:
            if st.session_state.get('selected_ingredient') != selected_from_search:
                st.session_state['selected_ingredient'] = selected_from_search
                st.rerun()

        # Get all ingredients for the list
        ingredients_df = get_all_ingredients()

        # Get current selection
        selected_ingredient = st.session_state.get('selected_ingredient')

        # Filter display
        st.caption(f"{len(ingredients_df)} total ingredients")

        st.markdown("---")

        # Create list of ingredient names with categories
        ingredient_options = ingredients_df['name'].tolist()

        # Find current index
        current_index = 0
        if selected_ingredient and selected_ingredient in ingredient_options:
            current_index = ingredient_options.index(selected_ingredient)

        # Scrollable list using buttons styled as list items
        st.markdown("##### Browse All")

        # Create scrollable container
        with st.container(height=450):
            for idx, ingredient_name in enumerate(ingredient_options):
                is_selected = ingredient_name == st.session_state.get('selected_ingredient')
                if st.button(
                    ingredient_name,
                    key=f"ing_{idx}_{ingredient_name}",
                    use_container_width=True,
                    type="primary" if is_selected else "secondary"
                ):
                    if st.session_state.get('selected_ingredient') != ingredient_name:
                        st.session_state['selected_ingredient'] = ingredient_name
                        st.rerun()

    # Main content area - use the radio selection directly for display
    selected_ingredient = st.session_state.get('selected_ingredient')

    if selected_ingredient:
        # Header with ingredient name and freshness
        pricing_df, inventory_df, vendor_inv_df = get_ingredient_details(selected_ingredient)

        # Get freshness from most recent scrape
        latest_scrape = None
        if not pricing_df.empty and 'scraped_at' in pricing_df.columns:
            latest_scrape = pricing_df['scraped_at'].dropna().max()

        freshness_status, freshness_color, days_old = get_freshness_status(latest_scrape)

        # Map color to CSS class
        freshness_class_map = {
            'green': 'fresh' if days_old == 0 else 'recent',
            'orange': 'stale',
            'red': 'stale',
            'gray': 'unknown'
        }
        freshness_class = freshness_class_map.get(freshness_color, 'unknown')

        # Title row with freshness badge
        st.title(selected_ingredient)

        # Subheader row with freshness and link
        if not pricing_df.empty:
            product_url = pricing_df['product_url'].dropna().iloc[0] if 'product_url' in pricing_df.columns and pricing_df['product_url'].notna().any() else None
            vendor_name = pricing_df['vendor'].iloc[0] if 'vendor' in pricing_df.columns else None
        else:
            product_url = None
            vendor_name = None

        # Build header info row with vendor name
        header_html = f'''
        <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 16px;">
            {f'<span style="font-weight: 600; color: #9ca3af;">{vendor_name}</span>' if vendor_name else ''}
            <span class="freshness-badge freshness-{freshness_class}">{freshness_status}</span>
            {f'<a href="{product_url}" target="_blank" style="color: #3b82f6; text-decoration: none;">View on vendor site →</a>' if product_url else ''}
        </div>
        '''
        st.markdown(header_html, unsafe_allow_html=True)

        st.divider()

        # Pricing table (full width)
        st.header("💰 Pricing")
        render_price_table(pricing_df, inventory_df, vendor_inv_df)

        # Stock Availability section (only shows for IO with warehouse data)
        st.header("📦 Stock Availability")
        render_inventory_section(inventory_df, vendor_inv_df)

        # Additional details section
        st.divider()

        with st.expander("📋 Raw Data", expanded=False):
            tab1, tab2 = st.tabs(["Pricing Data", "Inventory Data"])

            with tab1:
                if not pricing_df.empty:
                    st.dataframe(pricing_df, use_container_width=True)
                else:
                    st.info("No pricing data")

            with tab2:
                if not inventory_df.empty:
                    st.dataframe(inventory_df, use_container_width=True)
                elif not vendor_inv_df.empty:
                    st.dataframe(vendor_inv_df, use_container_width=True)
                else:
                    st.info("No inventory data")

    else:
        # Welcome screen
        st.title("Ingredient Database")
        st.markdown("""
        ### Welcome!

        Use the sidebar to search and select an ingredient to view:
        - **Stock availability** by warehouse location
        - **Pricing** with all tier levels
        - **Data freshness** indicators
        """)

        st.divider()

        # Vendor stats table
        st.markdown("**Vendors:**")
        conn = get_connection()
        vendor_stats_query = """
            SELECT
                v.name as "Vendor",
                COUNT(DISTINCT vi.variant_id) as "Products",
                COUNT(DISTINCT vi.vendor_ingredient_id) as "Size Variants",
                MAX(ss.scraped_at) as last_scraped
            FROM vendors v
            LEFT JOIN vendoringredients vi ON v.vendor_id = vi.vendor_id
            LEFT JOIN scrapesources ss ON vi.current_source_id = ss.source_id
            WHERE v.status = 'active'
            GROUP BY v.vendor_id, v.name
            ORDER BY "Products" DESC
        """
        vendor_stats_df = pd.read_sql(vendor_stats_query, conn)

        # Format the last_scraped column
        def format_scrape_date(dt):
            if pd.isna(dt):
                return "-"
            try:
                if isinstance(dt, str):
                    dt = pd.to_datetime(dt)
                return dt.strftime("%m/%d/%y %H:%M")
            except:
                return "-"

        vendor_stats_df["Last Scraped"] = vendor_stats_df["last_scraped"].apply(format_scrape_date)
        vendor_stats_df = vendor_stats_df.drop(columns=["last_scraped"])

        st.dataframe(vendor_stats_df, hide_index=True, use_container_width=True)

        st.divider()

        # Categories table
        st.markdown("**Categories:**")
        categories_query = """
            SELECT
                c.name as "Category",
                COUNT(DISTINCT i.ingredient_id) as "Ingredients"
            FROM categories c
            LEFT JOIN ingredients i ON c.category_id = i.category_id
            GROUP BY c.category_id, c.name
            HAVING COUNT(DISTINCT i.ingredient_id) > 0
            ORDER BY "Ingredients" DESC
        """
        categories_df = pd.read_sql(categories_query, conn)
        st.dataframe(categories_df, hide_index=True, use_container_width=True)


if __name__ == "__main__":
    main()
