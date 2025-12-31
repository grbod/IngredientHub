# IngredientHub - Project Context for Claude

## Overview
Scrapers for B2B wholesale ingredient marketplaces:
- **IngredientsOnline.com (IO)** - B2B marketplace with tiered per-kg pricing
- **BulkSupplements.com (BS)** - Shopify store with per-package pricing
- **BoxNutra.com (BN)** - Shopify store with per-package pricing
- **TrafaPharma.com (TP)** - Custom PHP site with per-size pricing

## Project Structure
```
/IngredientHub/
├── backend/
│   ├── IO_scraper.py               # IngredientsOnline scraper (GraphQL API) - 3,225 lines
│   ├── bulksupplements_scraper.py  # BulkSupplements scraper (Shopify JSON) - 2,141 lines
│   ├── boxnutra_scraper.py         # BoxNutra scraper (Shopify JSON + HTML) - 2,013 lines
│   ├── trafapharma_scraper.py      # TrafaPharma scraper (HTML parsing) - 2,355 lines
│   ├── api/                        # FastAPI backend
│   │   ├── main.py                 # App entry, CORS, lifespan
│   │   ├── routes/
│   │   │   ├── scrapers.py         # POST /run, GET /status, GET /cron-suggestions
│   │   │   ├── runs.py             # GET /runs, GET /runs/{id}, GET /runs/{id}/alerts
│   │   │   └── alerts.py           # GET /alerts, GET /alerts/summary
│   │   └── services/
│   │       └── database.py         # DatabasePool connection management
│   ├── app.py                      # Streamlit frontend (legacy)
│   ├── .env                        # Credentials (not in git)
│   ├── venv/                       # Python virtual environment
│   ├── ingredients.db              # SQLite fallback (if no Supabase)
│   ├── tests/                      # Pytest test suite (198 tests, 12 files)
│   └── output/                     # CSV/JSON output files (not in git)
├── frontend/                       # React 19 + Vite + TypeScript
│   ├── src/
│   │   ├── pages/                  # Dashboard, Products, ProductDetail, PriceComparison, Admin
│   │   ├── hooks/                  # 10+ data fetching hooks
│   │   ├── lib/                    # Supabase client, API client, types
│   │   └── components/ui/          # shadcn/ui components
│   ├── package.json
│   ├── vite.config.ts
│   └── tailwind.config.js
├── CLAUDE.md                       # This file
└── .gitignore
```

## Environment Setup
```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install pandas requests psycopg2-binary playwright beautifulsoup4 python-dotenv fastapi uvicorn

# Create .env file with credentials
cat > .env << 'EOF'
IO_EMAIL=your_email@example.com
IO_PASSWORD=your_password
SUPABASE_DB_URL=postgresql://postgres.PROJECT_ID:PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres
EOF
```

## Usage Examples
```bash
# All commands run from backend/ directory
cd backend
source venv/bin/activate

# IO scraper - full run
python IO_scraper.py

# IO scraper - limited test run, no browser
python IO_scraper.py --max-products 50 --no-playwright

# BS scraper - full run
python bulksupplements_scraper.py

# BS scraper - limited test run
python bulksupplements_scraper.py --max-products 50

# BoxNutra scraper - full run
python boxnutra_scraper.py

# BoxNutra scraper - limited test run
python boxnutra_scraper.py --max-products 50

# TrafaPharma scraper - full run
python trafapharma_scraper.py

# TrafaPharma scraper - limited test run
python trafapharma_scraper.py --max-products 50

# Resume from checkpoint (any scraper)
python IO_scraper.py --resume

# Run tests
pytest tests/

# Start API server
uvicorn api.main:app --reload --port 8000

# Start frontend dev server
cd ../frontend && npm run dev
```

---

## Backend Architecture

### Common Patterns Across All Scrapers

All 4 scrapers share consistent architecture patterns for maintainability:

#### AlertType Enum
```python
class AlertType(Enum):
    NEW_PRODUCT = ("new_product", AlertSeverity.INFO)
    REACTIVATED = ("reactivated", AlertSeverity.INFO)
    PRICE_DECREASE_MAJOR = ("price_decrease_major", AlertSeverity.CRITICAL)  # >30%
    PRICE_INCREASE_MAJOR = ("price_increase_major", AlertSeverity.WARNING)   # >30%
    STOCK_OUT = ("stock_out", AlertSeverity.WARNING)
    STALE_VARIANT = ("stale_variant", AlertSeverity.WARNING)
    PARSE_FAILURE = ("parse_failure", AlertSeverity.WARNING)
    MISSING_REQUIRED = ("missing_required", AlertSeverity.WARNING)
    DB_ERROR = ("db_error", AlertSeverity.CRITICAL)
    HTTP_ERROR = ("http_error", AlertSeverity.CRITICAL)
```

#### UpsertResult Dataclass
```python
@dataclass
class UpsertResult:
    vendor_ingredient_id: int      # New or existing ID
    is_new: bool                   # True if newly created
    was_stale: bool = False        # True if reactivated from stale
    changed_fields: Dict[str, Tuple[Any, Any]] = field(default_factory=dict)  # field → (old, new)
```

#### DatabaseConnection Wrapper
- Auto-reconnect on connection loss (SSL errors, timeouts, broken pipes)
- `execute_with_retry(func, *args, max_retries=3)` - retry with reconnect
- Detects PostgreSQL vs SQLite and uses appropriate placeholders (`%s` vs `?`)
- Handles both Supabase (PostgreSQL) and local SQLite fallback

#### StatsTracker Class
Tracks all metrics during a scrape run:
```python
class StatsTracker:
    # Counters
    products_discovered: int
    products_processed: int
    products_skipped: int
    products_failed: int
    variants_new: int
    variants_updated: int
    variants_unchanged: int
    variants_stale: int
    variants_reactivated: int

    # Methods
    record_new_product(sku, name, vendor_ingredient_id)
    record_reactivated(sku, name, stale_since, vendor_ingredient_id)
    record_price_change(sku, name, old_price, new_price)  # >30% triggers alert
    record_stock_change(sku, name, was_in_stock, is_in_stock)
    record_stale(sku, name, last_seen_at)
    record_failure(slug, error_type, error_msg)
    print_report()  # Final console output
```

### Data Flow: Scraping to Database

```
1. SCRAPER STARTUP
   ├─ DatabaseConnection.connect() → PostgreSQL or SQLite
   ├─ init_database() → Create schema + seed lookup tables
   └─ StatsTracker(vendor_id, is_full_scrape, max_products)

2. PRODUCT DISCOVERY
   ├─ Fetch product list from vendor API/HTML
   └─ stats.products_discovered += count

3. FOR EACH PRODUCT
   ├─ get_existing_price() → For change detection
   ├─ get_existing_stock_status() → For change detection
   ├─ Parse product data
   └─ save_to_database(rows, stats)
       ├─ insert_scrape_source() → source_id
       ├─ get_or_create_category() → category_id
       ├─ get_or_create_manufacturer() → manufacturer_id
       ├─ get_or_create_ingredient() → ingredient_id
       ├─ get_or_create_variant() → variant_id
       └─ FOR EACH SKU VARIANT
           ├─ upsert_vendor_ingredient() → UpsertResult
           │   ├─ If new → stats.record_new_product()
           │   └─ If was_stale → stats.record_reactivated()
           ├─ delete_old_price_tiers() + insert_price_tier()
           │   └─ If >30% change → stats.record_price_change()
           ├─ upsert_order_rule()
           ├─ upsert_packaging_size()
           └─ upsert_inventory()
               └─ If stock_out → stats.record_stock_change()

4. CHECKPOINT (every N products)
   ├─ save_to_csv(all_data, output_file)
   ├─ db_wrapper.commit()
   └─ save_checkpoint(processed_skus, ...)

5. COMPLETION
   ├─ persist_run_to_database(stats) → ScrapeRuns record
   ├─ persist_alerts_to_database(stats.alerts) → ScrapeAlerts records
   ├─ stats.print_report()
   └─ clear_checkpoint()
```

### Stale Tracking (Soft-Delete)

Products not seen during a full scrape are marked stale instead of deleted:

```
active (initial state)
   ↓ [not seen in full scrape]
stale (status='stale', stale_since=timestamp)
   ↓ [re-encountered later]
active (status='active', stale_since=NULL) → triggers REACTIVATED alert
```

**Important:** Stale marking only runs on **full scrapes** (no `--max-products` limit).

### CLI Arguments (All Scrapers)

| Argument | Description |
|----------|-------------|
| `--max-products N` | Limit to N products (disables stale marking) |
| `--resume` | Resume from checkpoint file |
| `--checkpoint-interval N` | Save every N products (default: 10-25) |
| `--page-size N` | Products per API page |
| `--no-playwright` | Skip browser fallback (IO only) |

---

## API Layer (FastAPI)

### Endpoints

**Scrapers** (`/api/scrapers/`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/{vendor_id}/run` | Start scraper (background process) |
| GET | `/{vendor_id}/status` | Check if running, get progress |
| GET | `/cron-suggestions` | Recommended schedules per vendor |

**Runs** (`/api/runs/`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | List runs with pagination/filtering |
| GET | `/{run_id}` | Get specific run details |
| GET | `/{run_id}/alerts` | Get alerts for a run |

**Alerts** (`/api/alerts/`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | List alerts with filters |
| GET | `/summary` | Alert counts by severity/type/vendor |

### Running the API
```bash
cd backend
source venv/bin/activate
uvicorn api.main:app --reload --port 8000
```

---

## Frontend Architecture

### Tech Stack
- **Framework:** React 19 + TypeScript
- **Build:** Vite
- **Styling:** Tailwind CSS + shadcn/ui components
- **Data:** TanStack React Query + Supabase client
- **Routing:** React Router 7

### Pages

| Page | Path | Description |
|------|------|-------------|
| Dashboard | `/` | Vendor stats, product counts, freshness indicators |
| Products | `/products` | Browse ingredients with search, filters, pagination |
| ProductDetail | `/products/:id` | Single ingredient pricing across vendors |
| PriceComparison | `/compare` | Side-by-side vendor price comparison |
| Admin | `/admin` | Scraper control, run history, alert monitoring |

### Key Hooks

| Hook | Purpose |
|------|---------|
| `useIngredients(options)` | Paginated ingredient list with vendor/stock aggregation |
| `useIngredientDetail(id)` | Single ingredient with pricing, inventory, URLs |
| `usePriceComparison(search)` | Cross-vendor price comparison data |
| `useVendorStats()` | Dashboard stats (product counts, last scraped) |
| `useScrapeRuns(options)` | Run history with filtering |
| `useAlerts(options)` | Alert list with severity/type filters |
| `useScraperStatus(vendorId)` | Real-time scraper status (5s polling) |
| `useTriggerScraper()` | Mutation to start scraper run |

### Vendor Color Scheme
- **IngredientsOnline:** Sky blue (`bg-sky-50`, `text-sky-700`)
- **BulkSupplements:** Emerald green (`bg-emerald-50`, `text-emerald-700`)
- **BoxNutra:** Violet purple (`bg-violet-50`, `text-violet-700`)
- **TrafaPharma:** Amber yellow (`bg-amber-50`, `text-amber-700`)

### Running the Frontend
```bash
cd frontend
npm install
npm run dev  # http://localhost:5173
```

---

## Database Schema

### PostgreSQL (Supabase)
Primary database. Connection via Session Pooler for IPv4 compatibility:
- Host: `aws-0-us-west-2.pooler.supabase.com`
- Port: `6543`
- User: `postgres.PROJECT_ID`
- Database: `postgres`

Set `USE_POSTGRES = False` in scraper to use SQLite fallback.

### Core Tables

**Vendors** (4 vendors)
```sql
CREATE TABLE Vendors (
    vendor_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,        -- 'IngredientsOnline', 'BulkSupplements', 'BoxNutra', 'TrafaPharma'
    pricing_model TEXT,               -- 'tiered', 'fixed'
    status TEXT DEFAULT 'active'
)
-- Vendor IDs: IO=1, BS=4, BN=25, TP=26
```

**Ingredients** (Master ingredient list)
```sql
CREATE TABLE Ingredients (
    ingredient_id INTEGER PRIMARY KEY,
    category_id INTEGER REFERENCES Categories(category_id),
    name TEXT NOT NULL,
    status TEXT DEFAULT 'active'
)
```

**IngredientVariants** (Ingredient + manufacturer combinations)
```sql
CREATE TABLE IngredientVariants (
    variant_id INTEGER PRIMARY KEY,
    ingredient_id INTEGER NOT NULL REFERENCES Ingredients(ingredient_id),
    manufacturer_id INTEGER REFERENCES Manufacturers(manufacturer_id),
    variant_name TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    UNIQUE(ingredient_id, manufacturer_id, variant_name)
)
```

**VendorIngredients** (Core junction with status tracking)
```sql
CREATE TABLE VendorIngredients (
    vendor_ingredient_id INTEGER PRIMARY KEY,
    vendor_id INTEGER NOT NULL REFERENCES Vendors(vendor_id),
    variant_id INTEGER NOT NULL REFERENCES IngredientVariants(variant_id),
    sku TEXT,
    raw_product_name TEXT,
    shipping_responsibility TEXT,      -- 'buyer' (IO) or 'vendor' (BS/BN)
    shipping_terms TEXT,               -- 'EXW', 'FCA', etc.
    current_source_id INTEGER REFERENCES ScrapeSources(source_id),
    last_seen_at TEXT,                 -- ISO timestamp (staleness detection)
    status TEXT DEFAULT 'active',      -- 'active', 'stale', 'inactive'
    stale_since TEXT,                  -- ISO timestamp when marked stale
    UNIQUE(vendor_id, variant_id, sku)
)
```

**PriceTiers** (Tiered or flat pricing)
```sql
CREATE TABLE PriceTiers (
    price_tier_id INTEGER PRIMARY KEY,
    vendor_ingredient_id INTEGER NOT NULL REFERENCES VendorIngredients,
    pricing_model_id INTEGER NOT NULL REFERENCES PricingModels,
    min_quantity REAL DEFAULT 0,       -- 0=flat, 25/50/100+=tiered
    price REAL NOT NULL,
    price_per_kg REAL,                 -- Normalized for comparison
    effective_date TEXT NOT NULL
)
```

### Tracking Tables

**ScrapeRuns** (Run statistics)
```sql
CREATE TABLE ScrapeRuns (
    run_id INTEGER PRIMARY KEY,
    vendor_id INTEGER NOT NULL,
    started_at TEXT, completed_at TEXT, status TEXT,
    products_discovered INTEGER, products_processed INTEGER,
    products_skipped INTEGER, products_failed INTEGER,
    variants_new INTEGER, variants_updated INTEGER,
    variants_unchanged INTEGER, variants_stale INTEGER, variants_reactivated INTEGER,
    price_alerts INTEGER, stock_alerts INTEGER, data_quality_alerts INTEGER,
    is_full_scrape INTEGER, max_products_limit INTEGER
)
```

**ScrapeAlerts** (Individual alerts)
```sql
CREATE TABLE ScrapeAlerts (
    alert_id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL,
    vendor_ingredient_id INTEGER,
    alert_type TEXT,                   -- 'new_product', 'price_decrease_major', etc.
    severity TEXT,                     -- 'info', 'warning', 'critical'
    sku TEXT, product_name TEXT,
    old_value TEXT, new_value TEXT, change_percent REAL,
    message TEXT, created_at TEXT
)
```

### Flat Tables (CSV mirrors)
- `BSPricing` - BulkSupplements pricing (variant_id unique)
- `BoxNutraPricing` - BoxNutra pricing (variant_id unique)
- `TrafaPricing` - TrafaPharma pricing (product_id, size_id unique)

### Table Relationships
```
Categories
    └─ Ingredients
        └─ IngredientVariants
            ├─ Manufacturers
            └─ VendorIngredients
                ├─ Vendors
                ├─ ScrapeSources
                ├─ PriceTiers
                ├─ PackagingSizes
                ├─ OrderRules
                └─ InventoryLocations → InventoryLevels

ScrapeRuns → ScrapeAlerts
```

---

## Testing

### Test Suite Overview
- **198 tests** across 12 files
- All major functions tested against all 4 scrapers

### Test Files

| File | Focus |
|------|-------|
| `test_stats_tracker.py` | Alert recording, report generation |
| `test_staleness.py` | Stale marking, reactivation |
| `test_upsert.py` | Insert-or-update logic |
| `test_price_tiers.py` | Tiered/flat pricing |
| `test_integration.py` | End-to-end workflows |
| `test_parsing.py` | SKU, size, category parsing |
| `test_get_or_create.py` | Reference table lookups |
| `test_progress_tracking.py` | Checkpoint/resume |
| `test_edge_cases.py` | Error handling |
| `test_product_filtering.py` | Vendor-specific filters |
| `test_connection_errors.py` | Database reconnection |

### Running Tests
```bash
cd backend
source venv/bin/activate
pytest tests/                    # All tests
pytest tests/test_staleness.py   # Specific file
pytest -v                        # Verbose output
pytest -k "boxnutra"             # Filter by name
```

---

## IngredientsOnline.com (IO)

### API Architecture

#### Two GraphQL Endpoints
1. **Magento Backend** (primary): `https://pwaktx64p8stvio.ingredientsonline.com/graphql`
   - Products, pricing, inventory
   - No authentication required for most queries

2. **IOPlaza Gateway**: `https://ioplaza-gateway.ingredientsonline.com/graphql`
   - Different auth system
   - Currently not used

### SKU Structure
Product SKUs follow this pattern:
```
[product_id]-[variant_code]-[attribute_id]-[manufacturer_id]

Example: 59410-100-10312-11455
         │      │    │      └── Manufacturer ID (11455 = Sunnycare)
         │      │    └── Attribute ID
         │      └── Variant/Packaging Code (100 = 25kg Drum)
         └── Product ID
```

### Variant Codes (Packaging Types)

The second segment of variant SKUs indicates packaging size. **Complete mapping from 3,427 products (4,321 variants) - 154 unique codes.**

#### Most Common Codes

| Code | Packaging | Count |
|------|-----------|-------|
| `100` | 25 kg Drum | 1675 |
| `101` | 25 kg Bag | 373 |
| `142` | 0.03kg Bag | 326 |
| `102` | 25 kg Carton | 166 |
| `115` | 5 kg Bag | 119 |
| `104` | 20 kg Carton | 97 |
| `106` | 1kg Bag | 90 |
| `110` | 20kg Bag | 89 |
| `105` | 50 lb Bag | 82 |

#### Filtering Recommendations

**Sample codes (< 1kg) to exclude:**
`142`, `160`, `124`, `181`, `143`, `413`, `420`, `201`, `148`, `163`, `215`, `178`, `108`, `220`, `287`, `356`, `204`, `166`, `225`, `578`, `465`, `546`, `548`, `460`, `156`, `229`, `123`, `261`, `317`, `248`, `179`, `177`, `221`, `367`, `258`, `297`, `208`, `280`

**Standard bulk codes (25kg metric):**
`100`, `101`, `102`

### Inventory Data

#### Multiple Variants Per Warehouse
The inventory API returns **one entry per variant per warehouse**. A product with 4 packaging options will return 4 inventory entries for each warehouse.

Example for Astragalus P.E. 50% (chino warehouse):
```
Code 100 (25kg Drum): 1125 kg
Code 201 (100g Bag):  300 kg
Code 142 (0.03kg):    0.3 kg
Code 160 (0.05kg):    0.25 kg
```

**Important**: When aggregating inventory, keep the highest quantity per warehouse to avoid sample sizes overwriting bulk inventory.

#### Warehouse Codes
| Code | Location |
|------|----------|
| `chino` | Chino, CA |
| `nj` | New Jersey |
| `sw` | Southwest |
| `edison` | Edison, NJ |

### Common Issues

#### Headless Browser Detection
The site detects headless browsers. Use `headless=False` for Playwright.

#### API Rate Limiting
The API may timeout under heavy load. Implement retries with backoff.

#### Parent vs Variant SKU for Inventory
- **Parent SKU** (e.g., `59410-SUNNYCARE-11455`): Returns inventory for ALL variants
- **Variant SKU** (e.g., `59410-100-10312-11455`): Returns "Internal server error"

Always use parent SKU when querying inventory.

---

## BulkSupplements.com (BS)

### API Architecture
Shopify-based store. Uses standard Shopify JSON endpoints:
- Product list: `https://www.bulksupplements.com/products.json?page=N&limit=250`
- Product detail: `https://www.bulksupplements.com/products/HANDLE.json`

No authentication required.

### Data Model
- **Per-package pricing** (not per-kg tiered like IO)
- Products have multiple variants by form (Powder, Capsules, Softgels) and size (100g, 250g, 500g, 1kg)
- Only **powder variants** are scraped (filtered by `option1 == 'powder'`)

### SKU Structure
```
[PRODUCT_CODE][SIZE]

Example: MAGTAU250
         │      └── 250 grams
         └── Magnesium Taurate
```

### Variant Filtering
Products with no powder variants are skipped with status `[SKIPPED-NO_POWDER]`:
- Oil-based products (castor oil, fish oil)
- Pill-only products (melatonin pills)
- Capsule-only products

### Key Differences from IO

| Aspect | IngredientsOnline | BulkSupplements |
|--------|-------------------|-----------------|
| Pricing model | Per-kg tiered | Per-package fixed |
| Min order | 25kg multiples | Any single package |
| Shipping | Buyer pays (EXW) | Free (vendor pays) |
| Inventory | Multi-warehouse qty | In stock / Out of stock |
| Authentication | Required for pricing | Not required |

---

## BoxNutra.com (BN)

### API Architecture
Shopify-based store, similar to BulkSupplements:
- Product list: `https://www.boxnutra.com/products.json?page=N&limit=250`
- Product detail: `https://www.boxnutra.com/products/HANDLE.json`

No authentication required.

### Key Differences from BulkSupplements
- **Availability:** JSON API returns `null` for availability, must scrape HTML for stock status
- **Product filtering:** Filters out non-ingredient products (shipping insurance, gift cards, deposits)
- **Third-party filtering:** Skips products from marketplace sellers (Super Powders, Heray Spice, etc.)
- **Grams field:** Direct grams field in JSON (no parsing needed like BS)

### Business Model
```python
BOXNUTRA_BUSINESS_MODEL = {
    'order_rule_type': 'fixed_pack',
    'shipping_responsibility': 'vendor',  # Free shipping $49+
}
```

---

## TrafaPharma.com (TP)

### Platform Architecture
Custom PHP site (likely CodeIgniter) with server-side rendering. **No REST/JSON API available** - all data extracted via HTML parsing.

### Technical Details
- **Products:** ~663 total
- **Pricing:** Per-size (different prices for each size variant)
- **Pagination:** Infinite scroll via AJAX POST to `/products/index/pg/`
- **Authentication:** Not required

### Data Extraction Method
1. **Product Discovery:** Parse `/products` page, find links with "Add to Cart" image
2. **Product Details:** GET product page, extract name from `<title>`, code from "Product code:" text
3. **Size Prices:** For each size option, POST to product URL with `prod_size={size_id}` to get updated price

### Product Data Schema
```python
{
    'product_id': int,        # From /cart/add_to_wishlist/{id}
    'product_code': str,      # e.g., "RM2078"
    'product_name': str,
    'category': str,
    'size_id': str,           # Dropdown option value
    'size_name': str,         # e.g., "2.2 lbs/1 kg", "25kgs"
    'size_kg': float,         # Parsed kg value
    'price': float,           # None if "Inquire Bulk Price"
    'price_per_kg': float,
    'url': str
}
```

### Size Variants
Products have variable size options:
- Small: 10g, 25g, 50g, 100g
- Medium: 1 lb (450g), 1 kg
- Bulk: 25kg, Bulk Price (inquiry required)

### "Inquire Bulk Price" Products
Some products/sizes show "Inquire Bulk Price" instead of a fixed price. These are stored with `price=NULL` in the database.

---

## Vendor Comparison

| Aspect | IO | BS | BN | TrafaPharma |
|--------|----|----|----| ------------|
| Platform | Magento/GraphQL | Shopify JSON | Shopify JSON | Custom PHP |
| API | GraphQL | REST JSON | REST JSON | None (HTML) |
| Auth | Required | None | None | None |
| Pricing | Per-kg tiered | Per-package | Per-package | Per-size |
| Sizes | 25kg standard | Multiple (g to kg) | Multiple (g to kg) | Variable (10g to 25kg) |
| Inventory | Multi-warehouse | In stock/OOS | In stock/OOS (HTML) | Not available |
| Shipping | Buyer pays (EXW) | Vendor (free) | Vendor ($49+) | Vendor |
| Method | API queries | JSON fetch | JSON + HTML | HTML parse + POST |
