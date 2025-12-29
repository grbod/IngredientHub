# IOscraper - Project Context for Claude

## Overview
Scrapers for B2B wholesale ingredient marketplaces:
- **IngredientsOnline.com (IO)** - B2B marketplace with tiered per-kg pricing
- **BulkSupplements.com (BS)** - Shopify store with per-package pricing
- **BoxNutra.com (BN)** - Shopify store with per-package pricing
- **TrafaPharma.com (TP)** - Custom PHP site with per-size pricing

## Project Structure
```
/IOscraper/
├── IO_scraper.py                   # IngredientsOnline scraper (GraphQL API)
├── bulksupplements_scraper.py      # BulkSupplements scraper (Shopify JSON)
├── boxnutra_scraper.py             # BoxNutra scraper (Shopify JSON + HTML)
├── trafapharma_scraper.py          # TrafaPharma scraper (HTML parsing)
├── .env                            # Credentials (not in git)
├── venv/                           # Python virtual environment
├── ingredients.db                  # SQLite fallback (if no Supabase)
└── CLAUDE.md                       # This file
```

## Environment Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install pandas requests psycopg2-binary playwright beautifulsoup4

# Create .env file with credentials
cat > .env << 'EOF'
IO_EMAIL=your_email@example.com
IO_PASSWORD=your_password
SUPABASE_DB_URL=postgresql://postgres.PROJECT_ID:PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres
EOF
```

## Usage Examples
```bash
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
```

## Database

### PostgreSQL (Supabase)
Primary database. Connection via Session Pooler for IPv4 compatibility:
- Host: `aws-0-us-west-2.pooler.supabase.com`
- Port: `6543`
- User: `postgres.PROJECT_ID`
- Database: `postgres`

Set `USE_POSTGRES = False` in scraper to use SQLite fallback.

### Key Tables
- `Vendors` - IO, BulkSupplements, BoxNutra, and TrafaPharma
- `Ingredients` - Ingredient names
- `VendorIngredients` - SKU-level product data per vendor
- `PriceTiers` - Pricing (tiered for IO, per-package for BS/BN)
- `Pricing` / `BSPricing` / `BoxNutraPricing` / `TrafaPricing` - Flat tables mirroring CSV output

---

## IngredientsOnline.com (IO)

## API Architecture

### Two GraphQL Endpoints
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

## Variant Codes (Packaging Types)

The second segment of variant SKUs indicates packaging size. **Complete mapping from 3,427 products (4,321 variants) - 154 unique codes.**

### Complete Code → Packaging Map

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
| `420` | 50g Bag | 78 |
| `250` | 1gal Jug | 77 |
| `406` | 5gal Pail | 75 |
| `201` | 100g Bag | 70 |
| `111` | 20 kg Drum | 68 |
| `188` | 55 gal Drum | 64 |
| `246` | 25lb Carton | 59 |
| `413` | 30g Bag | 44 |
| `160` | 0.05kg Bag | 44 |
| `103` | 10 kg Carton | 41 |
| `124` | 0.1kg Bag | 31 |
| `181` | 10g Bag | 29 |
| `136` | 15kg Carton | 26 |
| `267` | 40 lb Bag | 21 |
| `313` | 420lb Drum | 20 |
| `148` | 25g Bag | 20 |
| `146` | 10kg Bag | 19 |
| `126` | 50lb Carton | 19 |
| `163` | 20g Bag | 17 |
| `158` | 40lb Carton | 17 |
| `143` | 5g Bag | 17 |
| `113` | 10 kg Drum | 16 |
| `165` | 5kg Drum | 16 |
| `351` | (1,665 pieces) Carton | 15 |
| `180` | 44lb Bag | 15 |
| `280` | 1oz Bottle | 14 |
| `114` | 5kg Carton | 13 |
| `198` | 20 lb Carton | 13 |
| `128` | 25 lb Bag | 13 |
| `109` | 1kg Tin | 13 |
| `354` | (2,500 pieces) Carton | 13 |
| `159` | 200kg Drum | 13 |
| `215` | 30g Bottle | 11 |
| `178` | 200g Bag | 11 |
| `133` | 15 kg Bag | 11 |
| `353` | (2,000 pieces) Carton | 9 |
| `108` | 0.5kg Bag | 8 |
| `120` | 5 kg Tin | 8 |
| `502` | 1kg Bag | 7 |
| `258` | 500 g Bottle | 7 |
| `152` | 190 kg Drum | 7 |
| `185` | 55 lb Bag | 6 |
| `220` | 10g Bottle | 6 |
| `367` | 500g Bag | 5 |
| `345` | (6,000 pieces) Carton | 5 |
| `189` | 18kg Pail | 5 |
| `249` | 1kg Jug | 5 |
| `187` | 5 gal Drum | 4 |
| `221` | 15g Bag | 4 |
| `306` | 50lb Drum | 4 |
| `297` | 50ml Bottle | 4 |
| `373` | 300Kg Drum | 4 |
| `117` | 50kg Drum | 4 |
| `166` | 250g Bag | 4 |
| `412` | 20kg Pail | 4 |
| `358` | (10,000 pieces) Carton | 4 |
| `287` | 500g Bulk Sample | 4 |
| `225` | 300g Bag | 4 |
| `428` | 900kg IBC Totes | 4 |
| `394` | (3,000 pieces) Carton | 4 |
| `217` | 250kg Drum | 4 |
| `578` | 110g Bottle | 4 |
| `236` | 280Kg Drum | 3 |
| `112` | 15 kg Drum | 3 |
| `379` | (3,000 pieces) Drum | 3 |
| `562` | (100,000 pieces) Carton | 3 |
| `561` | (70,000 pieces) Carton | 3 |
| `168` | 1lb Bag | 3 |
| `161` | 275 kg Drum | 3 |
| `356` | 60g Bag | 3 |
| `278` | 20 kg Carboy | 3 |
| `204` | 150g Bag | 3 |
| `208` | 250ml Bottle | 3 |
| `137` | 50 kg Carton | 3 |
| `154` | 55 lb Carton | 2 |
| `554` | 24kg Drum | 2 |
| `465` | 450g Bottle | 2 |
| `200` | 44 lb Carton | 2 |
| `479` | 38.4lb Pail | 2 |
| `125` | 30 kg Drum | 2 |
| `546` | 0.5kg Bottle | 2 |
| `571` | 23kg Drum | 2 |
| `388` | 450lb Drum | 2 |
| `548` | 200g Bottle | 2 |
| `213` | 285kg Drum | 2 |
| `553` | 25kg Carboy | 2 |
| `558` | 1380kg Tote | 2 |
| `230` | 22.5 kg Carton | 2 |
| `167` | 1000 kg IBC | 2 |
| `535` | 570lb Drum | 2 |
| `294` | 200L Drum | 1 |
| `463` | 204kg Drum | 1 |
| `153` | 30lb Bag | 1 |
| `380` | (1,445 pieces) Carton | 1 |
| `350` | (1,250 pieces) Carton | 1 |
| `195` | 1 kg Box | 1 |
| `107` | 1 kg Carton | 1 |
| `460` | 450g Bag | 1 |
| `149` | 230 kg Drum | 1 |
| `151` | 6 kg Carton | 1 |
| `119` | 180 kg Drum | 1 |
| `390` | 45lb Carton | 1 |
| `197` | 30 lb Carton | 1 |
| `471` | 5lb Bottle | 1 |
| `342` | (4,500 pieces) Carton | 1 |
| `362` | 35lb Bag | 1 |
| `281` | 17kg Bag | 1 |
| `139` | 80lb Drum | 1 |
| `141` | 35 lb Drum | 1 |
| `484` | 3kg Bag | 1 |
| `552` | 25kg Pail | 1 |
| `432` | 16kg Pail | 1 |
| `381` | 170Kg Drum | 1 |
| `186` | 18kg Drum | 1 |
| `232` | 33lb Bag | 1 |
| `240` | 7kg Drum | 1 |
| `156` | 0.06kg Bag | 1 |
| `229` | 100g Bottle | 1 |
| `247` | 16kg Carton | 1 |
| `497` | 8.6kg Carton | 1 |
| `123` | 0.1 kg Tin | 1 |
| `127` | 50 kg Bag | 1 |
| `545` | 19kg Pail | 1 |
| `279` | 26kg Carboy | 1 |
| `572` | 5.5kg Jug | 1 |
| `261` | 100ml Bottle | 1 |
| `551` | 1700lb Bag | 1 |
| `210` | 600g Bag | 1 |
| `323` | 660lb Drum | 1 |
| `461` | 400g Bottle | 1 |
| `310` | 425lb Drum | 1 |
| `317` | 130g Bag | 1 |
| `248` | 300g Bottle | 1 |
| `387` | 290Kg Drum | 1 |
| `549` | 40kg Bag | 1 |
| `322` | 13kg Drum | 1 |
| `179` | 50g Bottle | 1 |
| `121` | 1kg Bottle | 1 |
| `343` | (30,000 pieces) Carton | 1 |
| `327` | 1Kg Drum | 1 |
| `177` | 0.3kg Bag | 1 |
| `301` | 715lb Drum | 1 |
| `302` | 3575lb IBC Totes | 1 |
| `303` | 825lb Drum | 1 |
| `304` | 3000lb IBC Totes | 1 |

### Filtering Recommendations

**Sample codes (< 1kg) to exclude:**
`142`, `160`, `124`, `181`, `143`, `413`, `420`, `201`, `148`, `163`, `215`, `178`, `108`, `220`, `287`, `356`, `204`, `166`, `225`, `578`, `465`, `546`, `548`, `460`, `156`, `229`, `123`, `261`, `317`, `248`, `179`, `177`, `221`, `367`, `258`, `297`, `208`, `280`

**Standard bulk codes (25kg metric):**
`100`, `101`, `102`

**All bulk codes (≥20kg or ≥40lb):**
`100`, `101`, `102`, `104`, `110`, `111`, `136`, `159`, `152`, `161`, `217`, `236`, `373`, `428`, `167`, `558`, `117`, `137`, `125`, `213`, `149`, `119`, `381`, `387`, `463`, `554`, `571`, `553`, `278`, `230`, `281`, `247`, `412`, `189`, `186`, `240`, `322`, `545`, `432`, `552`, `279`, `151`, `497`, `549`, `127`

## Inventory Data

### Multiple Variants Per Warehouse
The inventory API returns **one entry per variant per warehouse**. A product with 4 packaging options will return 4 inventory entries for each warehouse.

Example for Astragalus P.E. 50% (chino warehouse):
```
Code 100 (25kg Drum): 1125 kg
Code 201 (100g Bag):  300 kg
Code 142 (0.03kg):    0.3 kg
Code 160 (0.05kg):    0.25 kg
```

**Important**: When aggregating inventory, keep the highest quantity per warehouse to avoid sample sizes overwriting bulk inventory.

### Inventory Quantities Can Be Decimal
Some inventory quantities are decimal strings like `"0.09"`, `"0.27"`, `"1.5"`. Use `int(float(qty))` to parse.

### Warehouse Codes
| Code | Location |
|------|----------|
| `chino` | Chino, CA |
| `nj` | New Jersey |
| `sw` | Southwest |
| `edison` | Edison, NJ |

## Common Issues

### Headless Browser Detection
The site detects headless browsers. Use `headless=False` for Playwright.

### API Rate Limiting
The API may timeout under heavy load. Implement retries with backoff.

### Parent vs Variant SKU for Inventory
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
- **Grams field:** Direct grams field in JSON (no parsing needed like BS)

### Scraper Usage
```bash
# Full run
python boxnutra_scraper.py

# Limited test run
python boxnutra_scraper.py --max-products 50
```

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

### Scraper Usage
```bash
# Full run
python trafapharma_scraper.py

# Limited test run
python trafapharma_scraper.py --max-products 50

# Discovery only (list products without fetching details)
python trafapharma_scraper.py --discovery-only

# Resume from checkpoint
python trafapharma_scraper.py --resume
```

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

### Database
Uses same Supabase/SQLite database as IO and BS scrapers. Creates `TrafaPricing` table for flat data storage:
- `TrafaPricing` - Per-size pricing data

### Key Differences from IO, BS, and BN

| Aspect | IO | BS | BN | TrafaPharma |
|--------|----|----|----| ------------|
| Platform | Magento/GraphQL | Shopify JSON | Shopify JSON | Custom PHP |
| API | GraphQL | REST JSON | REST JSON | None (HTML) |
| Auth | Required | None | None | None |
| Pricing | Per-kg tiered | Per-package | Per-package | Per-size |
| Sizes | 25kg standard | Multiple (g to kg) | Multiple (g to kg) | Variable (10g to 25kg) |
| Inventory | Multi-warehouse | In stock/OOS | In stock/OOS (HTML) | Not available |
| Method | API queries | JSON fetch | JSON + HTML | HTML parse + POST |
