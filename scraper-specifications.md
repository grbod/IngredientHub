# Ingredient Scraper Specifications

## Overview

Two scrapers feeding into unified ingredient database for B2B powder sourcing comparison.

---

## IngredientsOnline Scraper

### Data Source
- URL: `https://www.ingredientsonline.com/`
- Auth: Required (login)
- Model: B2B tiered pricing, 25kg drum increments, EXW buyer pays freight

---

### Current Fields (Keep As-Is)

These fields are already being captured correctly:

- `product_name` - Raw product name
- `product_sku` - Product SKU
- `variant_sku` - Variant SKU
- `tier_quantity` - Quantity threshold for price tier
- `price` - Price at this tier (already in $/kg)
- `original_price` - Pre-discount price if on sale
- `discount_percent` - Discount percentage
- `price_type` - "tiered" or "flat_rate"
- `url` - Full product URL
- `scraped_at` - ISO timestamp
- `inv_chino_qty` - Inventory quantity at Chino, CA
- `inv_chino_leadtime` - Lead time in days
- `inv_chino_eta` - Expected arrival date
- `inv_nj_qty` - Inventory quantity at Newark, NJ
- `inv_nj_leadtime` - Lead time in days
- `inv_nj_eta` - Expected arrival date
- `inv_sw_qty` - Inventory quantity at Southwest
- `inv_sw_leadtime` - Lead time in days
- `inv_sw_eta` - Expected arrival date

---

### New Fields to Add

#### Parsed from Existing Data

| Field | Source | How to Parse |
|-------|--------|--------------|
| `ingredient_name` | `product_name` | Remove " by {manufacturer}" suffix from end of string |
| `manufacturer` | `product_name` | Extract text after " by " at end of string |
| `category` | `url` | Extract first path segment after domain (e.g., `/botanicals/acerola/` → "botanicals") |
| `price_per_kg` | `price` | Copy directly - IO already quotes in $/kg |

**Parsing Examples:**

| product_name | ingredient_name | manufacturer |
|--------------|-----------------|--------------|
| "Acerola Cherry Extract 17% VC by Skyherb" | "Acerola Cherry Extract 17% VC" | "Skyherb" |
| "Vitamin C 99% USP by DSM" | "Vitamin C 99% USP" | "DSM" |
| "MCC 102 Granular by Sigachi" | "MCC 102 Granular" | "Sigachi" |

| url | category |
|-----|----------|
| `https://www.ingredientsonline.com/botanicals/acerola-cherry/` | "botanicals" |
| `https://www.ingredientsonline.com/amino-acids/l-carnitine/` | "amino-acids" |
| `https://www.ingredientsonline.com/excipients/mcc-102/` | "excipients" |

#### Hardcoded Values (IO Business Model)

These are constants - same for every IO product:

| Field | Value | Reason |
|-------|-------|--------|
| `order_rule_type` | "fixed_multiple" | IO sells in drum increments only |
| `order_rule_base_qty` | 25 | Must order in multiples of 25kg |
| `order_rule_unit` | "kg" | Base unit is kilograms |
| `packaging_size` | 25 | Standard drum size |
| `packaging_unit` | "kg" | Packaging measured in kg |
| `packaging_description` | "25kg Fiber Drum" | Standard packaging type |
| `shipping_responsibility` | "buyer" | IO is EXW - buyer arranges freight |
| `shipping_terms` | "EXW" | Ex Works pricing |

---

### Final CSV Column Order

```
product_name,ingredient_name,manufacturer,category,product_sku,variant_sku,tier_quantity,price,price_per_kg,original_price,discount_percent,price_type,order_rule_type,order_rule_base_qty,order_rule_unit,packaging_size,packaging_unit,packaging_description,shipping_responsibility,shipping_terms,url,scraped_at,inv_chino_qty,inv_chino_leadtime,inv_chino_eta,inv_nj_qty,inv_nj_leadtime,inv_nj_eta,inv_sw_qty,inv_sw_leadtime,inv_sw_eta
```

---

## BulkSupplements Scraper

### Data Source
- URL: `https://www.bulksupplements.com/`
- Auth: Not required (public Shopify API)
- Model: B2C per-package pricing, fixed pack sizes, shipping included

---

### Critical Change: Filter to Powder Only

**FILTER OUT all non-powder variants.** We only care about B2B bulk powder sourcing.

#### Filter Logic
- **INCLUDE:** `option1 == "Powder"` (case-insensitive)
- **EXCLUDE:** Everything else

#### Forms to Exclude
- Capsule / Capsules
- Softgel / Softgels
- Tablet / Tablets
- Liquid
- Veg Capsule / Veg Capsules
- Veggie Capsule / Veggie Capsules
- Gummy / Gummies
- Chewable / Chewables
- Lozenge / Lozenges

**Expected Result:** Your 221 rows should drop to ~179 powder-only rows.

---

### Current Fields to Keep

- `product_id` - Shopify product ID
- `product_title` - Raw product title
- `variant_id` - Shopify variant ID
- `variant_sku` - SKU
- `variant_barcode` - UPC/EAN barcode
- `price` - Variant price
- `compare_at_price` - Original price if on sale
- `in_stock` - Boolean availability
- `product_type` - Category (rename to `category` in output)
- `product_url` - Product URL (rename to `url` in output)
- `scraped_at` - ISO timestamp

---

### Fields to Remove

These are not needed for B2B powder sourcing:

- `product_handle` - URL slug, redundant with full URL
- `vendor` - Always "BulkSupplements.com"
- `variant_title` - Redundant with option1 + option2
- `option1` - Used for filtering, not needed in output after filter applied
- `option3` - Never populated
- `weight` - Not useful for comparison
- `weight_unit` - Not useful for comparison

---

### New Fields to Add

#### Parsed from Existing Data

| Field | Source | How to Parse |
|-------|--------|--------------|
| `ingredient_name` | `product_title` | Clean/trim whitespace (keep full name for now) |
| `category` | `product_type` | Rename field |
| `pack_size_g` | `option2` | Parse to grams (see conversion table below) |
| `pack_size_description` | `option2` | Keep raw value |
| `price_per_kg` | Calculated | Formula: `(price / pack_size_g) * 1000` |
| `stock_status` | `in_stock` | Convert: True → "in_stock", False → "out_of_stock" |

#### Pack Size Parsing (option2 → grams)

| option2 Value | pack_size_g |
|---------------|-------------|
| "10 Grams (0.35 oz)" | 10 |
| "25 Grams (0.88 oz)" | 25 |
| "50 Grams (1.76 oz)" | 50 |
| "100 Grams (3.5 oz)" | 100 |
| "250 Grams (8.8 oz)" | 250 |
| "500 Grams (1.1 lbs)" | 500 |
| "700 Grams (1.5 lbs)" | 700 |
| "1 Kilogram (2.2 lbs)" | 1000 |
| "5 Kilograms (11 lbs)" | 5000 |
| "20 Kilograms (44 lbs)" | 20000 |
| "25 Kilograms (55 lbs)" | 25000 |

**Parsing Rule:**
1. If contains "Kilogram" → extract number, multiply by 1000
2. If contains "Gram" → extract number directly

#### Price Per Kg Calculation Examples

| price | pack_size_g | price_per_kg | Calculation |
|-------|-------------|--------------|-------------|
| $15.97 | 100 | $159.70 | (15.97 / 100) × 1000 |
| $19.97 | 250 | $79.88 | (19.97 / 250) × 1000 |
| $28.97 | 500 | $57.94 | (28.97 / 500) × 1000 |
| $38.97 | 1000 | $38.97 | (38.97 / 1000) × 1000 |
| $450.00 | 25000 | $18.00 | (450.00 / 25000) × 1000 |

#### Hardcoded Values (BS Business Model)

| Field | Value | Reason |
|-------|-------|--------|
| `order_rule_type` | "fixed_pack" | BS sells specific pack sizes only |
| `shipping_responsibility` | "vendor" | BS includes free shipping |

---

### Final CSV Column Order

```
product_id,product_title,ingredient_name,category,variant_id,variant_sku,variant_barcode,pack_size_g,pack_size_description,price,compare_at_price,price_per_kg,in_stock,stock_status,order_rule_type,shipping_responsibility,url,scraped_at
```

---

## Summary of Changes

### IngredientsOnline

| Action | Field(s) |
|--------|----------|
| **ADD (parsed)** | `ingredient_name`, `manufacturer`, `category`, `price_per_kg` |
| **ADD (hardcoded)** | `order_rule_type`, `order_rule_base_qty`, `order_rule_unit`, `packaging_size`, `packaging_unit`, `packaging_description`, `shipping_responsibility`, `shipping_terms` |
| **KEEP** | All existing fields |
| **REMOVE** | None |

### BulkSupplements

| Action | Field(s) |
|--------|----------|
| **FILTER** | Keep only rows where `option1 == "Powder"` |
| **ADD (parsed)** | `ingredient_name`, `pack_size_g`, `pack_size_description`, `price_per_kg`, `stock_status` |
| **ADD (hardcoded)** | `order_rule_type`, `shipping_responsibility` |
| **RENAME** | `product_type` → `category`, `product_url` → `url` |
| **REMOVE** | `product_handle`, `vendor`, `variant_title`, `option1`, `option2`, `option3`, `weight`, `weight_unit` |

---

## Database Import Mapping Reference

### IngredientsOnline → Database

| CSV Column | Database Table.Column |
|------------|----------------------|
| `ingredient_name` | `Ingredients.name` |
| `manufacturer` | `Manufacturers.name` |
| `category` | `Categories.name` |
| `product_sku` | `VendorIngredients.sku` |
| `price` | `PriceTiers.price` |
| `price_per_kg` | `PriceTiers.price_per_kg` |
| `tier_quantity` | `PriceTiers.min_quantity` |
| `price_type` | `PriceTiers.pricing_model_id` (tiered→3, flat_rate→1) |
| `order_rule_type` | `OrderRules.rule_type_id` (fixed_multiple→1) |
| `order_rule_base_qty` | `OrderRules.base_quantity` |
| `packaging_size` | `PackagingSizes.quantity` |
| `packaging_description` | `PackagingSizes.description` |
| `shipping_responsibility` | `VendorIngredients.shipping_responsibility` |
| `shipping_terms` | `VendorIngredients.shipping_terms` |
| `url` | `ScrapeSources.product_url` |
| `scraped_at` | `ScrapeSources.scraped_at` |
| `inv_*_qty` | `InventoryLevels.quantity_available` |
| `inv_*_leadtime` | `InventoryLevels.lead_time_days` |
| `inv_*_eta` | `InventoryLevels.expected_arrival` |

### BulkSupplements → Database

| CSV Column | Database Table.Column |
|------------|----------------------|
| `ingredient_name` | `Ingredients.name` |
| `category` | `Categories.name` |
| `variant_sku` | `VendorIngredients.sku` |
| `variant_barcode` | `VendorIngredients.barcode` |
| `price` | `PriceTiers.price` |
| `price_per_kg` | `PriceTiers.price_per_kg` |
| `pack_size_g` | `PackagingSizes.quantity` (divide by 1000 for kg) |
| `pack_size_description` | `PackagingSizes.description` |
| `order_rule_type` | `OrderRules.rule_type_id` (fixed_pack→2) |
| `stock_status` | `InventoryLevels.stock_status` |
| `shipping_responsibility` | `VendorIngredients.shipping_responsibility` |
| `url` | `ScrapeSources.product_url` |
| `scraped_at` | `ScrapeSources.scraped_at` |
