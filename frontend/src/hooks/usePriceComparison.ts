import { useQuery } from '@tanstack/react-query'
import { supabase } from '@/lib/supabase'

export interface PriceComparisonItem {
  ingredient_name: string
  ingredient_id: number
  vendors: {
    vendor_id: number
    vendor_name: string
    sku: string | null
    product_name: string | null
    best_price_per_kg: number | null
    min_order_qty: number | null
    last_seen: string | null
  }[]
}

interface VendorIngredientRow {
  vendor_ingredient_id: number
  sku: string | null
  raw_product_name: string | null
  last_seen_at: string | null
  vendor: { vendor_id: number; name: string } | null
  price_tiers: { price: number; price_per_kg: number | null; min_quantity: number | null }[]
}

interface VariantRow {
  variant_id: number
  vendor_ingredients: VendorIngredientRow[]
}

interface IngredientRow {
  ingredient_id: number
  name: string
  variants: VariantRow[]
}

export function usePriceComparison(search?: string) {
  return useQuery({
    queryKey: ['price-comparison', search],
    queryFn: async () => {
      let query = supabase
        .from('ingredients')
        .select(`
          ingredient_id,
          name,
          variants:ingredientvariants(
            variant_id,
            vendor_ingredients:vendoringredients(
              vendor_ingredient_id,
              sku,
              raw_product_name,
              last_seen_at,
              vendor:vendors(vendor_id, name),
              price_tiers:pricetiers(price, price_per_kg, min_quantity)
            )
          )
        `)
        .order('name')
        .limit(100)

      if (search) {
        query = query.ilike('name', `%${search}%`)
      }

      const { data, error } = await query

      if (error) throw error

      // Transform data into comparison format
      const comparisons: PriceComparisonItem[] = []
      const ingredients = data as unknown as IngredientRow[]

      for (const ingredient of ingredients || []) {
        const vendorMap = new Map<number, PriceComparisonItem['vendors'][0]>()

        for (const variant of ingredient.variants || []) {
          for (const vi of variant.vendor_ingredients || []) {
            const vendor = vi.vendor
            if (!vendor) continue

            const priceTiers = vi.price_tiers || []
            const bestPrice = priceTiers.reduce((best, tier) => {
              if (!tier.price_per_kg) return best
              if (!best || tier.price_per_kg < best) return tier.price_per_kg
              return best
            }, null as number | null)

            const minQty = priceTiers[0]?.min_quantity || null

            const existing = vendorMap.get(vendor.vendor_id)
            if (!existing || (bestPrice && (!existing.best_price_per_kg || bestPrice < existing.best_price_per_kg))) {
              vendorMap.set(vendor.vendor_id, {
                vendor_id: vendor.vendor_id,
                vendor_name: vendor.name,
                sku: vi.sku,
                product_name: vi.raw_product_name,
                best_price_per_kg: bestPrice,
                min_order_qty: minQty,
                last_seen: vi.last_seen_at,
              })
            }
          }
        }

        if (vendorMap.size > 0) {
          comparisons.push({
            ingredient_name: ingredient.name,
            ingredient_id: ingredient.ingredient_id,
            vendors: Array.from(vendorMap.values()).sort((a, b) => {
              if (!a.best_price_per_kg) return 1
              if (!b.best_price_per_kg) return -1
              return a.best_price_per_kg - b.best_price_per_kg
            }),
          })
        }
      }

      return comparisons
    },
  })
}
