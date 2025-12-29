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

export function usePriceComparison(search?: string) {
  return useQuery({
    queryKey: ['price-comparison', search],
    queryFn: async () => {
      // Step 1: Get ingredients (filtered by search)
      let ingredientsQuery = supabase
        .from('ingredients')
        .select('ingredient_id, name')
        .order('name')
        .limit(50)

      if (search) {
        ingredientsQuery = ingredientsQuery.ilike('name', `%${search}%`)
      }

      const { data: ingredients, error: ingError } = await ingredientsQuery
      if (ingError) throw ingError
      if (!ingredients || ingredients.length === 0) return []

      const ingredientIds = ingredients.map(i => i.ingredient_id)

      // Step 2: Get variants for these ingredients
      const { data: variants } = await supabase
        .from('ingredientvariants')
        .select('variant_id, ingredient_id')
        .in('ingredient_id', ingredientIds)

      if (!variants || variants.length === 0) return []

      const variantIds = variants.map(v => v.variant_id)
      const variantToIngredient = new Map(variants.map(v => [v.variant_id, v.ingredient_id]))

      // Step 3: Get vendor ingredients for these variants
      const { data: vendorIngredients } = await supabase
        .from('vendoringredients')
        .select('vendor_ingredient_id, vendor_id, variant_id, sku, raw_product_name, last_seen_at')
        .in('variant_id', variantIds)
        .or('status.eq.active,status.is.null')

      if (!vendorIngredients || vendorIngredients.length === 0) return []

      const viIds = vendorIngredients.map(vi => vi.vendor_ingredient_id)

      // Step 4: Get vendors and price tiers in parallel
      const [vendorsRes, priceTiersRes] = await Promise.all([
        supabase.from('vendors').select('vendor_id, name'),
        supabase.from('pricetiers').select('vendor_ingredient_id, price, price_per_kg, min_quantity').in('vendor_ingredient_id', viIds)
      ])

      const vendors = vendorsRes.data || []
      const priceTiers = priceTiersRes.data || []

      const vendorMap = new Map(vendors.map(v => [v.vendor_id, v.name]))

      // Group price tiers by vendor_ingredient_id
      const tiersByVi = new Map<number, typeof priceTiers>()
      for (const tier of priceTiers) {
        if (!tiersByVi.has(tier.vendor_ingredient_id)) {
          tiersByVi.set(tier.vendor_ingredient_id, [])
        }
        tiersByVi.get(tier.vendor_ingredient_id)!.push(tier)
      }

      // Build comparison data
      const ingredientData = new Map<number, PriceComparisonItem>()

      for (const ing of ingredients) {
        ingredientData.set(ing.ingredient_id, {
          ingredient_id: ing.ingredient_id,
          ingredient_name: ing.name,
          vendors: []
        })
      }

      // Group vendor data by ingredient
      const vendorByIngredient = new Map<number, Map<number, PriceComparisonItem['vendors'][0]>>()

      for (const vi of vendorIngredients) {
        const ingredientId = variantToIngredient.get(vi.variant_id)
        if (!ingredientId) continue

        if (!vendorByIngredient.has(ingredientId)) {
          vendorByIngredient.set(ingredientId, new Map())
        }

        const vendorData = vendorByIngredient.get(ingredientId)!
        const tiers = tiersByVi.get(vi.vendor_ingredient_id) || []

        // Find best price per kg
        const bestPrice = tiers.reduce((best, tier) => {
          if (!tier.price_per_kg) return best
          if (!best || tier.price_per_kg < best) return tier.price_per_kg
          return best
        }, null as number | null)

        const minQty = tiers[0]?.min_quantity || null

        const existing = vendorData.get(vi.vendor_id)
        if (!existing || (bestPrice && (!existing.best_price_per_kg || bestPrice < existing.best_price_per_kg))) {
          vendorData.set(vi.vendor_id, {
            vendor_id: vi.vendor_id,
            vendor_name: vendorMap.get(vi.vendor_id) || 'Unknown',
            sku: vi.sku,
            product_name: vi.raw_product_name,
            best_price_per_kg: bestPrice,
            min_order_qty: minQty,
            last_seen: vi.last_seen_at,
          })
        }
      }

      // Build final result
      const comparisons: PriceComparisonItem[] = []

      for (const [ingredientId, vendorData] of vendorByIngredient) {
        const ing = ingredientData.get(ingredientId)
        if (!ing) continue

        const vendorsList = Array.from(vendorData.values()).sort((a, b) => {
          if (!a.best_price_per_kg) return 1
          if (!b.best_price_per_kg) return -1
          return a.best_price_per_kg - b.best_price_per_kg
        })

        if (vendorsList.length > 0) {
          comparisons.push({
            ingredient_id: ingredientId,
            ingredient_name: ing.ingredient_name,
            vendors: vendorsList,
          })
        }
      }

      // Sort by ingredient name
      comparisons.sort((a, b) => a.ingredient_name.localeCompare(b.ingredient_name))

      return comparisons
    },
  })
}
