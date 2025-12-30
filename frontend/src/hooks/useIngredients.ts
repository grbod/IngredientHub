import { useQuery } from '@tanstack/react-query'
import { supabase } from '@/lib/supabase'

export interface Ingredient {
  ingredient_id: number
  name: string
  category_name: string | null
  status: string | null
  vendors: string[]
}

interface UseIngredientsOptions {
  search?: string
  limit?: number
  offset?: number
}

export function useIngredients(options: UseIngredientsOptions = {}) {
  const { search, limit = 50, offset = 0 } = options

  return useQuery({
    queryKey: ['ingredients', { search, limit, offset }],
    queryFn: async () => {
      // First get ingredients
      let query = supabase
        .from('ingredients')
        .select('ingredient_id, name, category_id, status')
        .order('name', { ascending: true })
        .range(offset, offset + limit - 1)

      if (search) {
        query = query.ilike('name', `%${search}%`)
      }

      const { data: ingredients, error } = await query

      if (error) throw error

      // Get categories
      const { data: categories } = await supabase
        .from('categories')
        .select('category_id, name')

      const categoryMap = new Map(
        categories?.map((c) => [c.category_id, c.name]) || []
      )

      // Get ingredient IDs for current page
      const ingredientIds = (ingredients || []).map((i) => i.ingredient_id)

      // Get vendors for these ingredients
      const ingredientVendors = await fetchVendorsForIngredients(ingredientIds)

      const enrichedIngredients = (ingredients || []).map((i) => ({
        ingredient_id: i.ingredient_id,
        name: i.name,
        category_name: i.category_id ? categoryMap.get(i.category_id) || null : null,
        status: i.status,
        vendors: ingredientVendors.get(i.ingredient_id) || [],
      }))

      return { data: enrichedIngredients as Ingredient[], count: ingredients?.length || 0 }
    },
  })
}

async function fetchVendorsForIngredients(
  ingredientIds: number[]
): Promise<Map<number, string[]>> {
  if (ingredientIds.length === 0) {
    return new Map()
  }

  // Get variants for these ingredients
  const { data: variants } = await supabase
    .from('ingredientvariants')
    .select('ingredient_id, variant_id')
    .in('ingredient_id', ingredientIds)

  if (!variants || variants.length === 0) {
    return new Map()
  }

  const variantIds = variants.map((v) => v.variant_id)

  // Get vendor ingredients for these variants
  const { data: vendorIngredients } = await supabase
    .from('vendoringredients')
    .select('variant_id, vendor_id')
    .in('variant_id', variantIds)

  if (!vendorIngredients || vendorIngredients.length === 0) {
    return new Map()
  }

  // Get all vendors
  const { data: vendors } = await supabase
    .from('vendors')
    .select('vendor_id, name')

  const vendorMap = new Map(vendors?.map((v) => [v.vendor_id, v.name]) || [])

  // Build variant -> ingredient lookup
  const variantToIngredient = new Map(
    variants.map((v) => [v.variant_id, v.ingredient_id])
  )

  // Build ingredient -> vendors map
  const ingredientVendors = new Map<number, Set<string>>()
  for (const vi of vendorIngredients) {
    const ingredientId = variantToIngredient.get(vi.variant_id)
    if (!ingredientId) continue

    const vendorName = vendorMap.get(vi.vendor_id)
    if (!vendorName) continue

    if (!ingredientVendors.has(ingredientId)) {
      ingredientVendors.set(ingredientId, new Set())
    }
    ingredientVendors.get(ingredientId)!.add(vendorName)
  }

  // Convert Sets to sorted arrays
  const result = new Map<number, string[]>()
  for (const [ingredientId, vendorSet] of ingredientVendors) {
    result.set(ingredientId, Array.from(vendorSet).sort())
  }

  return result
}

export function useIngredientCount() {
  return useQuery({
    queryKey: ['ingredient-count'],
    queryFn: async () => {
      const { count, error } = await supabase
        .from('ingredients')
        .select('*', { count: 'exact', head: true })

      if (error) throw error
      return count || 0
    },
  })
}
