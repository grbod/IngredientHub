import { useQuery } from '@tanstack/react-query'
import { supabase } from '@/lib/supabase'

export type StockStatus = 'in_stock' | 'out_of_stock' | 'unknown'

export interface Ingredient {
  ingredient_id: number
  name: string
  category_name: string | null
  category_id: number | null
  status: string | null
  vendors: string[]
  vendor_count: number
  stock_status: StockStatus
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

      // Get vendors and stock status for these ingredients
      const ingredientInfo = await fetchVendorsAndStockForIngredients(ingredientIds)

      const enrichedIngredients = (ingredients || []).map((i) => {
        const info = ingredientInfo.get(i.ingredient_id)
        return {
          ingredient_id: i.ingredient_id,
          name: i.name,
          category_name: i.category_id ? categoryMap.get(i.category_id) || null : null,
          category_id: i.category_id,
          status: i.status,
          vendors: info?.vendors || [],
          vendor_count: info?.vendors.length || 0,
          stock_status: info?.stock_status || 'unknown' as const,
        }
      })

      return { data: enrichedIngredients as Ingredient[], count: ingredients?.length || 0 }
    },
  })
}

interface VendorStockInfo {
  vendors: string[]
  stock_status: 'in_stock' | 'out_of_stock' | 'unknown'
}

async function fetchVendorsAndStockForIngredients(
  ingredientIds: number[]
): Promise<Map<number, VendorStockInfo>> {
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
    .select('vendor_ingredient_id, variant_id, vendor_id')
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

  // Get vendor_ingredient_ids for stock lookup
  const viIds = vendorIngredients.map((vi) => vi.vendor_ingredient_id)

  // Get simple inventory (BS/BN/TP - in stock / out of stock)
  const { data: simpleInv } = await supabase
    .from('vendorinventory')
    .select('vendor_ingredient_id, stock_status')
    .in('vendor_ingredient_id', viIds)

  // Get warehouse inventory levels (IO - multi-warehouse)
  const { data: invLocations } = await supabase
    .from('inventorylocations')
    .select('inventory_location_id, vendor_ingredient_id')
    .in('vendor_ingredient_id', viIds)

  let warehouseStockMap = new Map<number, boolean>()
  if (invLocations && invLocations.length > 0) {
    const invLocIds = invLocations.map((il) => il.inventory_location_id)
    const { data: invLevels } = await supabase
      .from('inventorylevels')
      .select('inventory_location_id, stock_status, quantity_available')
      .in('inventory_location_id', invLocIds)

    // Map inventory_location_id to vendor_ingredient_id
    const locToVi = new Map(
      invLocations.map((il) => [il.inventory_location_id, il.vendor_ingredient_id])
    )

    for (const level of invLevels || []) {
      const viId = locToVi.get(level.inventory_location_id)
      if (!viId) continue
      // If any location has stock, mark as in_stock
      if (level.stock_status === 'in_stock' || (level.quantity_available && level.quantity_available > 0)) {
        warehouseStockMap.set(viId, true)
      } else if (!warehouseStockMap.has(viId)) {
        warehouseStockMap.set(viId, false)
      }
    }
  }

  // Build simple inventory map
  const simpleStockMap = new Map<number, string>(
    simpleInv?.map((si) => [si.vendor_ingredient_id, si.stock_status || 'unknown']) || []
  )

  // Build vendor_ingredient_id -> ingredient_id lookup
  const viToIngredient = new Map<number, number>()
  for (const vi of vendorIngredients) {
    const ingredientId = variantToIngredient.get(vi.variant_id)
    if (ingredientId) {
      viToIngredient.set(vi.vendor_ingredient_id, ingredientId)
    }
  }

  // Build ingredient -> vendors map and aggregate stock status
  const ingredientVendors = new Map<number, Set<string>>()
  const ingredientStockStatus = new Map<number, 'in_stock' | 'out_of_stock' | 'unknown'>()

  for (const vi of vendorIngredients) {
    const ingredientId = variantToIngredient.get(vi.variant_id)
    if (!ingredientId) continue

    const vendorName = vendorMap.get(vi.vendor_id)
    if (!vendorName) continue

    if (!ingredientVendors.has(ingredientId)) {
      ingredientVendors.set(ingredientId, new Set())
    }
    ingredientVendors.get(ingredientId)!.add(vendorName)

    // Check stock status for this vendor_ingredient
    const simpleStock = simpleStockMap.get(vi.vendor_ingredient_id)
    const hasWarehouseStock = warehouseStockMap.get(vi.vendor_ingredient_id)

    let viStock: 'in_stock' | 'out_of_stock' | 'unknown' = 'unknown'
    if (simpleStock === 'in_stock' || hasWarehouseStock === true) {
      viStock = 'in_stock'
    } else if (simpleStock === 'out_of_stock' || hasWarehouseStock === false) {
      viStock = 'out_of_stock'
    }

    // Aggregate: if any vendor has stock, ingredient is in_stock
    const currentStatus = ingredientStockStatus.get(ingredientId) || 'unknown'
    if (viStock === 'in_stock') {
      ingredientStockStatus.set(ingredientId, 'in_stock')
    } else if (viStock === 'out_of_stock' && currentStatus !== 'in_stock') {
      ingredientStockStatus.set(ingredientId, 'out_of_stock')
    }
    // Leave as 'unknown' if no stock info
  }

  // Convert Sets to sorted arrays and combine with stock status
  const result = new Map<number, VendorStockInfo>()
  for (const [ingredientId, vendorSet] of ingredientVendors) {
    result.set(ingredientId, {
      vendors: Array.from(vendorSet).sort(),
      stock_status: ingredientStockStatus.get(ingredientId) || 'unknown',
    })
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
