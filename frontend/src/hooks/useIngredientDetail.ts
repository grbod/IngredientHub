import { useQuery } from '@tanstack/react-query'
import { supabase } from '@/lib/supabase'

export interface PriceTier {
  vendor_ingredient_id: number
  vendor_id: number
  vendor_name: string
  sku: string | null
  packaging: string | null
  pack_size: number
  min_quantity: number | null
  price: number
  price_per_kg: number | null
}

export interface InventoryLevel {
  vendor_ingredient_id: number
  vendor_name: string
  sku: string | null
  warehouse: string
  quantity_available: number
  stock_status: string | null
}

export interface SimpleInventory {
  vendor_ingredient_id: number
  vendor_name: string
  sku: string | null
  stock_status: string | null
}

export interface IngredientDetail {
  ingredient_id: number
  name: string
  category_name: string | null
  priceTiers: PriceTier[]
  warehouseInventory: InventoryLevel[]
  simpleInventory: SimpleInventory[]
}

export function useIngredientDetail(ingredientId: number | undefined) {
  return useQuery({
    queryKey: ['ingredient-detail', ingredientId],
    queryFn: async (): Promise<IngredientDetail | null> => {
      if (!ingredientId) return null

      // Get ingredient info
      const { data: ingredient, error: ingError } = await supabase
        .from('ingredients')
        .select('ingredient_id, name, category_id')
        .eq('ingredient_id', ingredientId)
        .single()

      if (ingError) throw ingError
      if (!ingredient) return null

      // Get category name
      let categoryName: string | null = null
      if (ingredient.category_id) {
        const { data: cat } = await supabase
          .from('categories')
          .select('name')
          .eq('category_id', ingredient.category_id)
          .single()
        categoryName = cat?.name || null
      }

      // Get vendors
      const { data: vendors } = await supabase.from('vendors').select('vendor_id, name')
      const vendorMap = new Map(vendors?.map((v) => [v.vendor_id, v.name]) || [])

      // Get all variant IDs for this ingredient
      const { data: variants } = await supabase
        .from('ingredientvariants')
        .select('variant_id')
        .eq('ingredient_id', ingredientId)

      if (!variants || variants.length === 0) {
        return {
          ingredient_id: ingredient.ingredient_id,
          name: ingredient.name,
          category_name: categoryName,
          priceTiers: [],
          warehouseInventory: [],
          simpleInventory: [],
        }
      }

      const variantIds = variants.map((v) => v.variant_id)

      // Get vendor ingredients for these variants
      const { data: vendorIngredients } = await supabase
        .from('vendoringredients')
        .select('vendor_ingredient_id, vendor_id, variant_id, sku, status')
        .in('variant_id', variantIds)
        .or('status.eq.active,status.is.null')

      if (!vendorIngredients || vendorIngredients.length === 0) {
        return {
          ingredient_id: ingredient.ingredient_id,
          name: ingredient.name,
          category_name: categoryName,
          priceTiers: [],
          warehouseInventory: [],
          simpleInventory: [],
        }
      }

      const viIds = vendorIngredients.map((vi) => vi.vendor_ingredient_id)

      // Get packaging sizes
      const { data: packaging } = await supabase
        .from('packagingsizes')
        .select('vendor_ingredient_id, description, quantity')
        .in('vendor_ingredient_id', viIds)

      const packagingMap = new Map(
        packaging?.map((p) => [
          p.vendor_ingredient_id,
          { description: p.description, quantity: p.quantity },
        ]) || []
      )

      // Get price tiers
      const { data: priceTiers } = await supabase
        .from('pricetiers')
        .select('vendor_ingredient_id, min_quantity, price, price_per_kg')
        .in('vendor_ingredient_id', viIds)
        .order('min_quantity', { ascending: true })

      // Build price tier list
      const priceTierList: PriceTier[] = []
      for (const pt of priceTiers || []) {
        const vi = vendorIngredients.find(
          (v) => v.vendor_ingredient_id === pt.vendor_ingredient_id
        )
        if (!vi) continue

        const pkg = packagingMap.get(pt.vendor_ingredient_id)
        priceTierList.push({
          vendor_ingredient_id: pt.vendor_ingredient_id,
          vendor_id: vi.vendor_id,
          vendor_name: vendorMap.get(vi.vendor_id) || 'Unknown',
          sku: vi.sku,
          packaging: pkg?.description || null,
          pack_size: pkg?.quantity || 0,
          min_quantity: pt.min_quantity,
          price: pt.price,
          price_per_kg: pt.price_per_kg,
        })
      }

      // Get warehouse inventory (for IO - multi-warehouse)
      const { data: locations } = await supabase.from('locations').select('location_id, name')
      const locationMap = new Map(locations?.map((l) => [l.location_id, l.name]) || [])

      const { data: invLocations } = await supabase
        .from('inventorylocations')
        .select('inventory_location_id, vendor_ingredient_id, location_id')
        .in('vendor_ingredient_id', viIds)

      const warehouseInventory: InventoryLevel[] = []
      if (invLocations && invLocations.length > 0) {
        const invLocIds = invLocations.map((il) => il.inventory_location_id)
        const { data: invLevels } = await supabase
          .from('inventorylevels')
          .select('inventory_location_id, quantity_available, stock_status')
          .in('inventory_location_id', invLocIds)

        for (const il of invLocations) {
          const level = invLevels?.find(
            (lv) => lv.inventory_location_id === il.inventory_location_id
          )
          const vi = vendorIngredients.find(
            (v) => v.vendor_ingredient_id === il.vendor_ingredient_id
          )
          if (!vi || !level) continue

          warehouseInventory.push({
            vendor_ingredient_id: il.vendor_ingredient_id,
            vendor_name: vendorMap.get(vi.vendor_id) || 'Unknown',
            sku: vi.sku,
            warehouse: locationMap.get(il.location_id) || 'Unknown',
            quantity_available: level.quantity_available,
            stock_status: level.stock_status,
          })
        }
      }

      // Get simple inventory (for BS/BN/TP - in stock / out of stock)
      const { data: simpleInv } = await supabase
        .from('vendorinventory')
        .select('vendor_ingredient_id, stock_status')
        .in('vendor_ingredient_id', viIds)

      const simpleInventory: SimpleInventory[] = []
      for (const si of simpleInv || []) {
        const vi = vendorIngredients.find(
          (v) => v.vendor_ingredient_id === si.vendor_ingredient_id
        )
        if (!vi) continue

        simpleInventory.push({
          vendor_ingredient_id: si.vendor_ingredient_id,
          vendor_name: vendorMap.get(vi.vendor_id) || 'Unknown',
          sku: vi.sku,
          stock_status: si.stock_status,
        })
      }

      return {
        ingredient_id: ingredient.ingredient_id,
        name: ingredient.name,
        category_name: categoryName,
        priceTiers: priceTierList,
        warehouseInventory,
        simpleInventory,
      }
    },
    enabled: !!ingredientId,
  })
}
