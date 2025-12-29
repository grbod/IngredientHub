import { useQuery } from '@tanstack/react-query'
import { supabase } from '@/lib/supabase'

export interface ProductWithDetails {
  vendor_ingredient_id: number
  sku: string | null
  raw_product_name: string | null
  status: string | null
  last_seen_at: string | null
  vendor: {
    vendor_id: number
    name: string
  }
  variant: {
    variant_id: number
    variant_name: string
    ingredient: {
      ingredient_id: number
      name: string
    }
  }
  price_tiers: {
    price: number
    price_per_kg: number | null
    min_quantity: number | null
  }[]
}

interface UseProductsOptions {
  vendorId?: number
  search?: string
  limit?: number
  offset?: number
}

export function useProducts(options: UseProductsOptions = {}) {
  const { vendorId, search, limit = 100, offset = 0 } = options

  return useQuery({
    queryKey: ['products', { vendorId, search, limit, offset }],
    queryFn: async () => {
      let query = supabase
        .from('vendoringredients')
        .select(`
          vendor_ingredient_id,
          sku,
          raw_product_name,
          status,
          last_seen_at,
          vendor:vendors!inner(vendor_id, name),
          variant:ingredientvariants!inner(
            variant_id,
            variant_name,
            ingredient:ingredients!inner(ingredient_id, name)
          ),
          price_tiers:pricetiers(price, price_per_kg, min_quantity)
        `)
        .order('last_seen_at', { ascending: false })
        .range(offset, offset + limit - 1)

      if (vendorId) {
        query = query.eq('vendor_id', vendorId)
      }

      if (search) {
        query = query.or(`raw_product_name.ilike.%${search}%,sku.ilike.%${search}%`)
      }

      const { data, error, count } = await query

      if (error) throw error
      return { data: data as unknown as ProductWithDetails[], count }
    },
  })
}

export function useProductCount(vendorId?: number) {
  return useQuery({
    queryKey: ['product-count', vendorId],
    queryFn: async () => {
      let query = supabase
        .from('vendoringredients')
        .select('*', { count: 'exact', head: true })

      if (vendorId) {
        query = query.eq('vendor_id', vendorId)
      }

      const { count, error } = await query
      if (error) throw error
      return count || 0
    },
  })
}
