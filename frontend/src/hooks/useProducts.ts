import { useQuery } from '@tanstack/react-query'
import { supabase } from '@/lib/supabase'

export interface ProductWithDetails {
  vendor_ingredient_id: number
  sku: string | null
  raw_product_name: string | null
  status: string | null
  last_seen_at: string | null
  vendor_id: number
  vendor_name?: string
}

interface UseProductsOptions {
  vendorId?: number
  search?: string
  limit?: number
  offset?: number
}

export function useProducts(options: UseProductsOptions = {}) {
  const { vendorId, search, limit = 50, offset = 0 } = options

  return useQuery({
    queryKey: ['products', { vendorId, search, limit, offset }],
    queryFn: async () => {
      // First get products
      let query = supabase
        .from('vendoringredients')
        .select('vendor_ingredient_id, sku, raw_product_name, status, last_seen_at, vendor_id')
        .order('last_seen_at', { ascending: false, nullsFirst: false })
        .range(offset, offset + limit - 1)

      if (vendorId) {
        query = query.eq('vendor_id', vendorId)
      }

      if (search) {
        query = query.or(`raw_product_name.ilike.%${search}%,sku.ilike.%${search}%`)
      }

      const { data: products, error } = await query

      if (error) throw error

      // Get vendor names
      const { data: vendors } = await supabase.from('vendors').select('vendor_id, name')
      const vendorMap = new Map(vendors?.map(v => [v.vendor_id, v.name]) || [])

      const enrichedProducts = (products || []).map(p => ({
        ...p,
        vendor_name: vendorMap.get(p.vendor_id) || 'Unknown'
      }))

      return { data: enrichedProducts as ProductWithDetails[], count: products?.length || 0 }
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
