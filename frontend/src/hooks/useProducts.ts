import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { VendorIngredientData } from '@/lib/api'

export type ProductWithDetails = VendorIngredientData

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
      const result = await api.getVendorIngredients({ vendorId, search, limit, offset })
      return { data: result.data as ProductWithDetails[], count: result.data.length }
    },
  })
}

export function useProductCount(vendorId?: number) {
  return useQuery({
    queryKey: ['product-count', vendorId],
    queryFn: async () => {
      const result = await api.getVendorIngredients({ vendorId, limit: 1, offset: 0 })
      return result.total
    },
  })
}
