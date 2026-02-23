import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { PriceComparisonData } from '@/lib/api'

export type PriceComparisonItem = PriceComparisonData

export function usePriceComparison(search?: string) {
  return useQuery({
    queryKey: ['price-comparison', search],
    queryFn: async () => {
      return api.getPriceComparison({ search })
    },
  })
}
