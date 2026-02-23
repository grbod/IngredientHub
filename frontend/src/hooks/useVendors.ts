import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'

export function useVendors() {
  return useQuery({
    queryKey: ['vendors'],
    queryFn: async () => {
      return api.getVendors()
    },
  })
}

export function useVendorStats() {
  return useQuery({
    queryKey: ['vendor-stats'],
    queryFn: async () => {
      return api.getVendorStats()
    },
  })
}
