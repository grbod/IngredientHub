import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { CategoryData } from '@/lib/api'

export type Category = CategoryData

export function useCategories() {
  return useQuery({
    queryKey: ['categories'],
    queryFn: async () => {
      return api.getCategories()
    },
  })
}
