import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { IngredientData } from '@/lib/api'

export type StockStatus = 'in_stock' | 'out_of_stock' | 'unknown'

export type Ingredient = IngredientData

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
      const result = await api.getIngredients({ search, limit, offset })
      return { data: result.data as Ingredient[], count: result.data.length }
    },
  })
}

export function useIngredientCount() {
  return useQuery({
    queryKey: ['ingredient-count'],
    queryFn: async () => {
      const result = await api.getIngredients({ limit: 1, offset: 0 })
      return result.total
    },
  })
}
