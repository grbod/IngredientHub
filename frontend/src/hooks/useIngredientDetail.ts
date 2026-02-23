import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { PriceTierData, InventoryLevelData, SimpleInventoryData, IngredientDetailData } from '@/lib/api'

export type PriceTier = PriceTierData
export type InventoryLevel = InventoryLevelData
export type SimpleInventory = SimpleInventoryData
export type IngredientDetail = IngredientDetailData

export function useIngredientDetail(ingredientId: number | undefined) {
  return useQuery({
    queryKey: ['ingredient-detail', ingredientId],
    queryFn: async (): Promise<IngredientDetail | null> => {
      if (!ingredientId) return null
      return api.getIngredientDetail(ingredientId)
    },
    enabled: !!ingredientId,
  })
}
