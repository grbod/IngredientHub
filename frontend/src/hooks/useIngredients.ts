import { useQuery } from '@tanstack/react-query'
import { supabase } from '@/lib/supabase'

export interface Ingredient {
  ingredient_id: number
  name: string
  category_name: string | null
  status: string | null
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

      const enrichedIngredients = (ingredients || []).map((i) => ({
        ingredient_id: i.ingredient_id,
        name: i.name,
        category_name: i.category_id ? categoryMap.get(i.category_id) || null : null,
        status: i.status,
      }))

      return { data: enrichedIngredients as Ingredient[], count: ingredients?.length || 0 }
    },
  })
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
