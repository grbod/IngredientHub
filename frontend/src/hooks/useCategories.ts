import { useQuery } from '@tanstack/react-query'
import { supabase } from '@/lib/supabase'

export interface Category {
  category_id: number
  name: string
  description: string | null
}

export function useCategories() {
  return useQuery({
    queryKey: ['categories'],
    queryFn: async () => {
      const { data, error } = await supabase
        .from('categories')
        .select('category_id, name, description')
        .order('name', { ascending: true })

      if (error) throw error
      return data as Category[]
    },
  })
}
