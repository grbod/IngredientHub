import { useQuery } from '@tanstack/react-query'
import { supabase } from '@/lib/supabase'

export function useVendors() {
  return useQuery({
    queryKey: ['vendors'],
    queryFn: async () => {
      const { data, error } = await supabase
        .from('vendors')
        .select('*')
        .order('name')

      if (error) throw error
      return data
    },
  })
}

export function useVendorStats() {
  return useQuery({
    queryKey: ['vendor-stats'],
    queryFn: async () => {
      const { data: vendors, error: vendorError } = await supabase
        .from('vendors')
        .select('*')

      if (vendorError) throw vendorError

      const stats = await Promise.all(
        vendors.map(async (vendor) => {
          // Get exact count of vendor_ingredients (size variants) using count: 'exact'
          // This bypasses the 1000 row default limit
          const { count: variantCount, error: vcError } = await supabase
            .from('vendoringredients')
            .select('*', { count: 'exact', head: true })
            .eq('vendor_id', vendor.vendor_id)

          if (vcError) throw vcError

          // Get distinct variant_ids (products) - need pagination for vendors with > 1000 rows
          let allVariantIds: number[] = []
          let from = 0
          const batchSize = 1000

          while (true) {
            const { data, error } = await supabase
              .from('vendoringredients')
              .select('variant_id')
              .eq('vendor_id', vendor.vendor_id)
              .range(from, from + batchSize - 1)

            if (error) throw error
            if (!data || data.length === 0) break

            allVariantIds.push(...data.map((vi) => vi.variant_id))

            if (data.length < batchSize) break
            from += batchSize
          }

          const uniqueVariants = new Set(allVariantIds)
          const productCount = uniqueVariants.size

          const { data: lastScrape } = await supabase
            .from('scrapesources')
            .select('scraped_at')
            .eq('vendor_id', vendor.vendor_id)
            .order('scraped_at', { ascending: false })
            .limit(1)
            .single()

          return {
            ...vendor,
            productCount,
            variantCount: variantCount || 0,
            lastScraped: lastScrape?.scraped_at || null,
          }
        })
      )

      return stats
    },
  })
}
