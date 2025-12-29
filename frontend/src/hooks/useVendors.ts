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
          const { count: productCount } = await supabase
            .from('vendoringredients')
            .select('*', { count: 'exact', head: true })
            .eq('vendor_id', vendor.vendor_id)

          const { data: lastScrape } = await supabase
            .from('scrapesources')
            .select('scraped_at')
            .eq('vendor_id', vendor.vendor_id)
            .order('scraped_at', { ascending: false })
            .limit(1)
            .single()

          return {
            ...vendor,
            productCount: productCount || 0,
            lastScraped: lastScrape?.scraped_at || null,
          }
        })
      )

      return stats
    },
  })
}
