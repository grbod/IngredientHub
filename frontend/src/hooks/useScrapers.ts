import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  api,
  type ScraperStatus,
  type CronSuggestion,
  type TriggerScraperOptions,
  type TriggerScraperResponse,
} from '@/lib/api'

/**
 * Poll scraper status for a vendor (refreshes every 5 seconds when enabled)
 */
export function useScraperStatus(vendorId: number | null) {
  return useQuery({
    queryKey: ['scraper-status', vendorId],
    queryFn: async () => {
      if (!vendorId) return null
      const result = await api.getScraperStatus(vendorId)
      return result
    },
    enabled: vendorId !== null,
    refetchInterval: 5000,
    refetchIntervalInBackground: false,
  })
}

/**
 * Mutation to trigger a scraper run
 * Invalidates related queries on success
 */
export function useTriggerScraper() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async ({
      vendorId,
      options,
    }: {
      vendorId: number
      options?: TriggerScraperOptions
    }): Promise<TriggerScraperResponse> => {
      return api.triggerScraper(vendorId, options)
    },
    onSuccess: (_data, variables) => {
      // Invalidate scraper status for the vendor
      queryClient.invalidateQueries({
        queryKey: ['scraper-status', variables.vendorId],
      })
      // Invalidate run history
      queryClient.invalidateQueries({
        queryKey: ['scrape-runs'],
      })
    },
  })
}

/**
 * Fetch cron scheduling suggestions for all vendors
 * Uses staleTime: Infinity since suggestions don't change frequently
 */
export function useCronSuggestions() {
  return useQuery({
    queryKey: ['cron-suggestions'],
    queryFn: async () => {
      const result = await api.getCronSuggestions()
      return result
    },
    staleTime: Infinity,
  })
}

// Re-export types for convenience
export type { ScraperStatus, CronSuggestion, TriggerScraperOptions, TriggerScraperResponse }
