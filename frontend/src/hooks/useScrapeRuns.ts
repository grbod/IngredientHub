import { useQuery } from '@tanstack/react-query'
import { api, type ScrapeRun, type ScrapeAlert, type GetRunsParams } from '@/lib/api'

/**
 * Fetch scrape run history with optional filters
 */
export function useScrapeRuns(options: GetRunsParams = {}) {
  const { vendor_id, status, limit = 50, offset = 0 } = options

  return useQuery({
    queryKey: ['scrape-runs', { vendor_id, status, limit, offset }],
    queryFn: async () => {
      const result = await api.getRuns({ vendor_id, status, limit, offset })
      return result
    },
  })
}

/**
 * Fetch a single scrape run by ID
 */
export function useScrapeRun(runId: number | null) {
  return useQuery({
    queryKey: ['scrape-run', runId],
    queryFn: async () => {
      if (!runId) return null
      const result = await api.getRun(runId)
      return result
    },
    enabled: runId !== null,
  })
}

/**
 * Fetch alerts for a specific scrape run
 */
export function useRunAlerts(runId: number | null) {
  return useQuery({
    queryKey: ['run-alerts', runId],
    queryFn: async () => {
      if (!runId) return []
      const result = await api.getRunAlerts(runId)
      return result
    },
    enabled: runId !== null,
  })
}

// Re-export types for convenience
export type { ScrapeRun, ScrapeAlert }
