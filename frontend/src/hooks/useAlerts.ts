import { useQuery } from '@tanstack/react-query'
import { api, type ScrapeAlert, type AlertSummary, type GetAlertsParams } from '@/lib/api'

interface UseAlertsOptions {
  vendorId?: number
  alertTypes?: string[]
  severity?: string[]
  runId?: number
  limit?: number
  offset?: number
}

/**
 * Fetch alerts with optional filters
 */
export function useAlerts(options: UseAlertsOptions = {}) {
  const { vendorId, alertTypes, severity, runId, limit = 50, offset = 0 } = options

  return useQuery({
    queryKey: ['alerts', { vendorId, alertTypes, severity, runId, limit, offset }],
    queryFn: async () => {
      const params: GetAlertsParams = {
        vendor_id: vendorId,
        alert_types: alertTypes,
        severity,
        run_id: runId,
        limit,
        offset,
      }
      const result = await api.getAlerts(params)
      return result
    },
  })
}

/**
 * Fetch alert summary counts by severity, type, and vendor
 */
export function useAlertSummary() {
  return useQuery({
    queryKey: ['alert-summary'],
    queryFn: async () => {
      const result = await api.getAlertSummary()
      return result
    },
  })
}

// Re-export types for convenience
export type { ScrapeAlert, AlertSummary }
