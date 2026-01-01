/**
 * API client for backend scraper operations
 */

// In production, use relative path (same-origin via nginx proxy)
// In development, use localhost:8000 or override with VITE_API_URL
const getApiBase = (): string => {
  // If explicitly set, use that
  if (import.meta.env.VITE_API_URL) {
    return import.meta.env.VITE_API_URL
  }
  // In production builds, use relative path (nginx will proxy)
  if (import.meta.env.PROD) {
    return '/api'
  }
  // Development default
  return 'http://localhost:8000/api'
}

const API_BASE = getApiBase()

/**
 * Get the full URL for a log streaming endpoint (for EventSource)
 */
export const getLogStreamUrl = (vendorId: number): string => {
  return `${API_BASE}/scrapers/${vendorId}/logs`
}

// ============================================================================
// Types
// ============================================================================

export interface ScrapeRun {
  run_id: number
  vendor_id: number
  vendor_name?: string
  status: 'pending' | 'running' | 'completed' | 'failed'
  started_at: string | null
  completed_at: string | null
  products_scraped: number
  products_updated: number
  products_new: number
  products_removed: number
  errors_count: number
  duration_seconds: number | null
  triggered_by: 'manual' | 'scheduled' | 'api'
  options?: Record<string, unknown>
}

export interface ScrapeAlert {
  alert_id: number
  run_id: number | null
  vendor_ingredient_id: number | null
  vendor_id: number
  vendor_name?: string
  alert_type: 'price_change' | 'new_product' | 'product_removed' | 'stock_change' | 'error' | 'warning'
  severity: 'info' | 'warning' | 'error' | 'critical'
  sku: string | null
  product_name: string | null
  old_value: string | null
  new_value: string | null
  change_percent: number | null
  message: string
  created_at: string
  product_url: string | null
  ingredient_id: number | null
}

export interface CronSuggestion {
  vendor_id: number
  vendor_name: string
  suggested_cron: string
  suggested_time_utc: string
  reason: string
  last_run: string | null
  avg_duration_minutes: number | null
  command?: string
}

export interface AlertSummary {
  total: number
  by_severity: {
    info: number
    warning: number
    error: number
    critical: number
  }
  by_type: {
    price_change: number
    new_product: number
    product_removed: number
    stock_change: number
    error: number
    warning: number
  }
  by_vendor: Array<{
    vendor_id: number
    vendor_name: string
    count: number
  }>
}

export interface ScraperStatus {
  vendor_id: number
  vendor_name: string
  is_running: boolean
  current_run_id: number | null
  progress: number | null
  current_product: string | null
  started_at: string | null
  estimated_completion: string | null
}

export interface TriggerScraperOptions {
  max_products?: number
  no_playwright?: boolean
  resume?: boolean
}

export interface TriggerScraperResponse {
  run_id: number
  status: string
  message: string
}

export interface GetRunsParams {
  vendor_id?: number
  status?: string
  limit?: number
  offset?: number
}

export interface GetAlertsParams {
  vendor_id?: number
  alert_types?: string[]
  severity?: string[]
  run_id?: number
  limit?: number
  offset?: number
}

// ============================================================================
// API Error
// ============================================================================

export class ApiError extends Error {
  status: number
  data?: unknown

  constructor(message: string, status: number, data?: unknown) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.data = data
  }
}

// ============================================================================
// Fetch Helper
// ============================================================================

async function fetchApi<T>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE}${endpoint}`

  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  })

  if (!response.ok) {
    let errorData: unknown
    try {
      errorData = await response.json()
    } catch {
      errorData = await response.text()
    }
    throw new ApiError(
      `API request failed: ${response.statusText}`,
      response.status,
      errorData
    )
  }

  return response.json()
}

// ============================================================================
// API Client
// ============================================================================

export const api = {
  /**
   * Trigger a scraper run for a vendor
   */
  triggerScraper: (
    vendorId: number,
    options?: TriggerScraperOptions
  ): Promise<TriggerScraperResponse> => {
    return fetchApi<TriggerScraperResponse>(`/scrapers/${vendorId}/run`, {
      method: 'POST',
      body: JSON.stringify(options || {}),
    })
  },

  /**
   * Get current scraper status for a vendor
   */
  getScraperStatus: (vendorId: number): Promise<ScraperStatus> => {
    return fetchApi<ScraperStatus>(`/scrapers/${vendorId}/status`)
  },

  /**
   * Stop a running scraper for a vendor
   */
  stopScraper: (vendorId: number): Promise<{ message: string; vendor_id: number; vendor_name: string; pid: number }> => {
    return fetchApi(`/scrapers/${vendorId}/stop`, { method: 'POST' })
  },

  /**
   * Get cron scheduling suggestions for all vendors
   */
  getCronSuggestions: (): Promise<CronSuggestion[]> => {
    return fetchApi<CronSuggestion[]>('/scrapers/cron-suggestions')
  },

  /**
   * Get scrape run history
   */
  getRuns: (params?: GetRunsParams): Promise<{ data: ScrapeRun[]; total: number }> => {
    const searchParams = new URLSearchParams()
    if (params?.vendor_id) searchParams.set('vendor_id', String(params.vendor_id))
    if (params?.status) searchParams.set('status', params.status)
    if (params?.limit) searchParams.set('limit', String(params.limit))
    if (params?.offset) searchParams.set('offset', String(params.offset))

    const query = searchParams.toString()
    return fetchApi<{ data: ScrapeRun[]; total: number }>(
      `/runs${query ? `?${query}` : ''}`
    )
  },

  /**
   * Get a single scrape run by ID
   */
  getRun: (runId: number): Promise<ScrapeRun> => {
    return fetchApi<ScrapeRun>(`/runs/${runId}`)
  },

  /**
   * Get alerts for a specific run
   */
  getRunAlerts: (runId: number): Promise<ScrapeAlert[]> => {
    return fetchApi<ScrapeAlert[]>(`/runs/${runId}/alerts`)
  },

  /**
   * Get alerts with optional filters
   */
  getAlerts: (params?: GetAlertsParams): Promise<{ data: ScrapeAlert[]; total: number }> => {
    const searchParams = new URLSearchParams()
    if (params?.vendor_id) searchParams.set('vendor_id', String(params.vendor_id))
    if (params?.alert_types?.length) {
      searchParams.set('alert_types', params.alert_types.join(','))
    }
    if (params?.severity?.length) {
      searchParams.set('severity', params.severity.join(','))
    }
    if (params?.run_id) searchParams.set('run_id', String(params.run_id))
    if (params?.limit) searchParams.set('limit', String(params.limit))
    if (params?.offset) searchParams.set('offset', String(params.offset))

    const query = searchParams.toString()
    return fetchApi<{ data: ScrapeAlert[]; total: number }>(
      `/alerts${query ? `?${query}` : ''}`
    )
  },

  /**
   * Get alert summary counts
   */
  getAlertSummary: (): Promise<AlertSummary> => {
    return fetchApi<AlertSummary>('/alerts/summary')
  },
}
