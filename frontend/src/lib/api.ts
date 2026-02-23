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
export const getLogStreamUrl = (vendorId: number, file?: string | null): string => {
  if (!file) return `${API_BASE}/scrapers/${vendorId}/logs`
  const params = new URLSearchParams({ file })
  return `${API_BASE}/scrapers/${vendorId}/logs?${params.toString()}`
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
  products_discovered?: number
  products_processed?: number
  products_skipped?: number
  products_failed?: number
  variants_new?: number
  variants_updated?: number
  variants_unchanged?: number
  variants_stale?: number
  variants_reactivated?: number
  price_alerts?: number
  stock_alerts?: number
  data_quality_alerts?: number
  is_full_scrape?: boolean
  max_products_limit?: number
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
  pid?: number | null
}

export interface ScraperLogFile {
  filename: string
  modified_at: string
  size_bytes: number
  is_active: boolean
  summary: Record<string, string>
}

export interface TriggerScraperOptions {
  max_products?: number
  no_playwright?: boolean
  resume?: boolean
}

export interface TriggerScraperResponse {
  message: string
  pid: number
  vendor_id: number
  vendor_name: string
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

export interface UpdateProductResponse {
  success: boolean
  vendor_ingredient_id: number
  vendor_id: number | null
  vendor_name: string | null
  sku: string | null
  old_values: Record<string, unknown>
  new_values: Record<string, unknown>
  changed_fields: Record<string, { old: unknown; new: unknown }>
  message: string
  duration_ms: number
  error?: string | null
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
   * Get available historical log files for a vendor
   */
  getScraperLogHistory: (vendorId: number, limit: number = 20): Promise<ScraperLogFile[]> => {
    const searchParams = new URLSearchParams()
    searchParams.set('limit', String(limit))
    return fetchApi<ScraperLogFile[]>(`/scrapers/${vendorId}/logs/history?${searchParams.toString()}`)
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
  getRuns: (params?: GetRunsParams): Promise<{ runs: ScrapeRun[]; total: number; limit: number; offset: number }> => {
    const searchParams = new URLSearchParams()
    if (params?.vendor_id) searchParams.set('vendor_id', String(params.vendor_id))
    if (params?.status) searchParams.set('status', params.status)
    if (params?.limit) searchParams.set('limit', String(params.limit))
    if (params?.offset) searchParams.set('offset', String(params.offset))

    const query = searchParams.toString()
    return fetchApi<{ runs: ScrapeRun[]; total: number; limit: number; offset: number }>(
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
  getRunAlerts: (runId: number): Promise<{ alerts: ScrapeAlert[]; total: number }> => {
    return fetchApi<{ alerts: ScrapeAlert[]; total: number }>(`/runs/${runId}/alerts`)
  },

  /**
   * Get alerts with optional filters
   */
  getAlerts: (params?: GetAlertsParams): Promise<{ alerts: ScrapeAlert[]; total: number; limit: number; offset: number }> => {
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
    return fetchApi<{ alerts: ScrapeAlert[]; total: number; limit: number; offset: number }>(
      `/alerts${query ? `?${query}` : ''}`
    )
  },

  /**
   * Get alert summary counts
   */
  getAlertSummary: (): Promise<AlertSummary> => {
    return fetchApi<AlertSummary>('/alerts/summary')
  },

  /**
   * Update a single product's price and inventory from vendor source
   */
  updateSingleProduct: (vendorIngredientId: number): Promise<UpdateProductResponse> => {
    return fetchApi<UpdateProductResponse>('/products/update-single', {
      method: 'POST',
      body: JSON.stringify({ vendor_ingredient_id: vendorIngredientId }),
    })
  },

  /**
   * Get basic product info for a vendor_ingredient_id
   */
  getProductInfo: (vendorIngredientId: number): Promise<{
    vendor_ingredient_id: number
    vendor_id: number
    sku: string | null
    raw_product_name: string | null
    last_seen_at: string | null
    vendor_name: string
    product_url: string | null
  }> => {
    return fetchApi(`/products/${vendorIngredientId}`)
  },

  // ===========================================================================
  // Data routes (replaces Supabase JS SDK queries)
  // ===========================================================================

  /**
   * Get paginated ingredients with vendor info and stock status
   */
  getIngredients: (params?: {
    search?: string
    limit?: number
    offset?: number
  }): Promise<{ data: IngredientData[]; total: number }> => {
    const searchParams = new URLSearchParams()
    if (params?.search) searchParams.set('search', params.search)
    if (params?.limit) searchParams.set('limit', String(params.limit))
    if (params?.offset) searchParams.set('offset', String(params.offset))
    const query = searchParams.toString()
    return fetchApi(`/ingredients${query ? `?${query}` : ''}`)
  },

  /**
   * Get full ingredient detail with price tiers and inventory
   */
  getIngredientDetail: (id: number): Promise<IngredientDetailData | null> => {
    return fetchApi(`/ingredients/${id}`)
  },

  /**
   * Get cross-vendor price comparison
   */
  getPriceComparison: (params?: {
    search?: string
  }): Promise<PriceComparisonData[]> => {
    const searchParams = new URLSearchParams()
    if (params?.search) searchParams.set('search', params.search)
    const query = searchParams.toString()
    return fetchApi(`/price-comparison${query ? `?${query}` : ''}`)
  },

  /**
   * Get paginated vendor ingredients (products)
   */
  getVendorIngredients: (params?: {
    vendorId?: number
    search?: string
    limit?: number
    offset?: number
  }): Promise<{ data: VendorIngredientData[]; total: number }> => {
    const searchParams = new URLSearchParams()
    if (params?.vendorId) searchParams.set('vendor_id', String(params.vendorId))
    if (params?.search) searchParams.set('search', params.search)
    if (params?.limit) searchParams.set('limit', String(params.limit))
    if (params?.offset) searchParams.set('offset', String(params.offset))
    const query = searchParams.toString()
    return fetchApi(`/vendor-ingredients${query ? `?${query}` : ''}`)
  },

  /**
   * Get all vendors
   */
  getVendors: (): Promise<VendorData[]> => {
    return fetchApi('/vendors')
  },

  /**
   * Get vendor stats (product counts, last scraped)
   */
  getVendorStats: (): Promise<VendorStatsData[]> => {
    return fetchApi('/vendors/stats')
  },

  /**
   * Get all categories
   */
  getCategories: (): Promise<CategoryData[]> => {
    return fetchApi('/categories')
  },
}

// ============================================================================
// Data Types (for data routes)
// ============================================================================

export interface IngredientData {
  ingredient_id: number
  name: string
  category_name: string | null
  category_id: number | null
  status: string | null
  vendors: string[]
  vendor_count: number
  stock_status: 'in_stock' | 'out_of_stock' | 'unknown'
}

export interface IngredientDetailData {
  ingredient_id: number
  name: string
  category_name: string | null
  priceTiers: PriceTierData[]
  warehouseInventory: InventoryLevelData[]
  simpleInventory: SimpleInventoryData[]
}

export interface PriceTierData {
  vendor_ingredient_id: number
  vendor_id: number
  vendor_name: string
  sku: string | null
  packaging: string | null
  pack_size: number
  min_quantity: number | null
  price: number | null
  price_per_kg: number | null
  product_url: string | null
  last_seen_at: string | null
}

export interface InventoryLevelData {
  vendor_ingredient_id: number
  vendor_name: string
  sku: string | null
  warehouse: string
  quantity_available: number
  stock_status: string | null
}

export interface SimpleInventoryData {
  vendor_ingredient_id: number
  vendor_name: string
  sku: string | null
  stock_status: string | null
}

export interface PriceComparisonData {
  ingredient_name: string
  ingredient_id: number
  vendors: {
    vendor_id: number
    vendor_name: string
    sku: string | null
    product_name: string | null
    best_price_per_kg: number | null
    min_order_qty: number | null
    last_seen: string | null
  }[]
}

export interface VendorIngredientData {
  vendor_ingredient_id: number
  sku: string | null
  raw_product_name: string | null
  status: string | null
  last_seen_at: string | null
  vendor_id: number
  vendor_name: string
}

export interface VendorData {
  vendor_id: number
  name: string
  pricing_model: string | null
  status: string | null
}

export interface VendorStatsData {
  vendor_id: number
  name: string
  pricing_model: string | null
  status: string | null
  productCount: number
  variantCount: number
  lastScraped: string | null
}

export interface CategoryData {
  category_id: number
  name: string
  description: string | null
}
