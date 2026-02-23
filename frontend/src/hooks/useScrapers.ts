import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState, useEffect, useCallback, useRef } from 'react'
import {
  api,
  getLogStreamUrl,
  type ScraperStatus,
  type CronSuggestion,
  type ScraperLogFile,
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
    staleTime: 0,
    refetchOnMount: 'always',
    refetchOnWindowFocus: true,
    refetchInterval: 5000,
    refetchIntervalInBackground: true,
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
 * Mutation to stop a running scraper
 * Invalidates related queries on success
 */
export function useStopScraper() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async (vendorId: number) => {
      return api.stopScraper(vendorId)
    },
    onSuccess: (_data, vendorId) => {
      // Invalidate scraper status for the vendor
      queryClient.invalidateQueries({
        queryKey: ['scraper-status', vendorId],
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

/**
 * Fetch recent scraper log files for a vendor.
 */
export function useScraperLogHistory(vendorId: number | null, limit: number = 20) {
  return useQuery({
    queryKey: ['scraper-log-history', vendorId, limit],
    queryFn: async () => {
      if (!vendorId) return []
      return api.getScraperLogHistory(vendorId, limit)
    },
    enabled: vendorId !== null,
    staleTime: 30000,
  })
}

/**
 * Connect to scraper log stream via SSE
 * Returns log lines and connection status
 */
export type LogConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'completed' | 'error'

export function useScraperLogs(
  vendorId: number | null,
  enabled: boolean = false,
  onComplete?: () => void,
  logFile?: string | null
) {
  const [logs, setLogs] = useState<string[]>([])
  const [status, setStatus] = useState<LogConnectionStatus>('disconnected')
  const [error, setError] = useState<string | null>(null)
  const eventSourceRef = useRef<EventSource | null>(null)
  const hasConnectedRef = useRef(false)
  const onCompleteRef = useRef(onComplete)
  onCompleteRef.current = onComplete

  const connect = useCallback(() => {
    if (!vendorId) return
    if (hasConnectedRef.current) return // Prevent reconnection loops

    // Close existing connection
    if (eventSourceRef.current) {
      eventSourceRef.current.close()
    }

    hasConnectedRef.current = true
    setStatus('connecting')
    setError(null)
    setLogs([])

    const url = getLogStreamUrl(vendorId, logFile)
    const eventSource = new EventSource(url)
    eventSourceRef.current = eventSource

    eventSource.onopen = () => {
      setStatus('connected')
    }

    eventSource.onmessage = (event) => {
      const line = event.data

      // Check for special status messages
      if (line.startsWith('[COMPLETED]')) {
        setStatus('completed')
        eventSource.close()
        onCompleteRef.current?.()
        return
      }
      if (line.startsWith('[ERROR]')) {
        setError(line)
        setStatus('error')
        eventSource.close()
        return
      }

      // Add regular log line
      setLogs((prev) => [...prev, line])
    }

    eventSource.onerror = () => {
      // SSE connection error - don't retry automatically
      setError('Connection failed')
      setStatus('error')
      eventSource.close()
    }
  }, [vendorId, logFile])

  const disconnect = useCallback(() => {
    hasConnectedRef.current = false
    if (eventSourceRef.current) {
      eventSourceRef.current.close()
      eventSourceRef.current = null
    }
    setStatus('disconnected')
  }, [])

  const clear = useCallback(() => {
    setLogs([])
    setError(null)
  }, [])

  // Auto-connect when enabled changes
  useEffect(() => {
    if (enabled && vendorId) {
      hasConnectedRef.current = false // Reset to allow new connection
      connect()
    } else {
      disconnect()
    }

    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close()
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled, vendorId, logFile])

  return {
    logs,
    status,
    error,
    connect,
    disconnect,
    clear,
    isConnected: status === 'connected',
    isCompleted: status === 'completed',
  }
}

// Re-export types for convenience
export type {
  ScraperStatus,
  CronSuggestion,
  ScraperLogFile,
  TriggerScraperOptions,
  TriggerScraperResponse
}
