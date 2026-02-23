import { useState, useEffect, useRef, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQueryClient } from '@tanstack/react-query'
import { useScrapeRuns, useScrapeRun, useRunAlerts } from '@/hooks/useScrapeRuns'
import { useAlerts } from '@/hooks/useAlerts'
import { useScraperStatus, useTriggerScraper, useStopScraper, useScraperLogs, useScraperLogHistory, type LogConnectionStatus } from '@/hooks/useScrapers'
import type { ScrapeRun } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'

// ============================================================================
// Constants & Types
// ============================================================================

const VENDORS = [
  { id: 1, name: 'IngredientsOnline', short: 'IO' },
  { id: 2, name: 'BulkSupplements', short: 'BS' },
  { id: 3, name: 'BoxNutra', short: 'BN' },
  { id: 4, name: 'TrafaPharma', short: 'TP' },
] as const
const TERMINAL_STORAGE_KEY = 'admin-terminal-tabs-v1'

// Light theme vendor colors (matching Dashboard)
const vendorStyles: Record<string, {
  bg: string
  border: string
  accent: string
  icon: string
  solidBg: string
  badgeBg: string
  badgeText: string
}> = {
  'IngredientsOnline': {
    bg: 'bg-sky-50',
    border: 'border-sky-200',
    accent: 'text-sky-700',
    icon: 'bg-sky-500',
    solidBg: 'bg-sky-600',
    badgeBg: 'bg-sky-100',
    badgeText: 'text-sky-700',
  },
  'BulkSupplements': {
    bg: 'bg-emerald-50',
    border: 'border-emerald-200',
    accent: 'text-emerald-700',
    icon: 'bg-emerald-500',
    solidBg: 'bg-emerald-600',
    badgeBg: 'bg-emerald-100',
    badgeText: 'text-emerald-700',
  },
  'BoxNutra': {
    bg: 'bg-violet-50',
    border: 'border-violet-200',
    accent: 'text-violet-700',
    icon: 'bg-violet-500',
    solidBg: 'bg-violet-600',
    badgeBg: 'bg-violet-100',
    badgeText: 'text-violet-700',
  },
  'TrafaPharma': {
    bg: 'bg-amber-50',
    border: 'border-amber-200',
    accent: 'text-amber-700',
    icon: 'bg-amber-500',
    solidBg: 'bg-amber-600',
    badgeBg: 'bg-amber-100',
    badgeText: 'text-amber-700',
  },
}

// Alert type configuration (light theme)
const alertTypeConfig: Record<string, { label: string; icon: string; color: string }> = {
  'price_decrease_major': { label: 'Price Drop', icon: '▼', color: 'text-green-600' },
  'price_increase_major': { label: 'Price Hike', icon: '▲', color: 'text-red-600' },
  'price_change': { label: 'Price Change', icon: '◆', color: 'text-amber-600' },
  'stock_out': { label: 'Stock Out', icon: '○', color: 'text-red-600' },
  'stock_change': { label: 'Stock', icon: '●', color: 'text-amber-600' },
  'stale_variant': { label: 'Stale', icon: '⏱', color: 'text-slate-500' },
  'new_product': { label: 'New', icon: '+', color: 'text-emerald-600' },
  'reactivated': { label: 'Back', icon: '↺', color: 'text-sky-600' },
  'product_removed': { label: 'Removed', icon: '−', color: 'text-red-600' },
  'error': { label: 'Error', icon: '!', color: 'text-red-600' },
  'warning': { label: 'Warning', icon: '⚠', color: 'text-amber-600' },
}

// Severity badge styles (light theme)
const severityStyles: Record<string, string> = {
  critical: 'bg-red-100 text-red-700 border-red-200',
  error: 'bg-red-100 text-red-700 border-red-200',
  warning: 'bg-amber-100 text-amber-700 border-amber-200',
  info: 'bg-sky-100 text-sky-700 border-sky-200',
}

// ============================================================================
// Helper Functions
// ============================================================================

function formatDuration(startStr: string | null, endStr: string | null): string {
  if (!startStr) return '—'
  if (!endStr) return 'Running...'

  const start = new Date(startStr)
  const end = new Date(endStr)
  const ms = end.getTime() - start.getTime()

  if (ms < 0) return '—'

  const seconds = Math.floor(ms / 1000)
  const minutes = Math.floor(seconds / 60)
  const hours = Math.floor(minutes / 60)

  if (hours > 0) {
    return `${hours}h ${minutes % 60}m`
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`
  }
  return `${seconds}s`
}

function formatRelativeTime(dateStr: string | null): string {
  if (!dateStr) return 'Never'

  const date = new Date(dateStr)
  const now = new Date()
  const diffMs = now.getTime() - date.getTime()
  const diffSecs = Math.floor(diffMs / 1000)
  const diffMins = Math.floor(diffSecs / 60)
  const diffHours = Math.floor(diffMins / 60)
  const diffDays = Math.floor(diffHours / 24)

  if (diffSecs < 60) return 'Just now'
  if (diffMins < 60) return `${diffMins}m ago`
  if (diffHours < 24) return `${diffHours}h ago`
  if (diffDays < 7) return `${diffDays}d ago`
  return date.toLocaleDateString()
}

function formatDateTime(dateStr: string | null): string {
  if (!dateStr) return '—'
  return new Date(dateStr).toLocaleString()
}

function getVendorStyle(name: string) {
  return vendorStyles[name] || vendorStyles['IngredientsOnline']
}

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

function formatLogSummary(summary?: Record<string, string>): string {
  if (!summary) return '—'

  const processed = summary.processed || summary.total_products_scraped
  const discovered = summary.discovered
  const failed = summary.failed
  const duration = summary.run_duration
  const fullScrape = summary.full_scrape

  const parts: string[] = []
  if (processed) parts.push(`Processed ${processed}`)
  if (discovered) parts.push(`Discovered ${discovered}`)
  if (failed) parts.push(`Failed ${failed}`)
  if (duration) parts.push(`Duration ${duration}`)
  if (fullScrape) parts.push(`Full ${fullScrape}`)

  return parts.length > 0 ? parts.join(' • ') : 'Summary unavailable'
}

// ============================================================================
// Scraper Card Component
// ============================================================================

interface ScraperCardProps {
  vendor: typeof VENDORS[number]
  lastRun?: string | null
  onExecute: (vendorId: number, vendorName: string, options?: { max_products?: number }) => void
}

function ScraperCard({ vendor, lastRun, onExecute }: ScraperCardProps) {
  const [showDialog, setShowDialog] = useState(false)
  const [maxProducts, setMaxProducts] = useState('')

  const { data: status } = useScraperStatus(vendor.id)
  const style = getVendorStyle(vendor.name)
  const isRunning = status?.is_running ?? false

  const handleTrigger = () => {
    const options = maxProducts ? { max_products: parseInt(maxProducts, 10) } : undefined
    onExecute(vendor.id, vendor.name, options)
    setShowDialog(false)
    setMaxProducts('')
  }

  return (
    <>
      <div className={`
        relative overflow-hidden rounded-xl border ${style.border} ${style.bg}
        p-5 transition-all duration-200 hover:shadow-md
      `}>
        {/* Vendor Icon Strip */}
        <div className={`absolute left-0 top-0 w-1 h-full ${style.icon}`} />

        {/* Status Indicator */}
        <div className="absolute top-4 right-4 flex items-center gap-2">
          <div className={`
            w-2 h-2 rounded-full
            ${isRunning ? 'bg-green-500 animate-pulse' : 'bg-slate-300'}
          `} />
          <span className="text-xs text-slate-500">
            {isRunning ? 'Running' : 'Idle'}
          </span>
        </div>

        <div className="pl-3">
          <h3 className={`font-semibold ${style.accent} mb-1`}>
            {vendor.name}
          </h3>
          <p className="text-xs text-slate-500 mb-4">
            {lastRun ? `Last run: ${formatRelativeTime(lastRun)}` : 'Never run'}
          </p>

          <Button
            size="sm"
            onClick={() => setShowDialog(true)}
            disabled={isRunning}
            className={`w-full ${style.solidBg} hover:opacity-90 text-white`}
          >
            {isRunning ? (
              <>
                <span className="w-2 h-2 rounded-full bg-white animate-pulse mr-2" />
                Running...
              </>
            ) : (
              'Execute'
            )}
          </Button>
        </div>
      </div>

      <Dialog open={showDialog} onOpenChange={setShowDialog}>
        <DialogContent className="bg-white border-slate-200">
          <DialogHeader>
            <DialogTitle className={`${style.accent}`}>
              Execute {vendor.name} Scraper
            </DialogTitle>
            <DialogDescription className="text-slate-500">
              Configure scraper parameters. Leave empty for full run.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 pt-4">
            <div>
              <label className="text-sm text-slate-600 mb-2 block">
                Max Products (optional)
              </label>
              <Input
                type="number"
                placeholder="Unlimited"
                value={maxProducts}
                onChange={(e) => setMaxProducts(e.target.value)}
                className="border-slate-200"
              />
            </div>
            <div className="flex gap-2 pt-2">
              <Button
                variant="outline"
                onClick={() => setShowDialog(false)}
                className="flex-1"
              >
                Cancel
              </Button>
              <Button
                onClick={handleTrigger}
                className={`flex-1 ${style.solidBg} text-white hover:opacity-90`}
              >
                Start Scraper
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </>
  )
}

// ============================================================================
// Summary Stats Bar
// ============================================================================

function SummaryStatsBar({
  totalRuns,
  activeAlerts,
  lastSuccessfulRun
}: {
  totalRuns: number
  activeAlerts: number
  lastSuccessfulRun: string | null
}) {
  return (
    <div className="grid grid-cols-3 gap-4">
      <div className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm">
        <div className="flex items-start justify-between">
          <div>
            <p className="text-sm text-slate-500 mb-1">Runs Today</p>
            <p className="text-2xl font-bold text-slate-900">{totalRuns}</p>
          </div>
          <div className="w-10 h-10 rounded-lg bg-blue-50 flex items-center justify-center">
            <svg className="w-5 h-5 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
          </div>
        </div>
      </div>

      <div className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm">
        <div className="flex items-start justify-between">
          <div>
            <p className="text-sm text-slate-500 mb-1">Active Alerts</p>
            <p className={`text-2xl font-bold ${activeAlerts > 0 ? 'text-amber-600' : 'text-slate-900'}`}>
              {activeAlerts}
            </p>
          </div>
          <div className={`w-10 h-10 rounded-lg ${activeAlerts > 0 ? 'bg-amber-50' : 'bg-slate-50'} flex items-center justify-center`}>
            <svg className={`w-5 h-5 ${activeAlerts > 0 ? 'text-amber-600' : 'text-slate-400'}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
            </svg>
          </div>
        </div>
      </div>

      <div className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm">
        <div className="flex items-start justify-between">
          <div>
            <p className="text-sm text-slate-500 mb-1">Last Success</p>
            <p className="text-xl font-bold text-slate-900">
              {lastSuccessfulRun ? formatRelativeTime(lastSuccessfulRun) : '—'}
            </p>
          </div>
          <div className="w-10 h-10 rounded-lg bg-green-50 flex items-center justify-center">
            <svg className="w-5 h-5 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
        </div>
      </div>
    </div>
  )
}

// ============================================================================
// Terminal Panel Component
// ============================================================================

interface TerminalTab {
  vendorId: number
  vendorName: string
  logFile?: string | null
  status: LogConnectionStatus
  logs: string[]
}

interface TerminalPanelProps {
  tabs: TerminalTab[]
  activeTabId: number | null
  onTabChange: (vendorId: number) => void
  onTabClose: (vendorId: number) => void
  onClear: (vendorId: number) => void
  onStop: (vendorId: number) => void
}

function TerminalPanel({ tabs, activeTabId, onTabChange, onTabClose, onClear, onStop }: TerminalPanelProps) {
  const terminalRef = useRef<HTMLDivElement>(null)
  const activeTab = tabs.find(t => t.vendorId === activeTabId)

  // Auto-scroll to bottom when new logs arrive
  useEffect(() => {
    if (terminalRef.current) {
      terminalRef.current.scrollTop = terminalRef.current.scrollHeight
    }
  }, [activeTab?.logs])

  if (tabs.length === 0) return null

  const getStatusColor = (status: LogConnectionStatus) => {
    switch (status) {
      case 'connected': return 'bg-green-500'
      case 'connecting': return 'bg-yellow-500 animate-pulse'
      case 'completed': return 'bg-slate-400'
      case 'error': return 'bg-red-500'
      default: return 'bg-slate-400'
    }
  }

  const getStatusText = (status: LogConnectionStatus) => {
    switch (status) {
      case 'connected': return 'Live'
      case 'connecting': return 'Connecting...'
      case 'completed': return 'Completed'
      case 'error': return 'Error'
      default: return 'Disconnected'
    }
  }

  return (
    <div className="bg-slate-900 rounded-xl border border-slate-700 overflow-hidden">
      {/* Tab bar */}
      <div className="flex items-center bg-slate-800 px-2 py-1 border-b border-slate-700 overflow-x-auto">
        {tabs.map((tab) => {
          const isActive = tab.vendorId === activeTabId
          return (
            <div
              key={tab.vendorId}
              className={`
                flex items-center gap-2 px-3 py-1.5 rounded-t cursor-pointer mr-1
                ${isActive ? 'bg-slate-900 text-white' : 'text-slate-400 hover:text-slate-200'}
              `}
              onClick={() => onTabChange(tab.vendorId)}
            >
              <span className={`w-2 h-2 rounded-full ${getStatusColor(tab.status)}`} />
              <span className="text-xs font-medium">
                {tab.vendorName}
                {tab.logFile ? ' (History)' : ''}
              </span>
              <button
                onClick={(e) => {
                  e.stopPropagation()
                  onTabClose(tab.vendorId)
                }}
                className="ml-1 text-slate-500 hover:text-slate-300 text-xs"
              >
                ×
              </button>
            </div>
          )
        })}
      </div>

      {/* Terminal content */}
      {activeTab && (
        <div className="relative">
          {/* Status bar */}
          <div className="flex items-center justify-between px-4 py-2 bg-slate-800/50 border-b border-slate-700">
            <div className="flex items-center gap-2">
              <span className={`w-2 h-2 rounded-full ${getStatusColor(activeTab.status)}`} />
              <span className="text-xs text-slate-400">{getStatusText(activeTab.status)}</span>
              {activeTab.logFile && (
                <span className="text-xs text-slate-500">• {activeTab.logFile}</span>
              )}
              <span className="text-xs text-slate-500">• {activeTab.logs.length} lines</span>
            </div>
            <div className="flex items-center gap-2">
              {!activeTab.logFile && (activeTab.status === 'connected' || activeTab.status === 'connecting') && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => onStop(activeTab.vendorId)}
                  className="h-6 px-2 text-xs text-red-400 hover:text-red-300 hover:bg-red-900/30"
                >
                  Stop
                </Button>
              )}
              <Button
                variant="ghost"
                size="sm"
                onClick={() => onClear(activeTab.vendorId)}
                className="h-6 px-2 text-xs text-slate-400 hover:text-white"
              >
                Clear
              </Button>
            </div>
          </div>

          {/* Log output */}
          <div
            ref={terminalRef}
            className="h-64 overflow-y-auto p-4 font-mono text-xs leading-relaxed"
          >
            {activeTab.logs.length === 0 ? (
              <div className="text-slate-500 italic">Waiting for output...</div>
            ) : (
              activeTab.logs.map((line, i) => (
                <div
                  key={i}
                  className={`
                    ${line.startsWith('[CONNECTED]') ? 'text-green-400' : ''}
                    ${line.startsWith('[COMPLETED]') ? 'text-blue-400' : ''}
                    ${line.startsWith('[ERROR]') ? 'text-red-400' : ''}
                    ${!line.startsWith('[') ? 'text-slate-300' : ''}
                  `}
                >
                  {line}
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  )
}

// ============================================================================
// Main Admin Page
// ============================================================================

export function Admin() {
  const navigate = useNavigate()
  const [vendorFilter, setVendorFilter] = useState<number | undefined>(undefined)
  const [selectedRunId, setSelectedRunId] = useState<number | null>(null)

  // Terminal state
  const [terminalTabs, setTerminalTabs] = useState<TerminalTab[]>([])
  const [activeTabId, setActiveTabId] = useState<number | null>(null)

  // Restore terminal tabs after reload so active runs/log sessions are recoverable.
  useEffect(() => {
    try {
      const raw = localStorage.getItem(TERMINAL_STORAGE_KEY)
      if (!raw) return

      const parsed = JSON.parse(raw) as {
        activeTabId: number | null
        tabs: Array<{ vendorId: number; vendorName: string; logFile?: string | null }>
      }
      if (!Array.isArray(parsed.tabs) || parsed.tabs.length === 0) return

      setTerminalTabs(parsed.tabs.map((tab) => ({
        vendorId: tab.vendorId,
        vendorName: tab.vendorName,
        logFile: tab.logFile ?? null,
        status: 'connecting',
        logs: [],
      })))

      const firstTabId = parsed.tabs[0]?.vendorId ?? null
      setActiveTabId(parsed.activeTabId ?? firstTabId)
    } catch {
      // Ignore invalid persisted state.
    }
  }, [])

  useEffect(() => {
    const tabsForStorage = terminalTabs.map(tab => ({
      vendorId: tab.vendorId,
      vendorName: tab.vendorName,
      logFile: tab.logFile ?? null,
    }))
    localStorage.setItem(TERMINAL_STORAGE_KEY, JSON.stringify({
      activeTabId,
      tabs: tabsForStorage,
    }))
  }, [terminalTabs, activeTabId])

  // Scraper mutations
  const queryClient = useQueryClient()
  const triggerMutation = useTriggerScraper()
  const stopMutation = useStopScraper()

  // Callback to invalidate status query when scraper completes
  const handleScraperComplete = useCallback((vendorId: number) => {
    queryClient.invalidateQueries({ queryKey: ['scraper-status', vendorId] })
  }, [queryClient])

  // SSE log streams - we'll manage multiple connections
  const logStreams = VENDORS.map(v => {
    const tab = terminalTabs.find(t => t.vendorId === v.id)
    const isTabOpen = !!tab
    const selectedLogFile = tab?.logFile ?? null
    // eslint-disable-next-line react-hooks/rules-of-hooks
    return useScraperLogs(v.id, isTabOpen, () => handleScraperComplete(v.id), selectedLogFile)
  })

  const logHistoryQueries = VENDORS.map(v => {
    // eslint-disable-next-line react-hooks/rules-of-hooks
    return useScraperLogHistory(v.id, 8)
  })

  // Update terminal tabs when log data changes
  // Use refs to track previous values and avoid infinite loops
  const prevLogDataRef = useRef<string>('')

  useEffect(() => {
    const currentLogData = logStreams.map(s => `${s.status}:${s.logs.length}`).join('|')
    if (currentLogData === prevLogDataRef.current) return
    prevLogDataRef.current = currentLogData

    setTerminalTabs(prev => {
      // Only update if there are actual changes
      let hasChanges = false
      const updated = prev.map((tab) => {
        const vendorIdx = VENDORS.findIndex(v => v.id === tab.vendorId)
        if (vendorIdx === -1) return tab
        const stream = logStreams[vendorIdx]
        if (tab.status !== stream.status || tab.logs.length !== stream.logs.length) {
          hasChanges = true
          return { ...tab, status: stream.status, logs: stream.logs }
        }
        return tab
      })
      return hasChanges ? updated : prev
    })
  })

  const { data: runsData, isLoading: runsLoading } = useScrapeRuns({
    vendor_id: vendorFilter,
    limit: 15
  })

  const { data: alertsData, isLoading: alertsLoading } = useAlerts({
    vendorId: vendorFilter,
    alertTypes: ['price_decrease_major', 'price_increase_major', 'stock_out', 'stale_variant', 'price_change', 'stock_change'],
    limit: 20,
  })
  const { data: selectedRunData, isLoading: selectedRunLoading } = useScrapeRun(selectedRunId)
  const { data: selectedRunAlerts, isLoading: selectedRunAlertsLoading } = useRunAlerts(selectedRunId)

  const runs = runsData?.runs || []
  const alerts = alertsData?.alerts || []
  const selectedRunFromList = runs.find((run) => run.run_id === selectedRunId) || null
  const selectedRun = selectedRunData || selectedRunFromList
  const selectedRunAlertList = selectedRunAlerts || []
  const recentLogFiles = VENDORS.flatMap((vendor, idx) => {
    const logs = logHistoryQueries[idx].data || []
    return logs.map(log => ({
      vendorId: vendor.id,
      vendorName: vendor.name,
      ...log,
    }))
  }).sort((a, b) => new Date(b.modified_at).getTime() - new Date(a.modified_at).getTime())
  const logHistoryLoading = logHistoryQueries.some(q => q.isLoading)

  // Calculate summary stats
  const totalRunsToday = runs.filter((r: ScrapeRun) => {
    if (!r.started_at) return false
    const runDate = new Date(r.started_at)
    const today = new Date()
    return runDate.toDateString() === today.toDateString()
  }).length

  const lastSuccessfulRun = runs.find((r: ScrapeRun) => r.status === 'completed')?.completed_at || null

  // Get last run per vendor for scraper cards
  const vendorLastRuns: Record<number, string | null> = {}
  VENDORS.forEach(v => {
    const lastRun = runs.find((r: ScrapeRun) => r.vendor_id === v.id)
    vendorLastRuns[v.id] = lastRun?.started_at || null
  })

  // Handle scraper execution
  const handleExecute = (vendorId: number, vendorName: string, options?: { max_products?: number }) => {
    // Trigger the scraper - add terminal tab only AFTER success
    triggerMutation.mutate(
      { vendorId, options },
      {
        onSuccess: () => {
          // Add or focus terminal tab after scraper starts
          setTerminalTabs(prev => {
            const existing = prev.find(t => t.vendorId === vendorId)
            if (existing) {
              // Clear existing logs and reconnect
              return prev.map(t => t.vendorId === vendorId ? {
                ...t,
                logFile: null,
                logs: [],
                status: 'connecting' as LogConnectionStatus
              } : t)
            }
            // Add new tab
            return [...prev, {
              vendorId,
              vendorName,
              logFile: null,
              status: 'connecting' as LogConnectionStatus,
              logs: []
            }]
          })
          setActiveTabId(vendorId)
        }
      }
    )
  }

  // Terminal handlers
  const handleTabClose = (vendorId: number) => {
    setTerminalTabs(prev => prev.filter(t => t.vendorId !== vendorId))
    if (activeTabId === vendorId) {
      const remaining = terminalTabs.filter(t => t.vendorId !== vendorId)
      setActiveTabId(remaining.length > 0 ? remaining[0].vendorId : null)
    }
  }

  const handleClear = (vendorId: number) => {
    setTerminalTabs(prev => prev.map(t => t.vendorId === vendorId ? { ...t, logs: [] } : t))
  }

  const handleStop = (vendorId: number) => {
    stopMutation.mutate(vendorId)
  }

  const handleOpenLog = (vendorId: number, vendorName: string, filename: string) => {
    setTerminalTabs(prev => {
      const existing = prev.find(t => t.vendorId === vendorId)
      if (existing) {
        return prev.map(t => t.vendorId === vendorId ? {
          ...t,
          logFile: filename,
          logs: [],
          status: 'connecting' as LogConnectionStatus
        } : t)
      }
      return [...prev, {
        vendorId,
        vendorName,
        logFile: filename,
        status: 'connecting' as LogConnectionStatus,
        logs: []
      }]
    })
    setActiveTabId(vendorId)
  }

  const handleOpenAlertProduct = (alert: { ingredient_id?: number | null; sku?: string | null; product_name?: string | null }) => {
    const ingredientId = alert.ingredient_id
    if (ingredientId) {
      setSelectedRunId(null)
      navigate(`/products/${ingredientId}`)
      return
    }

    const term = alert.sku || alert.product_name
    if (term) {
      setSelectedRunId(null)
      navigate(`/products?search=${encodeURIComponent(term)}`)
    }
  }

  // Status badge colors for light theme
  const getStatusBadgeClass = (status: string) => {
    switch (status) {
      case 'completed': return 'bg-green-100 text-green-700 border-green-200'
      case 'running': return 'bg-blue-100 text-blue-700 border-blue-200'
      case 'failed': return 'bg-red-100 text-red-700 border-red-200'
      default: return 'bg-slate-100 text-slate-700 border-slate-200'
    }
  }

  return (
    <div className="space-y-6 pb-12">
      {/* Hero Header */}
      <div className="relative overflow-hidden rounded-xl hero-gradient hero-shimmer p-8">
        <div className="absolute inset-0 bg-grid-white/5 [mask-image:linear-gradient(0deg,transparent,black)]" />
        <div className="relative">
          <div className="flex items-center gap-3 mb-2">
            <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-white/10 backdrop-blur border border-white/10">
              <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              </svg>
            </div>
            <h1 className="text-3xl font-bold text-white tracking-tight">
              Scraper Admin
            </h1>
          </div>
          <p className="text-slate-300 max-w-2xl">
            Monitor scraper runs, trigger manual scrapes, and review actionable alerts
          </p>
        </div>
        <div className="absolute right-8 top-1/2 -translate-y-1/2 opacity-10">
          <svg className="w-32 h-32 text-white" fill="currentColor" viewBox="0 0 24 24">
            <path d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
            <circle cx="12" cy="12" r="3" />
          </svg>
        </div>
      </div>

      {/* Scraper Control Cards */}
      <div>
        <h2 className="text-lg font-semibold text-slate-900 mb-4">Scrapers</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {VENDORS.map((vendor) => (
            <ScraperCard
              key={vendor.id}
              vendor={vendor}
              lastRun={vendorLastRuns[vendor.id]}
              onExecute={handleExecute}
            />
          ))}
        </div>
      </div>

      {/* Terminal Panel */}
      {terminalTabs.length > 0 && (
        <TerminalPanel
          tabs={terminalTabs}
          activeTabId={activeTabId}
          onTabChange={setActiveTabId}
          onTabClose={handleTabClose}
          onClear={handleClear}
          onStop={handleStop}
        />
      )}

      {/* Log History */}
      <Card className="bg-white border-slate-200 shadow-sm overflow-hidden">
        <CardHeader className="pb-3 border-b border-slate-100">
          <div className="flex items-center justify-between">
            <h3 className="font-semibold text-slate-900">Log History</h3>
            <span className="text-xs text-slate-500">Open old logs and review run summaries</span>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow className="border-slate-100 hover:bg-transparent">
                <TableHead className="text-xs text-slate-500">Vendor</TableHead>
                <TableHead className="text-xs text-slate-500">File</TableHead>
                <TableHead className="text-xs text-slate-500">Updated</TableHead>
                <TableHead className="text-xs text-slate-500">Size</TableHead>
                <TableHead className="text-xs text-slate-500">Summary</TableHead>
                <TableHead className="text-xs text-slate-500 w-24">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {logHistoryLoading ? (
                Array.from({ length: 3 }).map((_, i) => (
                  <TableRow key={i} className="border-slate-100">
                    <TableCell colSpan={6}>
                      <div className="h-8 bg-slate-100 rounded animate-pulse" />
                    </TableCell>
                  </TableRow>
                ))
              ) : recentLogFiles.length === 0 ? (
                <TableRow className="border-slate-100 hover:bg-transparent">
                  <TableCell colSpan={6} className="py-8 text-center text-sm text-slate-500">
                    No log files found yet
                  </TableCell>
                </TableRow>
              ) : (
                recentLogFiles.slice(0, 16).map((log) => {
                  const style = getVendorStyle(log.vendorName)
                  return (
                    <TableRow key={`${log.vendorId}-${log.filename}`} className="border-slate-100 hover:bg-slate-50">
                      <TableCell>
                        <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs border ${style.badgeBg} ${style.badgeText} ${style.border}`}>
                          {log.vendorName}
                        </span>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <span className="text-xs font-mono text-slate-700">{log.filename}</span>
                          {log.is_active && (
                            <Badge className="text-[10px] bg-green-100 text-green-700 border-green-200 border">
                              Active
                            </Badge>
                          )}
                        </div>
                      </TableCell>
                      <TableCell className="text-sm text-slate-500">
                        {formatRelativeTime(log.modified_at)}
                      </TableCell>
                      <TableCell className="text-sm text-slate-500">
                        {formatFileSize(log.size_bytes)}
                      </TableCell>
                      <TableCell className="text-xs text-slate-600 max-w-[360px]">
                        <span className="truncate block" title={formatLogSummary(log.summary)}>
                          {formatLogSummary(log.summary)}
                        </span>
                      </TableCell>
                      <TableCell>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => handleOpenLog(log.vendorId, log.vendorName, log.filename)}
                          className="h-6 px-2 text-xs text-slate-600 hover:text-slate-900"
                        >
                          Open
                        </Button>
                      </TableCell>
                    </TableRow>
                  )
                })
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Summary Stats Bar */}
      <SummaryStatsBar
        totalRuns={totalRunsToday}
        activeAlerts={alertsData?.total ?? alerts.length}
        lastSuccessfulRun={lastSuccessfulRun}
      />

      {/* Recent Runs Table (full width) */}
      <Card className="bg-white border-slate-200 shadow-sm overflow-hidden">
        <CardHeader className="pb-3 border-b border-slate-100">
          <div className="flex items-center justify-between">
            <h3 className="font-semibold text-slate-900">Recent Runs</h3>
            <div className="flex gap-1">
              <Button
                variant={vendorFilter === undefined ? 'secondary' : 'ghost'}
                size="sm"
                onClick={() => setVendorFilter(undefined)}
                className="h-7 px-3 text-xs"
              >
                All
              </Button>
              {VENDORS.map((v) => {
                const style = getVendorStyle(v.name)
                const isActive = vendorFilter === v.id
                return (
                  <Button
                    key={v.id}
                    variant={isActive ? 'secondary' : 'ghost'}
                    size="sm"
                    onClick={() => setVendorFilter(v.id)}
                    className={`h-7 px-3 text-xs flex items-center gap-1.5`}
                  >
                    <span className={`w-2 h-2 rounded-full ${style.solidBg} ${isActive ? '' : 'opacity-50'}`} />
                    {v.short}
                  </Button>
                )
              })}
            </div>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow className="border-slate-100 hover:bg-transparent">
                <TableHead className="text-xs text-slate-500">Vendor</TableHead>
                <TableHead className="text-xs text-slate-500">Status</TableHead>
                <TableHead className="text-xs text-slate-500">Products</TableHead>
                <TableHead className="text-xs text-slate-500">Alerts</TableHead>
                <TableHead className="text-xs text-slate-500">Duration</TableHead>
                <TableHead className="text-xs text-slate-500">Started</TableHead>
                <TableHead className="text-xs text-slate-500 w-24">Details</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {runsLoading ? (
                Array.from({ length: 3 }).map((_, i) => (
                  <TableRow key={i} className="border-slate-100">
                    <TableCell colSpan={7}>
                      <div className="h-8 bg-slate-100 rounded animate-pulse" />
                    </TableCell>
                  </TableRow>
                ))
              ) : runs.length === 0 ? (
                <TableRow className="border-slate-100 hover:bg-transparent">
                  <TableCell colSpan={7} className="py-12">
                    <div className="flex flex-col items-center gap-3 text-center">
                      <div className="w-12 h-12 rounded-full bg-slate-100 flex items-center justify-center">
                        <svg className="w-5 h-5 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                        </svg>
                      </div>
                      <div>
                        <p className="text-sm text-slate-600">No runs recorded yet</p>
                        <p className="text-xs text-slate-400 mt-1">
                          Run your first scrape using the buttons above
                        </p>
                      </div>
                    </div>
                  </TableCell>
                </TableRow>
              ) : (
                runs.map((run) => {
                  const vendorName = run.vendor_name || VENDORS.find(v => v.id === run.vendor_id)?.name || 'Unknown'
                  const style = getVendorStyle(vendorName)
                  const productsProcessed = run.products_processed ?? run.products_discovered ?? 0
                  const productsDiscovered = run.products_discovered ?? productsProcessed
                  const variantsNew = run.variants_new ?? 0
                  const runAlerts = (run.price_alerts ?? 0) + (run.stock_alerts ?? 0) + (run.data_quality_alerts ?? 0)

                  return (
                    <TableRow key={run.run_id} className="border-slate-100 hover:bg-slate-50">
                      <TableCell>
                        <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs border ${style.badgeBg} ${style.badgeText} ${style.border}`}>
                          {vendorName}
                        </span>
                      </TableCell>
                      <TableCell>
                        <Badge className={`text-xs border ${getStatusBadgeClass(run.status)}`}>
                          {run.status === 'running' && (
                            <span className="w-1.5 h-1.5 rounded-full bg-current animate-pulse mr-1.5" />
                          )}
                          {run.status}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-sm text-slate-700">
                        {productsProcessed}
                        <span className="text-slate-400"> / </span>
                        <span className="text-slate-500">{productsDiscovered} discovered</span>
                        <span className="text-slate-400"> • </span>
                        <span className="text-slate-500">{variantsNew} new variants</span>
                      </TableCell>
                      <TableCell>
                        <span className={`text-sm ${runAlerts > 0 ? 'text-amber-600' : 'text-slate-500'}`}>
                          {runAlerts}
                        </span>
                      </TableCell>
                      <TableCell className="text-sm text-slate-600">
                        {formatDuration(run.started_at, run.completed_at)}
                      </TableCell>
                      <TableCell className="text-sm text-slate-500">
                        {formatRelativeTime(run.started_at)}
                      </TableCell>
                      <TableCell>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => setSelectedRunId(run.run_id)}
                          className="h-6 px-2 text-xs text-slate-600 hover:text-slate-900"
                        >
                          View
                        </Button>
                      </TableCell>
                    </TableRow>
                  )
                })
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Run Details Dialog */}
      <Dialog
        open={selectedRunId !== null}
        onOpenChange={(open) => {
          if (!open) setSelectedRunId(null)
        }}
      >
        <DialogContent className="bg-white border-slate-200 max-w-5xl">
          <DialogHeader>
            <DialogTitle className="text-slate-900">
              Run Details
              {selectedRunId ? ` #${selectedRunId}` : ''}
            </DialogTitle>
            <DialogDescription className="text-slate-500">
              Review metrics and alerts from this scraper run.
            </DialogDescription>
          </DialogHeader>

          {!selectedRun && selectedRunLoading ? (
            <div className="space-y-3 pt-2">
              <div className="h-16 bg-slate-100 rounded animate-pulse" />
              <div className="h-48 bg-slate-100 rounded animate-pulse" />
            </div>
          ) : !selectedRun ? (
            <div className="text-sm text-slate-500 py-8 text-center">
              Run details unavailable.
            </div>
          ) : (
            <div className="space-y-4 pt-2">
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <div className="rounded-lg border border-slate-200 p-3">
                  <p className="text-xs text-slate-500 mb-1">Status</p>
                  <Badge className={`text-xs border ${getStatusBadgeClass(selectedRun.status || 'unknown')}`}>
                    {selectedRun.status || 'unknown'}
                  </Badge>
                </div>
                <div className="rounded-lg border border-slate-200 p-3">
                  <p className="text-xs text-slate-500 mb-1">Started</p>
                  <p className="text-sm text-slate-800">{formatDateTime(selectedRun.started_at)}</p>
                </div>
                <div className="rounded-lg border border-slate-200 p-3">
                  <p className="text-xs text-slate-500 mb-1">Completed</p>
                  <p className="text-sm text-slate-800">{formatDateTime(selectedRun.completed_at)}</p>
                </div>
                <div className="rounded-lg border border-slate-200 p-3">
                  <p className="text-xs text-slate-500 mb-1">Duration</p>
                  <p className="text-sm text-slate-800">{formatDuration(selectedRun.started_at, selectedRun.completed_at)}</p>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                <div className="rounded-lg border border-slate-200 p-3">
                  <p className="text-xs text-slate-500 mb-2">Products</p>
                  <div className="text-xs text-slate-700 space-y-1">
                    <p>Discovered: {selectedRun.products_discovered ?? 0}</p>
                    <p>Processed: {selectedRun.products_processed ?? 0}</p>
                    <p>Skipped: {selectedRun.products_skipped ?? 0}</p>
                    <p>Failed: {selectedRun.products_failed ?? 0}</p>
                  </div>
                </div>
                <div className="rounded-lg border border-slate-200 p-3">
                  <p className="text-xs text-slate-500 mb-2">Variants</p>
                  <div className="text-xs text-slate-700 space-y-1">
                    <p>New: {selectedRun.variants_new ?? 0}</p>
                    <p>Updated: {selectedRun.variants_updated ?? 0}</p>
                    <p>Unchanged: {selectedRun.variants_unchanged ?? 0}</p>
                    <p>Stale: {selectedRun.variants_stale ?? 0}</p>
                    <p>Reactivated: {selectedRun.variants_reactivated ?? 0}</p>
                  </div>
                </div>
                <div className="rounded-lg border border-slate-200 p-3">
                  <p className="text-xs text-slate-500 mb-2">Run Config + Alerts</p>
                  <div className="text-xs text-slate-700 space-y-1">
                    <p>Full Scrape: {selectedRun.is_full_scrape === false ? 'No' : 'Yes'}</p>
                    <p>Max Products: {selectedRun.max_products_limit ?? 'Unlimited'}</p>
                    <p>Price Alerts: {selectedRun.price_alerts ?? 0}</p>
                    <p>Stock Alerts: {selectedRun.stock_alerts ?? 0}</p>
                    <p>Data Quality Alerts: {selectedRun.data_quality_alerts ?? 0}</p>
                  </div>
                </div>
              </div>

              <div className="rounded-lg border border-slate-200 overflow-hidden">
                <div className="px-3 py-2 border-b border-slate-100 bg-slate-50 flex items-center justify-between">
                  <p className="text-sm font-medium text-slate-800">Run Alerts</p>
                  <span className="text-xs text-slate-500">
                    {selectedRunAlertsLoading ? 'Loading...' : `${selectedRunAlertList.length} alerts`}
                  </span>
                </div>

                {selectedRunAlertsLoading ? (
                  <div className="p-3">
                    <div className="h-12 bg-slate-100 rounded animate-pulse" />
                  </div>
                ) : selectedRunAlertList.length === 0 ? (
                  <div className="p-4 text-sm text-slate-500 text-center">
                    No alerts recorded for this run.
                  </div>
                ) : (
                  <div className="max-h-64 overflow-y-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="border-slate-100 hover:bg-transparent">
                          <TableHead className="text-xs text-slate-500 w-24">Severity</TableHead>
                          <TableHead className="text-xs text-slate-500 w-32">Type</TableHead>
                          <TableHead className="text-xs text-slate-500">Product</TableHead>
                          <TableHead className="text-xs text-slate-500">Message</TableHead>
                          <TableHead className="text-xs text-slate-500 w-28">Actions</TableHead>
                          <TableHead className="text-xs text-slate-500 w-20">When</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {selectedRunAlertList.map((alert) => {
                          const typeConfig = alertTypeConfig[alert.alert_type] || { label: alert.alert_type, icon: '•', color: 'text-slate-500' }
                          const severityStyle = severityStyles[alert.severity] || severityStyles.info

                          return (
                            <TableRow key={alert.alert_id} className="border-slate-100">
                              <TableCell>
                                <Badge className={`text-xs border ${severityStyle}`}>
                                  {alert.severity}
                                </Badge>
                              </TableCell>
                              <TableCell>
                                <div className="flex items-center gap-1.5">
                                  <span className={`${typeConfig.color} text-sm`}>{typeConfig.icon}</span>
                                  <span className="text-xs text-slate-700">{typeConfig.label}</span>
                                </div>
                              </TableCell>
                              <TableCell className="text-sm text-slate-700">
                                {alert.product_name || alert.sku || '—'}
                              </TableCell>
                              <TableCell className="text-sm text-slate-600">
                                {alert.message}
                              </TableCell>
                              <TableCell>
                                <div className="flex items-center gap-1">
                                  {(alert.ingredient_id || alert.sku || alert.product_name) && (
                                    <Button
                                      variant="ghost"
                                      size="sm"
                                      onClick={() => handleOpenAlertProduct(alert)}
                                      className="h-6 px-2 text-xs text-slate-600 hover:text-slate-900"
                                    >
                                      Product
                                    </Button>
                                  )}
                                  {alert.product_url && (
                                    <a
                                      href={alert.product_url}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="inline-flex items-center h-6 px-2 text-xs text-blue-600 hover:text-blue-700"
                                    >
                                      Source ↗
                                    </a>
                                  )}
                                </div>
                              </TableCell>
                              <TableCell className="text-xs text-slate-500">
                                {formatRelativeTime(alert.created_at)}
                              </TableCell>
                            </TableRow>
                          )
                        })}
                      </TableBody>
                    </Table>
                  </div>
                )}
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>

      {/* Actionable Alerts */}
      <Card className="bg-white border-slate-200 shadow-sm overflow-hidden">
        <CardHeader className="pb-3 border-b border-slate-100">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <h3 className="font-semibold text-slate-900">Actionable Alerts</h3>
              <Badge className={`text-xs border ${
                alerts.length > 0
                  ? 'bg-red-100 text-red-700 border-red-200'
                  : 'bg-green-100 text-green-700 border-green-200'
              }`}>
                {alertsData?.total || alerts.length}
              </Badge>
            </div>
            <span className="text-xs text-slate-500">
              Price changes, stock-outs, stale products
            </span>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow className="border-slate-100 hover:bg-transparent">
                <TableHead className="text-xs text-slate-500 w-20">Severity</TableHead>
                <TableHead className="text-xs text-slate-500">Vendor</TableHead>
                <TableHead className="text-xs text-slate-500 w-24">Type</TableHead>
                <TableHead className="text-xs text-slate-500">Product</TableHead>
                <TableHead className="text-xs text-slate-500">Message</TableHead>
                <TableHead className="text-xs text-slate-500 w-28">Actions</TableHead>
                <TableHead className="text-xs text-slate-500 w-20">When</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {alertsLoading ? (
                Array.from({ length: 3 }).map((_, i) => (
                  <TableRow key={i} className="border-slate-100">
                    <TableCell colSpan={7}>
                      <div className="h-10 bg-slate-100 rounded animate-pulse" />
                    </TableCell>
                  </TableRow>
                ))
              ) : alerts.length === 0 ? (
                <TableRow className="border-slate-100 hover:bg-transparent">
                  <TableCell colSpan={7} className="py-12">
                    <div className="flex flex-col items-center gap-3 text-center">
                      <div className="w-14 h-14 rounded-full bg-green-50 flex items-center justify-center">
                        <svg className="w-7 h-7 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                        </svg>
                      </div>
                      <div>
                        <p className="text-sm text-green-700 font-medium">All clear!</p>
                        <p className="text-xs text-slate-500 mt-1">
                          No actionable alerts at the moment
                        </p>
                      </div>
                    </div>
                  </TableCell>
                </TableRow>
              ) : (
                alerts.map((alert) => {
                  const vendorName = alert.vendor_name || VENDORS.find(v => v.id === alert.vendor_id)?.name || 'Unknown'
                  const style = getVendorStyle(vendorName)
                  const typeConfig = alertTypeConfig[alert.alert_type] || { label: alert.alert_type, icon: '•', color: 'text-slate-500' }
                  const severityStyle = severityStyles[alert.severity] || severityStyles.info

                  return (
                    <TableRow key={alert.alert_id} className="border-slate-100 hover:bg-slate-50 group">
                      <TableCell>
                        <Badge className={`text-xs border ${severityStyle}`}>
                          {alert.severity}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs border ${style.badgeBg} ${style.badgeText} ${style.border}`}>
                          {vendorName}
                        </span>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1.5">
                          <span className={`${typeConfig.color} text-sm`}>{typeConfig.icon}</span>
                          <span className="text-xs text-slate-700">{typeConfig.label}</span>
                        </div>
                      </TableCell>
                      <TableCell className="max-w-[180px]">
                        <span className="text-sm text-slate-700 truncate block" title={alert.product_name || ''}>
                          {alert.product_name || alert.sku || '—'}
                        </span>
                      </TableCell>
                      <TableCell className="max-w-[200px]">
                        <span className="text-sm text-slate-600 truncate block" title={alert.message}>
                          {alert.message}
                        </span>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1 opacity-60 group-hover:opacity-100 transition-opacity">
                          {alert.ingredient_id && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => navigate(`/products/${alert.ingredient_id}`)}
                              className="h-6 px-2 text-xs text-slate-600 hover:text-slate-900"
                            >
                              View
                            </Button>
                          )}
                          {alert.product_url && (
                            <a
                              href={alert.product_url}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="inline-flex items-center h-6 px-2 text-xs text-blue-600 hover:text-blue-700"
                            >
                              Source ↗
                            </a>
                          )}
                        </div>
                      </TableCell>
                      <TableCell className="text-sm text-slate-500">
                        {formatRelativeTime(alert.created_at)}
                      </TableCell>
                    </TableRow>
                  )
                })
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  )
}
