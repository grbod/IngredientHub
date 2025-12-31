import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useScrapeRuns } from '@/hooks/useScrapeRuns'
import { useAlerts } from '@/hooks/useAlerts'
import { useScraperStatus, useTriggerScraper, useCronSuggestions } from '@/hooks/useScrapers'
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

const vendorStyles: Record<string, {
  bg: string
  border: string
  accent: string
  glow: string
  icon: string
  solidBg: string
}> = {
  'IngredientsOnline': {
    bg: 'bg-sky-500/10',
    border: 'border-sky-500/30',
    accent: 'text-sky-400',
    glow: 'shadow-sky-500/20',
    icon: 'bg-sky-500',
    solidBg: 'bg-sky-500',
  },
  'BulkSupplements': {
    bg: 'bg-emerald-500/10',
    border: 'border-emerald-500/30',
    accent: 'text-emerald-400',
    glow: 'shadow-emerald-500/20',
    icon: 'bg-emerald-500',
    solidBg: 'bg-emerald-500',
  },
  'BoxNutra': {
    bg: 'bg-violet-500/10',
    border: 'border-violet-500/30',
    accent: 'text-violet-400',
    glow: 'shadow-violet-500/20',
    icon: 'bg-violet-500',
    solidBg: 'bg-violet-500',
  },
  'TrafaPharma': {
    bg: 'bg-amber-500/10',
    border: 'border-amber-500/30',
    accent: 'text-amber-400',
    glow: 'shadow-amber-500/20',
    icon: 'bg-amber-500',
    solidBg: 'bg-amber-500',
  },
}

const alertTypeConfig: Record<string, { label: string; icon: string; color: string }> = {
  'price_decrease_major': { label: 'Price Drop', icon: '▼', color: 'text-green-400' },
  'price_increase_major': { label: 'Price Hike', icon: '▲', color: 'text-red-400' },
  'price_change': { label: 'Price Change', icon: '◆', color: 'text-amber-400' },
  'stock_out': { label: 'Stock Out', icon: '○', color: 'text-red-400' },
  'stock_change': { label: 'Stock', icon: '●', color: 'text-amber-400' },
  'stale_variant': { label: 'Stale', icon: '⏱', color: 'text-slate-400' },
  'new_product': { label: 'New', icon: '+', color: 'text-emerald-400' },
  'reactivated': { label: 'Back', icon: '↺', color: 'text-sky-400' },
  'product_removed': { label: 'Removed', icon: '−', color: 'text-red-400' },
  'error': { label: 'Error', icon: '!', color: 'text-red-400' },
  'warning': { label: 'Warning', icon: '⚠', color: 'text-amber-400' },
}

const severityStyles: Record<string, string> = {
  critical: 'bg-red-500/20 text-red-300 border-red-500/40',
  error: 'bg-red-500/20 text-red-300 border-red-500/40',
  warning: 'bg-amber-500/20 text-amber-300 border-amber-500/40',
  info: 'bg-sky-500/20 text-sky-300 border-sky-500/40',
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

function getVendorStyle(name: string) {
  return vendorStyles[name] || vendorStyles['IngredientsOnline']
}

// ============================================================================
// Scraper Card Component
// ============================================================================

interface ScraperCardProps {
  vendor: typeof VENDORS[number]
  lastRun?: string | null
}

function ScraperCard({ vendor, lastRun }: ScraperCardProps) {
  const [showDialog, setShowDialog] = useState(false)
  const [maxProducts, setMaxProducts] = useState('')

  const { data: status } = useScraperStatus(vendor.id)
  const triggerMutation = useTriggerScraper()

  const style = getVendorStyle(vendor.name)
  const isRunning = status?.is_running ?? false

  const handleTrigger = () => {
    const options = maxProducts ? { max_products: parseInt(maxProducts, 10) } : undefined
    triggerMutation.mutate({ vendorId: vendor.id, options })
    setShowDialog(false)
    setMaxProducts('')
  }

  return (
    <>
      <div className={`
        relative overflow-hidden rounded-lg border ${style.border} ${style.bg}
        p-4 transition-all duration-300
        hover:shadow-lg hover:${style.glow}
        backdrop-blur-sm
      `}>
        {/* LED Status Indicator */}
        <div className="absolute top-3 right-3 flex items-center gap-2">
          <div className={`
            w-2.5 h-2.5 rounded-full
            ${isRunning ? 'bg-green-400 animate-pulse shadow-lg shadow-green-400/50' : 'bg-slate-600'}
            transition-colors duration-300
          `} />
          <span className="text-[10px] font-mono uppercase tracking-wider text-slate-500">
            {isRunning ? 'ACTIVE' : 'IDLE'}
          </span>
        </div>

        {/* Vendor Icon Strip */}
        <div className={`absolute left-0 top-0 w-1 h-full ${style.icon}`} />

        <div className="pl-2">
          <h3 className={`font-semibold ${style.accent} text-sm mb-0.5`}>
            {vendor.name}
          </h3>
          <div className="flex items-center gap-2 mb-3">
            <p className="text-[10px] font-mono text-slate-500 uppercase tracking-wide">
              VENDOR #{vendor.id.toString().padStart(2, '0')}
            </p>
            <span className="text-slate-700">•</span>
            <p className={`text-[10px] font-mono ${lastRun ? 'text-slate-400' : 'text-slate-600'}`}>
              {lastRun ? `Last: ${formatRelativeTime(lastRun)}` : 'Never run'}
            </p>
          </div>

          <Button
            size="sm"
            variant={isRunning ? 'secondary' : 'default'}
            onClick={() => setShowDialog(true)}
            disabled={isRunning || triggerMutation.isPending}
            className={`
              w-full text-xs font-mono uppercase tracking-wider
              ${isRunning ? 'opacity-50' : ''}
            `}
          >
            {isRunning ? (
              <>
                <span className="w-2 h-2 rounded-full bg-green-400 animate-pulse mr-2" />
                Running...
              </>
            ) : triggerMutation.isPending ? (
              'Starting...'
            ) : (
              'Execute'
            )}
          </Button>
        </div>
      </div>

      <Dialog open={showDialog} onOpenChange={setShowDialog}>
        <DialogContent className="bg-slate-900 border-slate-700 text-slate-100">
          <DialogHeader>
            <DialogTitle className={`${style.accent} font-mono`}>
              Execute {vendor.name} Scraper
            </DialogTitle>
            <DialogDescription className="text-slate-400 font-mono text-sm">
              Configure scraper parameters. Leave empty for full run.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 pt-4">
            <div>
              <label className="text-xs font-mono uppercase tracking-wider text-slate-400 mb-2 block">
                Max Products (optional)
              </label>
              <Input
                type="number"
                placeholder="∞ Unlimited"
                value={maxProducts}
                onChange={(e) => setMaxProducts(e.target.value)}
                className="bg-slate-800 border-slate-700 text-slate-100 font-mono placeholder:text-slate-600"
              />
            </div>
            <div className="flex gap-2 pt-2">
              <Button
                variant="ghost"
                onClick={() => setShowDialog(false)}
                className="flex-1 font-mono text-slate-400 hover:text-slate-100"
              >
                Cancel
              </Button>
              <Button
                onClick={handleTrigger}
                className={`flex-1 font-mono ${style.solidBg} text-white hover:opacity-90`}
              >
                Confirm
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
      <div className="bg-slate-800/40 border border-slate-700/50 rounded-lg p-3 backdrop-blur-sm">
        <div className="flex items-center gap-2 mb-1">
          <svg className="w-4 h-4 text-sky-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M13 10V3L4 14h7v7l9-11h-7z" />
          </svg>
          <span className="text-[10px] font-mono uppercase tracking-wider text-slate-500">Runs Today</span>
        </div>
        <p className="text-2xl font-mono font-bold text-slate-100">{totalRuns}</p>
      </div>

      <div className="bg-slate-800/40 border border-slate-700/50 rounded-lg p-3 backdrop-blur-sm">
        <div className="flex items-center gap-2 mb-1">
          <svg className="w-4 h-4 text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
          <span className="text-[10px] font-mono uppercase tracking-wider text-slate-500">Active Alerts</span>
        </div>
        <p className={`text-2xl font-mono font-bold ${activeAlerts > 0 ? 'text-amber-400' : 'text-slate-100'}`}>
          {activeAlerts}
        </p>
      </div>

      <div className="bg-slate-800/40 border border-slate-700/50 rounded-lg p-3 backdrop-blur-sm">
        <div className="flex items-center gap-2 mb-1">
          <svg className="w-4 h-4 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <span className="text-[10px] font-mono uppercase tracking-wider text-slate-500">Last Success</span>
        </div>
        <p className="text-lg font-mono font-bold text-slate-100">
          {lastSuccessfulRun ? formatRelativeTime(lastSuccessfulRun) : '—'}
        </p>
      </div>
    </div>
  )
}

// ============================================================================
// Cron Suggestions Card
// ============================================================================

function CronSuggestionsCard() {
  const { data: suggestions, isLoading } = useCronSuggestions()
  const [copiedId, setCopiedId] = useState<number | null>(null)

  const copyToClipboard = (text: string, vendorId: number) => {
    navigator.clipboard.writeText(text)
    setCopiedId(vendorId)
    setTimeout(() => setCopiedId(null), 2000)
  }

  // Fallback suggestions if API not ready
  const fallbackSuggestions = VENDORS.map(v => ({
    vendor_id: v.id,
    vendor_name: v.name,
    suggested_cron: `0 ${v.id + 1} * * *`,
    reason: `Daily at ${v.id + 1}:00 AM`,
    command: `python ${v.name.toLowerCase()}_scraper.py`,
  }))

  const items = suggestions || fallbackSuggestions

  return (
    <Card className="bg-slate-900/50 border-slate-700/50 backdrop-blur">
      <CardHeader className="pb-3">
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-amber-400" />
          <h3 className="font-mono text-sm uppercase tracking-wider text-slate-300">
            Cron Schedule
          </h3>
        </div>
        <p className="text-xs text-slate-500 font-mono">
          Add to system crontab
        </p>
      </CardHeader>
      <CardContent className="space-y-3">
        {isLoading ? (
          <div className="space-y-2">
            {[1, 2, 3, 4].map(i => (
              <div key={i} className="h-16 bg-slate-800/50 rounded animate-pulse" />
            ))}
          </div>
        ) : (
          items.map((s) => {
            const style = getVendorStyle(s.vendor_name)
            const cronLine = `${s.suggested_cron} cd ~/IngredientHub/backend && source venv/bin/activate && ${s.command || `python ${s.vendor_name.toLowerCase()}_scraper.py`}`

            return (
              <div
                key={s.vendor_id}
                className={`p-3 rounded-lg border ${style.border} ${style.bg}`}
              >
                <div className="flex items-center justify-between mb-2">
                  <span className={`text-xs font-mono ${style.accent}`}>
                    {s.vendor_name}
                  </span>
                  <span className="text-[10px] text-slate-500 font-mono">
                    {s.reason || `${s.suggested_cron}`}
                  </span>
                </div>
                <div className="flex items-stretch gap-2">
                  <code className="flex-1 text-[10px] bg-slate-950 text-slate-300 p-2 rounded font-mono overflow-x-auto leading-relaxed border border-slate-800">
                    {cronLine}
                  </code>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => copyToClipboard(cronLine, s.vendor_id)}
                    className="px-2 text-slate-500 hover:text-slate-100 shrink-0"
                  >
                    {copiedId === s.vendor_id ? (
                      <span className="text-green-400 text-xs">✓</span>
                    ) : (
                      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                      </svg>
                    )}
                  </Button>
                </div>
              </div>
            )
          })
        )}
      </CardContent>
    </Card>
  )
}

// ============================================================================
// Main Admin Page
// ============================================================================

export function Admin() {
  const navigate = useNavigate()
  const [vendorFilter, setVendorFilter] = useState<number | undefined>(undefined)

  const { data: runsData, isLoading: runsLoading } = useScrapeRuns({
    vendor_id: vendorFilter,
    limit: 15
  })

  const { data: alertsData, isLoading: alertsLoading } = useAlerts({
    vendorId: vendorFilter,
    alertTypes: ['price_decrease_major', 'price_increase_major', 'stock_out', 'stale_variant', 'price_change', 'stock_change'],
    limit: 20,
  })

  const runs = runsData?.data || []
  const alerts = alertsData?.data || []

  // Calculate summary stats
  const totalRunsToday = runs.filter(r => {
    if (!r.started_at) return false
    const runDate = new Date(r.started_at)
    const today = new Date()
    return runDate.toDateString() === today.toDateString()
  }).length

  const lastSuccessfulRun = runs.find(r => r.status === 'completed')?.completed_at || null

  // Get last run per vendor for scraper cards
  const vendorLastRuns: Record<number, string | null> = {}
  VENDORS.forEach(v => {
    const lastRun = runs.find(r => r.vendor_id === v.id)
    vendorLastRuns[v.id] = lastRun?.started_at || null
  })

  return (
    <div className="space-y-6 pb-12">
      {/* Hero Header */}
      <div className="relative overflow-hidden rounded-xl hero-gradient hero-shimmer p-8">
        <div className="absolute inset-0 bg-grid-white/5 [mask-image:linear-gradient(0deg,transparent,black)]" />

        {/* Scan line effect */}
        <div className="absolute inset-0 pointer-events-none overflow-hidden">
          <div className="absolute w-full h-[1px] bg-white/10 animate-scan" />
        </div>

        <div className="relative">
          <div className="flex items-center gap-3 mb-2">
            <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-white/10 backdrop-blur border border-white/10">
              <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              </svg>
            </div>
            <div>
              <h1 className="text-3xl font-bold text-white tracking-tight font-mono">
                Scraper Admin
              </h1>
            </div>
          </div>
          <p className="text-slate-300 max-w-2xl font-mono text-sm">
            Monitor scraper runs, trigger manual scrapes, and review actionable alerts
          </p>
        </div>

        {/* Decorative gear */}
        <div className="absolute right-8 top-1/2 -translate-y-1/2 opacity-5">
          <svg className="w-40 h-40 text-white animate-spin-slow" fill="currentColor" viewBox="0 0 24 24">
            <path d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
            <circle cx="12" cy="12" r="3" />
          </svg>
        </div>
      </div>

      {/* Scraper Control Cards */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <div className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
          <h2 className="font-mono text-sm uppercase tracking-wider text-slate-600">
            Scrapers
          </h2>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {VENDORS.map((vendor) => (
            <ScraperCard
              key={vendor.id}
              vendor={vendor}
              lastRun={vendorLastRuns[vendor.id]}
            />
          ))}
        </div>
      </div>

      {/* Summary Stats Bar */}
      <SummaryStatsBar
        totalRuns={totalRunsToday}
        activeAlerts={alerts.length}
        lastSuccessfulRun={lastSuccessfulRun}
      />

      {/* Runs & Cron Grid */}
      <div className="grid md:grid-cols-3 gap-6">
        {/* Recent Runs Table */}
        <div className="md:col-span-2">
          <Card className="bg-slate-900/50 border-slate-700/50 backdrop-blur overflow-hidden">
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <div className="w-1.5 h-1.5 rounded-full bg-sky-400" />
                  <h3 className="font-mono text-sm uppercase tracking-wider text-slate-300">
                    Recent Runs
                  </h3>
                </div>
                <div className="flex gap-1">
                  <Button
                    variant={vendorFilter === undefined ? 'secondary' : 'ghost'}
                    size="sm"
                    onClick={() => setVendorFilter(undefined)}
                    className="h-7 px-3 text-[10px] font-mono uppercase"
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
                        className={`h-7 px-3 text-[10px] font-mono uppercase flex items-center gap-1.5 ${
                          isActive ? style.accent : 'text-slate-400 hover:text-slate-200'
                        }`}
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
                  <TableRow className="border-slate-700/50 hover:bg-transparent">
                    <TableHead className="text-[10px] font-mono uppercase text-slate-400">Vendor</TableHead>
                    <TableHead className="text-[10px] font-mono uppercase text-slate-400">Status</TableHead>
                    <TableHead className="text-[10px] font-mono uppercase text-slate-400">Products</TableHead>
                    <TableHead className="text-[10px] font-mono uppercase text-slate-400">Alerts</TableHead>
                    <TableHead className="text-[10px] font-mono uppercase text-slate-400">Duration</TableHead>
                    <TableHead className="text-[10px] font-mono uppercase text-slate-400">Started</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {runsLoading ? (
                    Array.from({ length: 3 }).map((_, i) => (
                      <TableRow key={i} className="border-slate-800/50">
                        <TableCell colSpan={6}>
                          <div className="h-8 bg-slate-800/30 rounded animate-pulse" />
                        </TableCell>
                      </TableRow>
                    ))
                  ) : runs.length === 0 ? (
                    <TableRow className="border-slate-800/50 hover:bg-transparent">
                      <TableCell colSpan={6} className="py-8">
                        <div className="flex flex-col items-center gap-3 text-center">
                          <div className="w-12 h-12 rounded-full bg-slate-800/60 border-2 border-dashed border-slate-700 flex items-center justify-center">
                            <svg className="w-5 h-5 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                            </svg>
                          </div>
                          <div>
                            <p className="text-sm text-slate-400 font-mono">No runs recorded yet</p>
                            <p className="text-xs text-slate-600 font-mono mt-1">
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
                      const statusColor = {
                        completed: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40',
                        running: 'bg-sky-500/20 text-sky-300 border-sky-500/40',
                        failed: 'bg-red-500/20 text-red-300 border-red-500/40',
                        pending: 'bg-slate-500/20 text-slate-300 border-slate-500/40',
                      }[run.status] || 'bg-slate-500/20 text-slate-300'

                      return (
                        <TableRow key={run.run_id} className="border-slate-800/50 hover:bg-slate-800/30">
                          <TableCell>
                            <span className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-mono border ${style.bg} ${style.accent} ${style.border}`}>
                              {vendorName}
                            </span>
                          </TableCell>
                          <TableCell>
                            <Badge className={`text-[10px] font-mono border ${statusColor}`}>
                              {run.status === 'running' && (
                                <span className="w-1.5 h-1.5 rounded-full bg-current animate-pulse mr-1.5" />
                              )}
                              {run.status}
                            </Badge>
                          </TableCell>
                          <TableCell className="font-mono text-xs text-slate-300">
                            <span className="text-slate-100">{run.products_scraped || run.products_updated || 0}</span>
                            <span className="text-slate-600"> / </span>
                            <span className="text-slate-500">{run.products_new || 0} new</span>
                          </TableCell>
                          <TableCell>
                            <span className={`font-mono text-xs ${(run.errors_count || 0) > 0 ? 'text-red-400' : 'text-slate-500'}`}>
                              {run.errors_count || 0}
                            </span>
                          </TableCell>
                          <TableCell className="font-mono text-xs text-slate-400">
                            {formatDuration(run.started_at, run.completed_at)}
                          </TableCell>
                          <TableCell className="font-mono text-[10px] text-slate-400">
                            {formatRelativeTime(run.started_at)}
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

        {/* Cron Suggestions */}
        <div>
          <CronSuggestionsCard />
        </div>
      </div>

      {/* Actionable Alerts */}
      <Card className="bg-slate-900/50 border-slate-700/50 backdrop-blur overflow-hidden">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className={`w-1.5 h-1.5 rounded-full ${alerts.length > 0 ? 'bg-red-400 animate-pulse' : 'bg-emerald-400'}`} />
              <h3 className="font-mono text-sm uppercase tracking-wider text-slate-300">
                Actionable Alerts
              </h3>
              <Badge className={`text-[10px] font-mono border ${
                alerts.length > 0
                  ? 'bg-red-500/20 text-red-300 border-red-500/40'
                  : 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40'
              }`}>
                {alertsData?.total || alerts.length}
              </Badge>
            </div>
            <span className="text-[10px] text-slate-500 font-mono">
              Price changes, stock-outs, stale products
            </span>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow className="border-slate-700/50 hover:bg-transparent">
                <TableHead className="text-[10px] font-mono uppercase text-slate-400 w-20">Severity</TableHead>
                <TableHead className="text-[10px] font-mono uppercase text-slate-400">Vendor</TableHead>
                <TableHead className="text-[10px] font-mono uppercase text-slate-400 w-24">Type</TableHead>
                <TableHead className="text-[10px] font-mono uppercase text-slate-400">Product</TableHead>
                <TableHead className="text-[10px] font-mono uppercase text-slate-400">Message</TableHead>
                <TableHead className="text-[10px] font-mono uppercase text-slate-400 w-28">Actions</TableHead>
                <TableHead className="text-[10px] font-mono uppercase text-slate-400 w-20">When</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {alertsLoading ? (
                Array.from({ length: 3 }).map((_, i) => (
                  <TableRow key={i} className="border-slate-800/50">
                    <TableCell colSpan={7}>
                      <div className="h-10 bg-slate-800/30 rounded animate-pulse" />
                    </TableCell>
                  </TableRow>
                ))
              ) : alerts.length === 0 ? (
                <TableRow className="border-slate-800/50 hover:bg-transparent">
                  <TableCell colSpan={7} className="py-10">
                    <div className="flex flex-col items-center gap-3 text-center">
                      <div className="w-14 h-14 rounded-full bg-emerald-500/10 border border-emerald-500/30 flex items-center justify-center">
                        <svg className="w-7 h-7 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                        </svg>
                      </div>
                      <div>
                        <p className="text-sm text-emerald-400 font-mono font-medium">All clear!</p>
                        <p className="text-xs text-slate-500 font-mono mt-1">
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
                  const typeConfig = alertTypeConfig[alert.alert_type] || { label: alert.alert_type, icon: '•', color: 'text-slate-400' }
                  const severityStyle = severityStyles[alert.severity] || severityStyles.info

                  return (
                    <TableRow key={alert.alert_id} className="border-slate-800/50 hover:bg-slate-800/30 group">
                      <TableCell>
                        <Badge className={`text-[10px] font-mono border ${severityStyle}`}>
                          {alert.severity}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <span className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-mono border ${style.bg} ${style.accent} ${style.border}`}>
                          {vendorName}
                        </span>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1.5">
                          <span className={`${typeConfig.color} text-sm`}>{typeConfig.icon}</span>
                          <span className="text-xs text-slate-300 font-mono">{typeConfig.label}</span>
                        </div>
                      </TableCell>
                      <TableCell className="max-w-[180px]">
                        <span className="text-xs text-slate-200 truncate block" title={alert.product_name || ''}>
                          {alert.product_name || alert.sku || '—'}
                        </span>
                      </TableCell>
                      <TableCell className="max-w-[200px]">
                        <span className="text-xs text-slate-300 truncate block" title={alert.message}>
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
                              className="h-6 px-2 text-[10px] font-mono text-slate-400 hover:text-slate-100"
                            >
                              View
                            </Button>
                          )}
                          {alert.product_url && (
                            <a
                              href={alert.product_url}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="inline-flex items-center h-6 px-2 text-[10px] font-mono text-sky-400 hover:text-sky-300"
                            >
                              Source ↗
                            </a>
                          )}
                        </div>
                      </TableCell>
                      <TableCell className="font-mono text-[10px] text-slate-400">
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
