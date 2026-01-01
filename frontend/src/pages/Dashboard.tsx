import { useState, useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { useVendorStats } from '@/hooks/useVendors'

/**
 * Custom hook for animating a number from 0 to a target value.
 * Uses requestAnimationFrame for smooth 60fps animation with ease-out cubic easing.
 */
function useCountAnimation(targetValue: number, duration = 1800): number {
  const [count, setCount] = useState(0)
  const hasAnimated = useRef(false)

  useEffect(() => {
    // Only animate once when we first get a non-zero target value
    if (targetValue === 0 || hasAnimated.current) return

    hasAnimated.current = true
    const startTime = performance.now()

    const animate = (currentTime: number) => {
      const elapsed = currentTime - startTime
      const progress = Math.min(elapsed / duration, 1)

      // Ease out cubic for nice deceleration at the end
      const easeOut = 1 - Math.pow(1 - progress, 3)
      const current = Math.floor(easeOut * targetValue)

      setCount(current)

      if (progress < 1) {
        requestAnimationFrame(animate)
      } else {
        // Ensure we end on the exact target value
        setCount(targetValue)
      }
    }

    requestAnimationFrame(animate)
  }, [targetValue, duration])

  return count
}

const vendorColors: Record<string, { bg: string; border: string; accent: string; icon: string }> = {
  'IngredientsOnline': { bg: 'bg-sky-50', border: 'border-sky-200', accent: 'text-sky-600', icon: 'bg-sky-500' },
  'BulkSupplements': { bg: 'bg-emerald-50', border: 'border-emerald-200', accent: 'text-emerald-600', icon: 'bg-emerald-500' },
  'BoxNutra': { bg: 'bg-violet-50', border: 'border-violet-200', accent: 'text-violet-600', icon: 'bg-violet-500' },
  'TrafaPharma': { bg: 'bg-amber-50', border: 'border-amber-200', accent: 'text-amber-600', icon: 'bg-amber-500' },
}

const pricingModelLabels: Record<string, string> = {
  'per_unit': 'Tiered $/kg pricing',
  'per_package': 'Per-package pricing',
  'per_size': 'Per-size pricing',
}

function formatDate(dateStr: string | null) {
  if (!dateStr) return 'Never'
  const date = new Date(dateStr)
  const now = new Date()
  const diffMs = now.getTime() - date.getTime()
  const diffHours = Math.floor(diffMs / (1000 * 60 * 60))
  const diffDays = Math.floor(diffHours / 24)

  if (diffHours < 1) return 'Just now'
  if (diffHours < 24) return `${diffHours}h ago`
  if (diffDays < 7) return `${diffDays}d ago`
  return date.toLocaleDateString()
}

function getFreshnessIndicator(lastScraped: string | null): { color: string; pulse: boolean } {
  if (!lastScraped) return { color: 'bg-red-500', pulse: false }
  const date = new Date(lastScraped)
  const now = new Date()
  const diffHours = (now.getTime() - date.getTime()) / (1000 * 60 * 60)

  if (diffHours < 24) return { color: 'bg-green-500', pulse: true }
  if (diffHours < 72) return { color: 'bg-yellow-500', pulse: false }
  return { color: 'bg-red-500', pulse: false }
}

export function Dashboard() {
  const navigate = useNavigate()
  const { data: stats, isLoading, error } = useVendorStats()

  // Calculate totals (will be 0 while loading)
  const totalProducts = stats?.reduce((sum, v) => sum + v.productCount, 0) || 0
  const totalVariants = stats?.reduce((sum, v) => sum + v.variantCount, 0) || 0
  const activeVendors = stats?.length || 0

  // Animated values for the stats cards - hooks must be called unconditionally
  const animatedProducts = useCountAnimation(totalProducts)
  const animatedVariants = useCountAnimation(totalVariants)
  const animatedVendors = useCountAnimation(activeVendors, 1200) // Faster for small numbers

  if (isLoading) {
    return (
      <div className="space-y-8">
        {/* Header skeleton */}
        <div className="relative overflow-hidden rounded-xl hero-gradient p-8">
          <div className="h-10 w-48 bg-white/10 rounded animate-pulse mb-2"></div>
          <div className="h-5 w-64 bg-white/10 rounded animate-pulse"></div>
        </div>
        {/* Stats skeleton */}
        <div className="grid gap-4 md:grid-cols-3">
          {[1, 2, 3].map(i => (
            <div key={i} className="h-32 bg-slate-100 rounded-xl animate-pulse"></div>
          ))}
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <div className="w-16 h-16 rounded-full bg-red-50 flex items-center justify-center mx-auto mb-4">
            <svg className="h-8 w-8 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
            </svg>
          </div>
          <p className="text-red-600 font-medium">Error loading data</p>
          <p className="text-sm text-slate-500 mt-1">{error.message}</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-8">
      {/* Hero Header */}
      <div className="relative overflow-hidden rounded-xl hero-gradient hero-shimmer p-8">
        <div className="absolute inset-0 bg-grid-white/5 [mask-image:linear-gradient(0deg,transparent,black)]"></div>
        <div className="relative">
          <div className="flex items-center gap-3 mb-2">
            <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-white/10 backdrop-blur">
              <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6" />
              </svg>
            </div>
            <p className="text-white text-sm">HEY</p>
            <h1 className="text-3xl font-bold text-white tracking-tight">Dashboard</h1>
          </div>
          <p className="text-slate-300 max-w-2xl">
            Real-time overview of {stats?.length || 0} vendor scrapers tracking {totalProducts.toLocaleString()} products
          </p>
        </div>
        <div className="absolute right-8 top-1/2 -translate-y-1/2 opacity-10">
          <svg className="w-32 h-32 text-white" fill="currentColor" viewBox="0 0 24 24">
            <path d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6" />
          </svg>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid gap-4 md:grid-cols-3">
        <div className="relative overflow-hidden rounded-xl border border-slate-200 bg-white p-6 shadow-sm hover:shadow-md transition-shadow">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-sm font-medium text-slate-500">Total Products</p>
              <p className="text-3xl font-bold text-slate-900 mt-1 tabular-nums">{animatedProducts.toLocaleString()}</p>
              <p className="text-xs text-slate-400 mt-2">Unique ingredients tracked</p>
            </div>
            <div className="flex items-center justify-center w-12 h-12 rounded-xl bg-blue-50">
              <svg className="w-6 h-6 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4" />
              </svg>
            </div>
          </div>
        </div>

        <div className="relative overflow-hidden rounded-xl border border-slate-200 bg-white p-6 shadow-sm hover:shadow-md transition-shadow">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-sm font-medium text-slate-500">Size Variants</p>
              <p className="text-3xl font-bold text-slate-900 mt-1 tabular-nums">{animatedVariants.toLocaleString()}</p>
              <p className="text-xs text-slate-400 mt-2">Different packaging options</p>
            </div>
            <div className="flex items-center justify-center w-12 h-12 rounded-xl bg-purple-50">
              <svg className="w-6 h-6 text-purple-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
              </svg>
            </div>
          </div>
        </div>

        <div className="relative overflow-hidden rounded-xl border border-slate-200 bg-white p-6 shadow-sm hover:shadow-md transition-shadow">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-sm font-medium text-slate-500">Active Vendors</p>
              <p className="text-3xl font-bold text-slate-900 mt-1 tabular-nums">{animatedVendors.toLocaleString()}</p>
              <p className="text-xs text-slate-400 mt-2">Connected data sources</p>
            </div>
            <div className="flex items-center justify-center w-12 h-12 rounded-xl bg-green-50">
              <svg className="w-6 h-6 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
              </svg>
            </div>
          </div>
        </div>
      </div>

      {/* Vendor Cards */}
      <div>
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-slate-900">Vendors</h2>
          <button
            onClick={() => navigate('/compare')}
            className="text-sm text-blue-600 hover:text-blue-700 font-medium flex items-center gap-1"
          >
            Compare prices
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
            </svg>
          </button>
        </div>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          {stats?.map((vendor) => {
            const colors = vendorColors[vendor.name] || { bg: 'bg-slate-50', border: 'border-slate-200', accent: 'text-slate-600', icon: 'bg-slate-500' }
            const freshness = getFreshnessIndicator(vendor.lastScraped)

            return (
              <div
                key={vendor.vendor_id}
                onClick={() => navigate(`/products?vendor=${vendor.name}`)}
                className={`relative overflow-hidden rounded-xl border ${colors.border} ${colors.bg} p-5 cursor-pointer hover:shadow-md transition-all group`}
              >
                {/* Vendor icon indicator */}
                <div className={`absolute top-0 left-0 w-1 h-full ${colors.icon}`}></div>

                <div className="flex items-start justify-between mb-4">
                  <div>
                    <h3 className="font-semibold text-slate-900 group-hover:text-slate-700">{vendor.name}</h3>
                    <p className={`text-xs ${colors.accent} mt-0.5`}>
                      {pricingModelLabels[vendor.pricing_model || ''] || vendor.pricing_model}
                    </p>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <span className={`w-2 h-2 rounded-full ${freshness.color} ${freshness.pulse ? 'animate-pulse' : ''}`}></span>
                    <span className="text-xs text-slate-500">{formatDate(vendor.lastScraped)}</span>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-3">
                  <div className="bg-white/60 rounded-lg p-2.5">
                    <p className="text-2xl font-bold text-slate-900">{vendor.productCount.toLocaleString()}</p>
                    <p className="text-xs text-slate-500">Products</p>
                  </div>
                  <div className="bg-white/60 rounded-lg p-2.5">
                    <p className="text-2xl font-bold text-slate-900">{vendor.variantCount.toLocaleString()}</p>
                    <p className="text-xs text-slate-500">Variants</p>
                  </div>
                </div>

                {/* Hover arrow */}
                <div className="absolute bottom-3 right-3 opacity-0 group-hover:opacity-100 transition-opacity">
                  <svg className={`w-5 h-5 ${colors.accent}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                  </svg>
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}
