import { useState, useMemo } from 'react'
import { usePriceComparison } from '@/hooks/usePriceComparison'
import { useVendors } from '@/hooks/useVendors'
import { useIngredientDetail } from '@/hooks/useIngredientDetail'
import { Input } from '@/components/ui/input'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { RefreshButton } from '@/components/UpdateProductDialog'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'

function formatPrice(price: number | null) {
  if (price === null) return '-'
  return `$${price.toFixed(2)}`
}

function formatTimeAgo(dateString: string | null): string {
  if (!dateString) return 'Unknown'

  // Database stores UTC timestamps without 'Z' suffix - append it if missing
  const normalizedDate = dateString.endsWith('Z') ? dateString : dateString + 'Z'
  const date = new Date(normalizedDate)
  const now = new Date()
  const diffMs = now.getTime() - date.getTime()
  const diffMins = Math.floor(diffMs / (1000 * 60))
  const diffHours = Math.floor(diffMs / (1000 * 60 * 60))
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24))

  if (diffMins < 60) return `${diffMins}m ago`
  if (diffHours < 24) return `${diffHours}h ago`
  if (diffDays === 1) return '1 day ago'
  if (diffDays < 30) return `${diffDays} days ago`
  return date.toLocaleDateString()
}

const vendorStyles: Record<string, { headerBg: string }> = {
  'IngredientsOnline': { headerBg: 'bg-sky-600' },
  'BulkSupplements': { headerBg: 'bg-emerald-600' },
  'BoxNutra': { headerBg: 'bg-violet-600' },
  'TrafaPharma': { headerBg: 'bg-amber-600' },
}

type SortOption = 'name' | 'savings' | 'vendors'

export function PriceComparison() {
  const [search, setSearch] = useState('')
  const [showMultiVendorOnly, setShowMultiVendorOnly] = useState(false)
  const [sortBy, setSortBy] = useState<SortOption>('vendors')
  const [selectedIngredient, setSelectedIngredient] = useState<{ id: number; name: string } | null>(null)
  const { data: vendors } = useVendors()
  const { data: comparisons, isLoading, error } = usePriceComparison(search || undefined)
  const { data: ingredientDetail, isLoading: isLoadingDetail } = useIngredientDetail(selectedIngredient?.id)

  const vendorNames = vendors?.map(v => v.name) || []

  // Process and sort comparisons
  const { processedComparisons, stats } = useMemo(() => {
    if (!comparisons) return { processedComparisons: [], stats: { multiVendorCount: 0, totalSavings: 0, avgSavingsPercent: 0 } }

    let totalSavings = 0
    let savingsPercents: number[] = []

    const processed = comparisons.map(item => {
      let lowestPrice: number | null = null
      let highestPrice: number | null = null
      let vendorsWithPrice = 0

      for (const v of item.vendors) {
        if (v.best_price_per_kg !== null) {
          vendorsWithPrice++
          if (lowestPrice === null || v.best_price_per_kg < lowestPrice) {
            lowestPrice = v.best_price_per_kg
          }
          if (highestPrice === null || v.best_price_per_kg > highestPrice) {
            highestPrice = v.best_price_per_kg
          }
        }
      }

      const maxSavings = lowestPrice && highestPrice && vendorsWithPrice > 1
        ? highestPrice - lowestPrice
        : 0

      if (maxSavings > 0 && lowestPrice) {
        totalSavings += maxSavings
        savingsPercents.push(Math.round((maxSavings / highestPrice!) * 100))
      }

      return {
        ...item,
        lowestPrice,
        highestPrice,
        vendorsWithPrice,
        maxSavings,
      }
    })

    const multiVendorCount = processed.filter(p => p.vendorsWithPrice > 1).length
    const avgSavingsPercent = savingsPercents.length > 0
      ? Math.round(savingsPercents.reduce((a, b) => a + b, 0) / savingsPercents.length)
      : 0

    // Filter
    const filtered = showMultiVendorOnly
      ? processed.filter(item => item.vendorsWithPrice > 1)
      : processed

    // Sort
    const sorted = filtered.sort((a, b) => {
      if (sortBy === 'vendors') {
        if (b.vendorsWithPrice !== a.vendorsWithPrice) {
          return b.vendorsWithPrice - a.vendorsWithPrice
        }
        return a.ingredient_name.localeCompare(b.ingredient_name)
      }
      if (sortBy === 'savings') {
        return b.maxSavings - a.maxSavings
      }
      return a.ingredient_name.localeCompare(b.ingredient_name)
    })

    return {
      processedComparisons: sorted,
      stats: { multiVendorCount, totalSavings, avgSavingsPercent }
    }
  }, [comparisons, showMultiVendorOnly, sortBy])

  return (
    <div className="space-y-6">
      {/* Hero Header */}
      <div className="relative overflow-hidden rounded-xl hero-gradient hero-shimmer p-8">
        <div className="absolute inset-0 bg-grid-white/5 [mask-image:linear-gradient(0deg,transparent,black)]"></div>
        <div className="relative">
          <div className="flex items-center justify-between">
            <div>
              <div className="flex items-center gap-3 mb-2">
                <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-white/10 backdrop-blur">
                  <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M3 6l3 1m0 0l-3 9a5.002 5.002 0 006.001 0M6 7l3 9M6 7l6-2m6 2l3-1m-3 1l-3 9a5.002 5.002 0 006.001 0M18 7l3 9m-3-9l-6-2m0-2v2m0 16V5m0 16H9m3 0h3" />
                  </svg>
                </div>
                <h1 className="text-3xl font-bold text-white tracking-tight">Price Comparison</h1>
              </div>
              <p className="text-slate-300 max-w-2xl">
                Find the best $/kg across {vendorNames.length} vendors
              </p>
            </div>
            <div className="flex items-center gap-2 text-sm">
              <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-green-500/20 text-green-300 font-medium border border-green-500/30">
                <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                </svg>
                Best
              </span>
              <span className="text-slate-400">= lowest $/kg</span>
            </div>
          </div>
        </div>
        <div className="absolute right-8 top-1/2 -translate-y-1/2 opacity-10">
          <svg className="w-32 h-32 text-white" fill="currentColor" viewBox="0 0 24 24">
            <path d="M3 6l3 1m0 0l-3 9a5.002 5.002 0 006.001 0M6 7l3 9M6 7l6-2m6 2l3-1m-3 1l-3 9a5.002 5.002 0 006.001 0M18 7l3 9m-3-9l-6-2m0-2v2m0 16V5m0 16H9m3 0h3" />
          </svg>
        </div>
      </div>

      {/* Search + Filters */}
      <div className="flex gap-3 items-center">
        <div className="relative flex-1">
          <div className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
          </div>
          <Input
            placeholder="Search ingredients..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="pl-9 h-9 border-slate-200"
          />
          {search && (
            <button
              onClick={() => setSearch('')}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          )}
        </div>

        <button
          onClick={() => setShowMultiVendorOnly(!showMultiVendorOnly)}
          className={`h-9 px-3 text-xs font-medium rounded-md border transition-all ${
            showMultiVendorOnly
              ? 'bg-blue-600 text-white border-blue-600'
              : 'bg-white text-slate-600 border-slate-200 hover:border-slate-300'
          }`}
        >
          <span className="flex items-center gap-1.5">
            {showMultiVendorOnly && (
              <svg className="w-3.5 h-3.5" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
              </svg>
            )}
            Multi-vendor only
          </span>
        </button>

        <div className="flex items-center border rounded-md overflow-hidden bg-slate-50">
          {[
            { key: 'vendors', label: 'By Vendors' },
            { key: 'savings', label: 'By Savings' },
            { key: 'name', label: 'A-Z' },
          ].map(({ key, label }) => (
            <button
              key={key}
              onClick={() => setSortBy(key as SortOption)}
              className={`px-3 py-1.5 text-xs transition-colors ${
                sortBy === key
                  ? 'bg-white text-slate-900 shadow-sm font-medium'
                  : 'text-slate-500 hover:text-slate-700'
              }`}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {/* Stats Summary - only when we have multi-vendor items */}
      {stats.multiVendorCount > 0 && (
        <div className="grid grid-cols-3 gap-3">
          <div className="bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg p-3 border border-blue-200">
            <div className="text-2xl font-bold text-blue-700">{stats.multiVendorCount}</div>
            <div className="text-xs text-blue-600">Items with competition</div>
          </div>
          <div className="bg-gradient-to-br from-green-50 to-green-100 rounded-lg p-3 border border-green-200">
            <div className="text-2xl font-bold text-green-700">{stats.avgSavingsPercent}%</div>
            <div className="text-xs text-green-600">Avg savings by shopping smart</div>
          </div>
          <div className="bg-gradient-to-br from-amber-50 to-amber-100 rounded-lg p-3 border border-amber-200">
            <div className="text-2xl font-bold text-amber-700">${stats.totalSavings.toFixed(0)}</div>
            <div className="text-xs text-amber-600">Max savings/kg across all</div>
          </div>
        </div>
      )}

      {/* Results */}
      {error ? (
        <Card className="border-red-200 bg-red-50">
          <CardContent className="p-4 text-center text-red-600">
            Error: {error.message}
          </CardContent>
        </Card>
      ) : isLoading ? (
        <Card>
          <CardContent className="p-8 text-center">
            <div className="animate-spin h-6 w-6 border-2 border-slate-200 border-t-slate-600 rounded-full mx-auto mb-2"></div>
            <p className="text-sm text-slate-500">Loading...</p>
          </CardContent>
        </Card>
      ) : processedComparisons.length === 0 ? (
        <Card>
          <CardContent className="p-8 text-center">
            <div className="w-12 h-12 rounded-full bg-slate-100 flex items-center justify-center mx-auto mb-3">
              <svg className="w-6 h-6 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
            </div>
            <p className="font-medium text-slate-700 mb-1">
              {showMultiVendorOnly ? 'No multi-vendor matches' : 'No ingredients found'}
            </p>
            <p className="text-sm text-slate-500">
              {showMultiVendorOnly
                ? 'Try disabling the filter or searching for something else'
                : 'Try a different search term'}
            </p>
          </CardContent>
        </Card>
      ) : (
        <Card className="border-0 shadow-md overflow-hidden">
          <div className="overflow-x-auto max-h-[calc(100vh-340px)]">
            <table className="w-full text-sm">
              <thead className="sticky top-0 z-10">
                <tr>
                  <th className="text-left font-semibold text-slate-700 py-2 px-3 bg-slate-100 border-b border-slate-200 min-w-[280px]">
                    <span className="flex items-center gap-2">
                      Ingredient
                      <span className="text-xs font-normal text-slate-400 bg-slate-200 px-1.5 py-0.5 rounded">
                        {processedComparisons.length}
                      </span>
                    </span>
                  </th>
                  {vendorNames.map((vendor) => {
                    const style = vendorStyles[vendor] || { headerBg: 'bg-gray-600' }
                    return (
                      <th
                        key={vendor}
                        className={`font-semibold py-2 px-3 text-center text-white min-w-[110px] ${style.headerBg}`}
                      >
                        <div className="text-xs">{vendor.replace('IngredientsOnline', 'IO').replace('BulkSupplements', 'BulkSupps')}</div>
                        <div className="text-[10px] font-normal opacity-80">$/kg</div>
                      </th>
                    )
                  })}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {processedComparisons.map((item, idx) => {
                  const { lowestPrice, vendorsWithPrice, maxSavings } = item
                  const hasCompetition = vendorsWithPrice > 1

                  const vendorPrices = new Map(
                    item.vendors.map(v => [v.vendor_name, v])
                  )

                  return (
                    <tr
                      key={item.ingredient_id}
                      className={`transition-colors ${
                        idx % 2 === 0 ? 'bg-white' : 'bg-slate-50/30'
                      } ${hasCompetition ? 'hover:bg-blue-50/50' : 'hover:bg-slate-50'}`}
                    >
                      <td className="py-1.5 px-3">
                        <div className="flex items-center gap-2">
                          {hasCompetition ? (
                            <span className="flex-shrink-0 w-5 h-5 rounded-full bg-blue-500 text-white text-[10px] font-bold flex items-center justify-center">
                              {vendorsWithPrice}
                            </span>
                          ) : (
                            <span className="flex-shrink-0 w-5 h-5 rounded-full bg-slate-200 text-slate-500 text-[10px] font-medium flex items-center justify-center">
                              1
                            </span>
                          )}
                          <span className={`font-medium ${hasCompetition ? 'text-slate-900' : 'text-slate-500'}`}>
                            {item.ingredient_name}
                          </span>
                          {hasCompetition && maxSavings > 0 && (
                            <span className="text-[10px] text-green-600 bg-green-50 px-1.5 py-0.5 rounded font-medium">
                              Save ${maxSavings.toFixed(0)}
                            </span>
                          )}
                        </div>
                      </td>
                      {vendorNames.map((vendorName) => {
                        const vendor = vendorPrices.get(vendorName)
                        const price = vendor?.best_price_per_kg ?? null
                        const isLowest = price !== null && price === lowestPrice
                        const showBest = isLowest && hasCompetition
                        const priceDiff = price && lowestPrice && !isLowest ? price - lowestPrice : null
                        const priceDiffPercent = priceDiff && lowestPrice ? Math.round((priceDiff / lowestPrice) * 100) : null

                        return (
                          <td
                            key={vendorName}
                            className={`py-1.5 px-3 text-center transition-colors ${showBest ? 'bg-green-100' : ''}`}
                          >
                            {vendor && price !== null ? (
                              <button
                                onClick={() => setSelectedIngredient({ id: item.ingredient_id, name: item.ingredient_name })}
                                className="group flex items-center justify-center gap-1 w-full hover:underline cursor-pointer"
                              >
                                {showBest && (
                                  <svg className="w-3.5 h-3.5 text-green-600 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                                    <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                                  </svg>
                                )}
                                <span className={showBest ? 'font-bold text-green-700' : 'text-slate-700'}>
                                  {formatPrice(price)}
                                </span>
                                {priceDiff !== null && priceDiffPercent !== null && priceDiffPercent > 0 && (
                                  <span className="text-[10px] text-red-500 font-medium">
                                    +{priceDiffPercent}%
                                  </span>
                                )}
                                <svg
                                  className="w-3 h-3 text-slate-400 opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0"
                                  fill="none"
                                  stroke="currentColor"
                                  viewBox="0 0 24 24"
                                >
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                                </svg>
                              </button>
                            ) : (
                              <span className="text-slate-200">-</span>
                            )}
                          </td>
                        )
                      })}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {/* Product Detail Modal */}
      <Dialog open={!!selectedIngredient} onOpenChange={(open) => !open && setSelectedIngredient(null)}>
        <DialogContent className="sm:max-w-2xl max-h-[85vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="text-xl">{selectedIngredient?.name}</DialogTitle>
          </DialogHeader>

          {isLoadingDetail ? (
            <div className="flex items-center justify-center py-12">
              <div className="animate-spin h-8 w-8 border-2 border-slate-200 border-t-slate-600 rounded-full"></div>
            </div>
          ) : ingredientDetail ? (
            <div className="space-y-4">
              {/* Group price tiers by vendor */}
              {(() => {
                const vendorGroups = new Map<string, typeof ingredientDetail.priceTiers>()
                for (const tier of ingredientDetail.priceTiers) {
                  const existing = vendorGroups.get(tier.vendor_name) || []
                  existing.push(tier)
                  vendorGroups.set(tier.vendor_name, existing)
                }

                return Array.from(vendorGroups.entries()).map(([vendorName, tiers]) => {
                  const isIO = vendorName === 'IngredientsOnline'
                  const style = vendorStyles[vendorName] || { headerBg: 'bg-gray-600' }

                  // Get inventory info
                  const warehouseInv = ingredientDetail.warehouseInventory.filter(
                    inv => inv.vendor_name === vendorName
                  )

                  // Get most recent last_seen_at for this vendor
                  const mostRecentUpdate = tiers.reduce((latest, tier) => {
                    if (!tier.last_seen_at) return latest
                    if (!latest) return tier.last_seen_at
                    return new Date(tier.last_seen_at) > new Date(latest) ? tier.last_seen_at : latest
                  }, null as string | null)

                  return (
                    <div key={vendorName} className="border rounded-lg overflow-hidden">
                      <div className={`${style.headerBg} text-white px-4 py-2 font-semibold text-sm flex items-center justify-between`}>
                        <span>{vendorName}</span>
                        <div className="flex items-center gap-2">
                          {mostRecentUpdate && (
                            <span className="text-xs font-normal opacity-80">
                              Updated {formatTimeAgo(mostRecentUpdate)}
                            </span>
                          )}
                          {tiers.length > 0 && (
                            <RefreshButton
                              vendorIngredientIds={[...new Set(tiers.map(t => t.vendor_ingredient_id))]}
                              vendorName={vendorName}
                              sku={tiers[0].sku}
                              ingredientId={selectedIngredient?.id}
                              variant="ghost"
                              className="h-6 w-6 p-0 text-white/70 hover:text-white hover:bg-white/20"
                            />
                          )}
                        </div>
                      </div>
                      <div className="p-3">
                        <table className="w-full text-sm">
                          <thead>
                            <tr className="text-slate-500 text-xs">
                              <th className="text-left pb-2 font-medium">Size</th>
                              {isIO && <th className="text-right pb-2 font-medium">Min Qty</th>}
                              <th className="text-right pb-2 font-medium">Price</th>
                              <th className="text-right pb-2 font-medium">$/kg</th>
                              <th className="text-right pb-2 font-medium">Stock</th>
                              {tiers.some(t => t.product_url) && <th className="w-8"></th>}
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-slate-100">
                            {tiers.map((tier, idx) => {
                              // For IO single-tier items, min qty defaults to pack size
                              const effectiveMinQty = isIO
                                ? (tier.min_quantity && tier.min_quantity > 1 ? tier.min_quantity : tier.pack_size)
                                : tier.min_quantity
                              // For IO single-tier items, show total price ($/kg × pack_size)
                              const isSingleTierIO = isIO && tiers.length === 1
                              const displayPrice = isSingleTierIO && tier.price_per_kg && tier.pack_size
                                ? tier.price_per_kg * tier.pack_size
                                : tier.price

                              return (
                              <tr key={idx} className="group hover:bg-slate-50">
                                <td className="py-1.5 text-slate-700">{tier.packaging || '-'}</td>
                                {isIO && (
                                  <td className="py-1.5 text-right text-slate-600">
                                    {effectiveMinQty ? `${effectiveMinQty}kg` : '-'}
                                  </td>
                                )}
                                <td className="py-1.5 text-right font-medium text-slate-900">
                                  {displayPrice != null ? `$${displayPrice.toFixed(2)}` : '-'}
                                </td>
                                <td className="py-1.5 text-right text-slate-600">
                                  {tier.price_per_kg ? `$${tier.price_per_kg.toFixed(2)}` : '-'}
                                </td>
                                <td className="py-1.5 text-right">
                                  {(() => {
                                    // For IO, show warehouse inventory stacked vertically
                                    if (isIO && warehouseInv.length > 0) {
                                      return (
                                        <div className="flex flex-col gap-0.5 items-end">
                                          {warehouseInv.map((inv, idx) => (
                                            <span
                                              key={idx}
                                              className={`text-xs px-2 py-0.5 rounded ${
                                                inv.quantity_available > 0
                                                  ? 'bg-green-50 text-green-700'
                                                  : 'bg-slate-100 text-slate-500'
                                              }`}
                                            >
                                              {inv.warehouse}: {inv.quantity_available}kg
                                            </span>
                                          ))}
                                        </div>
                                      )
                                    }
                                    // For other vendors, show simple stock status
                                    const tierStock = ingredientDetail.simpleInventory.find(
                                      inv => inv.vendor_ingredient_id === tier.vendor_ingredient_id
                                    )
                                    if (!tierStock) return <span className="text-slate-300">-</span>
                                    return (
                                      <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${
                                        tierStock.stock_status === 'in_stock'
                                          ? 'bg-green-100 text-green-700'
                                          : tierStock.stock_status === 'out_of_stock'
                                          ? 'bg-red-100 text-red-700'
                                          : tierStock.stock_status === 'inquire'
                                          ? 'bg-amber-100 text-amber-700'
                                          : 'bg-slate-100 text-slate-500'
                                      }`}>
                                        {tierStock.stock_status === 'in_stock' ? 'In Stock' :
                                         tierStock.stock_status === 'out_of_stock' ? 'Out of Stock' :
                                         tierStock.stock_status === 'inquire' ? 'Inquire' : 'Unknown'}
                                      </span>
                                    )
                                  })()}
                                </td>
                                {tiers.some(t => t.product_url) && (
                                  <td className="py-1.5 text-right">
                                    {tier.product_url ? (
                                      <a
                                        href={tier.product_url}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="inline-flex items-center justify-center w-6 h-6 rounded-md text-slate-400 hover:text-slate-700 hover:bg-slate-100 opacity-50 group-hover:opacity-100 transition-all"
                                        title="Open on vendor site"
                                      >
                                        <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                                        </svg>
                                      </a>
                                    ) : (
                                      <span className="w-6 h-6 inline-block"></span>
                                    )}
                                  </td>
                                )}
                              </tr>
                              )
                            })}
                          </tbody>
                        </table>

                        {/* Warehouse inventory for non-IO vendors (IO shows inline in Stock column) */}
                        {!isIO && warehouseInv.length > 0 && (
                          <div className="mt-3 pt-3 border-t border-slate-100">
                            <div className="text-xs text-slate-500 mb-2">Warehouse Inventory</div>
                            <div className="flex flex-wrap gap-2">
                              {warehouseInv.map((inv, idx) => (
                                <span
                                  key={idx}
                                  className={`text-xs px-2 py-1 rounded ${
                                    inv.quantity_available > 0
                                      ? 'bg-green-50 text-green-700'
                                      : 'bg-slate-100 text-slate-500'
                                  }`}
                                >
                                  {inv.warehouse}: {inv.quantity_available}kg
                                </span>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  )
                })
              })()}

              <div className="flex justify-end pt-2 border-t">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => window.open(`/products/${selectedIngredient?.id}`, '_blank')}
                >
                  Open Full Detail →
                </Button>
              </div>
            </div>
          ) : (
            <div className="text-center py-8 text-slate-500">No pricing data available</div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
