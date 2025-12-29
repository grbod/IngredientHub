import { useState, useMemo } from 'react'
import { usePriceComparison } from '@/hooks/usePriceComparison'
import { useVendors } from '@/hooks/useVendors'
import { Input } from '@/components/ui/input'
import { Card, CardContent } from '@/components/ui/card'

function formatPrice(price: number | null) {
  if (price === null) return '-'
  return `$${price.toFixed(2)}`
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
  const { data: vendors } = useVendors()
  const { data: comparisons, isLoading, error } = usePriceComparison(search || undefined)

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
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Price Comparison</h1>
          <p className="text-sm text-slate-500">
            Find the best $/kg across {vendorNames.length} vendors
          </p>
        </div>
        <div className="flex items-center gap-2 text-xs">
          <span className="inline-flex items-center gap-1 px-2 py-1 rounded bg-green-100 text-green-700 font-medium">
            <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
              <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
            </svg>
            Best
          </span>
          <span className="text-slate-400">= lowest $/kg</span>
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
                        className={`font-semibold py-2 px-3 text-center text-white text-xs min-w-[110px] ${style.headerBg}`}
                      >
                        {vendor.replace('IngredientsOnline', 'IO').replace('BulkSupplements', 'BulkSupps')}
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
                        const price = vendor?.best_price_per_kg
                        const isLowest = price !== null && price !== undefined && price === lowestPrice
                        const showBest = isLowest && hasCompetition
                        const priceDiff = price && lowestPrice && !isLowest ? price - lowestPrice : null
                        const priceDiffPercent = priceDiff && lowestPrice ? Math.round((priceDiff / lowestPrice) * 100) : null

                        return (
                          <td
                            key={vendorName}
                            className={`py-1.5 px-3 text-center transition-colors ${showBest ? 'bg-green-100' : ''}`}
                          >
                            {vendor && price !== null ? (
                              <div className="flex items-center justify-center gap-1">
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
                              </div>
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
    </div>
  )
}
