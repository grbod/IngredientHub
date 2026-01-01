import { useParams, useNavigate } from 'react-router-dom'
import { useIngredientDetail } from '@/hooks/useIngredientDetail'
import type { PriceTier, InventoryLevel, SimpleInventory } from '@/hooks/useIngredientDetail'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { RefreshButton } from '@/components/UpdateProductDialog'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

function VendorBadge({ vendor }: { vendor: string }) {
  const styles: Record<string, { bg: string; text: string; border: string }> = {
    'IngredientsOnline': { bg: 'bg-sky-50', text: 'text-sky-700', border: 'border-sky-200' },
    'BulkSupplements': { bg: 'bg-emerald-50', text: 'text-emerald-700', border: 'border-emerald-200' },
    'BoxNutra': { bg: 'bg-violet-50', text: 'text-violet-700', border: 'border-violet-200' },
    'TrafaPharma': { bg: 'bg-amber-50', text: 'text-amber-700', border: 'border-amber-200' },
  }
  const style = styles[vendor] || { bg: 'bg-gray-50', text: 'text-gray-700', border: 'border-gray-200' }

  return (
    <span className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-semibold border ${style.bg} ${style.text} ${style.border}`}>
      {vendor}
    </span>
  )
}

function StockBadge({ status }: { status: string | null }) {
  const statusLower = (status || '').toLowerCase()
  if (statusLower === 'in_stock' || statusLower === 'in stock') {
    return (
      <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium bg-green-50 text-green-700 border border-green-200">
        <span className="w-1.5 h-1.5 rounded-full bg-green-500"></span>
        In Stock
      </span>
    )
  }
  if (statusLower === 'out_of_stock' || statusLower === 'out of stock') {
    return (
      <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium bg-red-50 text-red-700 border border-red-200">
        <span className="w-1.5 h-1.5 rounded-full bg-red-500"></span>
        Out of Stock
      </span>
    )
  }
  if (statusLower === 'inquire') {
    return (
      <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium bg-amber-50 text-amber-700 border border-amber-200">
        <span className="w-1.5 h-1.5 rounded-full bg-amber-500"></span>
        Inquire
      </span>
    )
  }
  // Don't show badge for unknown status
  return null
}

function ExternalLinkIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      fill="none"
      stroke="currentColor"
      viewBox="0 0 24 24"
      strokeWidth={2}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M13.5 6H5.25A2.25 2.25 0 003 8.25v10.5A2.25 2.25 0 005.25 21h10.5A2.25 2.25 0 0018 18.75V10.5m-10.5 6L21 3m0 0h-5.25M21 3v5.25"
      />
    </svg>
  )
}

// Map warehouse names to state abbreviations
const WAREHOUSE_TO_STATE: Record<string, string> = {
  'Chino': 'CA',
  'chino': 'CA',
  'Edison': 'NJ',
  'edison': 'NJ',
  'nj': 'NJ',
  'Southwest': 'SW',
  'sw': 'SW',
}

function formatPrice(price: number | null): string {
  if (price === null || price === undefined) return '-'
  return `$${price.toFixed(2)}`
}

function formatPricePerKg(price: number | null): string {
  if (price === null || price === undefined) return '-'
  return `$${price.toFixed(2)}/kg`
}

function formatLastUpdated(dateStr: string | null): string {
  if (!dateStr) return 'Unknown'
  const date = new Date(dateStr)
  if (isNaN(date.getTime())) return 'Unknown'

  const now = new Date()
  const diffMs = now.getTime() - date.getTime()
  const diffMins = Math.floor(diffMs / (1000 * 60))
  const diffHours = Math.floor(diffMs / (1000 * 60 * 60))
  const diffDays = Math.floor(diffHours / 24)

  if (diffMins < 1) return 'Just now'
  if (diffMins < 60) return `${diffMins}m ago`
  if (diffHours < 24) return `${diffHours}h ago`
  if (diffDays < 7) return `${diffDays}d ago`
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

interface VendorPricingCardProps {
  vendorName: string
  priceTiers: PriceTier[]
  warehouseInventory: InventoryLevel[]
  simpleInventory: SimpleInventory[]
  ingredientId?: number
}

function VendorPricingCard({ vendorName, priceTiers, warehouseInventory, simpleInventory, ingredientId }: VendorPricingCardProps) {
  // Detect if this is a tiered pricing vendor (IO) or per-pack vendor (BS/BN/TP)
  const isIOVendor = vendorName === 'IngredientsOnline'

  // Find the most recent last_seen_at timestamp from all price tiers
  const mostRecentUpdate = priceTiers.reduce<string | null>((latest, tier) => {
    if (!tier.last_seen_at) return latest
    if (!latest) return tier.last_seen_at
    return new Date(tier.last_seen_at) > new Date(latest) ? tier.last_seen_at : latest
  }, null)

  // Group price tiers by SKU
  const tiersBySku = new Map<string, PriceTier[]>()
  for (const tier of priceTiers) {
    const key = tier.sku || 'unknown'
    if (!tiersBySku.has(key)) {
      tiersBySku.set(key, [])
    }
    tiersBySku.get(key)!.push(tier)
  }

  // Get inventory for this vendor
  const vendorWarehouseInv = warehouseInventory.filter(inv => inv.vendor_name === vendorName)
  const vendorSimpleInv = simpleInventory.filter(inv => inv.vendor_name === vendorName)

  // Build inventory lookup by SKU
  const warehouseInvBySku = new Map<string, InventoryLevel[]>()
  for (const inv of vendorWarehouseInv) {
    const key = inv.sku || 'unknown'
    if (!warehouseInvBySku.has(key)) {
      warehouseInvBySku.set(key, [])
    }
    warehouseInvBySku.get(key)!.push(inv)
  }

  const simpleInvBySku = new Map<string, SimpleInventory>()
  for (const inv of vendorSimpleInv) {
    const key = inv.sku || 'unknown'
    simpleInvBySku.set(key, inv)
  }

  const hasWarehouseInventory = vendorWarehouseInv.length > 0

  // Render tiered layout for IO (multiple price tiers per SKU)
  const renderTieredLayout = () => (
    <>
      {Array.from(tiersBySku.entries()).map(([sku, tiers]) => {
        const firstTier = tiers[0]
        const skuWarehouseInv = warehouseInvBySku.get(sku) || []

        return (
          <div key={sku} className="border-b border-slate-100 last:border-b-0">
            {/* SKU Header */}
            <div className="px-6 py-3 bg-slate-50/50 border-b border-slate-100">
              <div className="flex items-center gap-3">
                {firstTier.product_url ? (
                  <a
                    href={firstTier.product_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1.5 text-xs bg-slate-100 text-slate-600 px-2 py-1 rounded-md font-mono border border-slate-200 hover:bg-slate-200 hover:text-slate-900 transition-colors"
                  >
                    {sku}
                    <ExternalLinkIcon className="w-3 h-3" />
                  </a>
                ) : (
                  <code className="text-xs bg-slate-100 text-slate-600 px-2 py-1 rounded-md font-mono border border-slate-200">
                    {sku}
                  </code>
                )}
                {firstTier.packaging && (
                  <span className="text-sm text-slate-600">{firstTier.packaging}</span>
                )}
              </div>
            </div>

            {/* Price Tiers Table */}
            <Table>
              <TableHeader>
                <TableRow className="bg-white hover:bg-white">
                  <TableHead className="text-xs font-semibold text-slate-500 uppercase tracking-wider py-2">Min Qty</TableHead>
                  <TableHead className="text-xs font-semibold text-slate-500 uppercase tracking-wider py-2">Price/Pack</TableHead>
                  <TableHead className="text-xs font-semibold text-slate-500 uppercase tracking-wider py-2">Normalized $/kg</TableHead>
                  {hasWarehouseInventory && (
                    <TableHead className="text-xs font-semibold text-slate-500 uppercase tracking-wider py-2">Stock</TableHead>
                  )}
                </TableRow>
              </TableHeader>
              <TableBody>
                {tiers.map((tier, idx) => {
                  // For single-tier IO items, use pack_size as min qty when min_quantity is 0 or 1
                  const isSingleTier = tiers.length === 1
                  const effectiveMinQty = (isSingleTier && (tier.min_quantity === null || tier.min_quantity <= 1))
                    ? tier.pack_size
                    : tier.min_quantity

                  return (
                  <TableRow key={idx} className="hover:bg-slate-50/50">
                    <TableCell className="py-2.5 font-medium text-slate-900">
                      {effectiveMinQty ? `${effectiveMinQty}kg` : '-'}
                    </TableCell>
                    <TableCell className="py-2.5 text-slate-700">
                      {formatPrice(tier.price)}
                    </TableCell>
                    <TableCell className="py-2.5">
                      <span className="font-semibold text-slate-900">
                        {formatPricePerKg(tier.price_per_kg)}
                      </span>
                    </TableCell>
                    {hasWarehouseInventory && idx === 0 && (
                      <TableCell className="py-2.5" rowSpan={tiers.length}>
                        {skuWarehouseInv.length > 0 ? (
                          <div className="space-y-1">
                            {skuWarehouseInv
                              .filter(inv => inv.quantity_available > 0)
                              .sort((a, b) => b.quantity_available - a.quantity_available)
                              .map((inv, i) => {
                                const state = WAREHOUSE_TO_STATE[inv.warehouse] || inv.warehouse
                                return (
                                  <div key={i} className="flex items-center gap-2 text-sm">
                                    <span className="font-medium text-green-600">
                                      {Math.round(inv.quantity_available).toLocaleString()} kg
                                    </span>
                                    <span className="text-slate-400">({state})</span>
                                  </div>
                                )
                              })}
                          </div>
                        ) : (
                          <span className="text-red-500 text-sm">Out of Stock</span>
                        )}
                      </TableCell>
                    )}
                  </TableRow>
                )})}
              </TableBody>
            </Table>
          </div>
        )
      })}
    </>
  )

  // Render flat layout for BS/BN/TP (one row per variant, consolidated table)
  const renderFlatLayout = () => {
    // Get all variants sorted by pack_size
    const allVariants = Array.from(tiersBySku.entries())
      .map(([sku, tiers]) => ({ sku, tier: tiers[0] }))
      .sort((a, b) => (a.tier.pack_size || 0) - (b.tier.pack_size || 0))

    // Helper to format SKU display - convert ugly SKUs to size-based format
    const formatSkuDisplay = (sku: string, packaging: string | null): string => {
      // If SKU looks good, use it
      if (sku && sku !== 'unknown' && !sku.startsWith('None-') && !sku.startsWith('null-')) {
        return sku
      }
      // Otherwise, construct from packaging (e.g., "100 grams" -> "100g")
      if (packaging) {
        // Extract size and convert to compact format
        const match = packaging.match(/^([\d.]+)\s*(grams?|g|kg|kgs?|lb|lbs?|oz)/i)
        if (match) {
          const num = match[1]
          const unit = match[2].toLowerCase()
            .replace('grams', 'g').replace('gram', 'g')
            .replace('kgs', 'kg')
            .replace('lbs', 'lb')
          return `${num}${unit}`
        }
        // For text like "Bulk Price" or "25kgs", just use as-is
        return packaging
      }
      return '-'
    }

    // Check if any SKU has a good display value (not derived from packaging)
    const hasRealSkus = allVariants.some(({ sku }) =>
      sku && sku !== 'unknown' && !sku.startsWith('None-') && !sku.startsWith('null-')
    )

    // Column header: "SKU" if real SKUs exist, "Size" if showing derived sizes
    const skuColumnLabel = hasRealSkus ? 'SKU' : 'Size'

    return (
      <Table>
        <TableHeader>
          <TableRow className="bg-white hover:bg-white">
            <TableHead className="text-xs font-semibold text-slate-500 uppercase tracking-wider py-2">{skuColumnLabel}</TableHead>
            {hasRealSkus && (
              <TableHead className="text-xs font-semibold text-slate-500 uppercase tracking-wider py-2">Pack Size</TableHead>
            )}
            <TableHead className="text-xs font-semibold text-slate-500 uppercase tracking-wider py-2">Price</TableHead>
            <TableHead className="text-xs font-semibold text-slate-500 uppercase tracking-wider py-2">Normalized $/kg</TableHead>
            <TableHead className="text-xs font-semibold text-slate-500 uppercase tracking-wider py-2">Stock</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {allVariants.map(({ sku, tier }, idx) => {
            const skuSimpleInv = simpleInvBySku.get(sku)
            const displaySku = formatSkuDisplay(sku, tier.packaging)

            return (
              <TableRow key={sku} className={idx % 2 === 0 ? 'bg-white' : 'bg-slate-50/30'}>
                <TableCell className="py-2.5">
                  {tier.product_url ? (
                    <a
                      href={tier.product_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1.5 text-xs bg-slate-100 text-slate-600 px-2 py-1 rounded-md font-mono border border-slate-200 hover:bg-slate-200 hover:text-slate-900 transition-colors"
                    >
                      {displaySku}
                      <ExternalLinkIcon className="w-3 h-3" />
                    </a>
                  ) : (
                    <code className="text-xs bg-slate-100 text-slate-600 px-2 py-1 rounded-md font-mono border border-slate-200">
                      {displaySku}
                    </code>
                  )}
                </TableCell>
                {hasRealSkus && (
                  <TableCell className="py-2.5 text-slate-700">
                    {tier.packaging || '-'}
                  </TableCell>
                )}
                <TableCell className="py-2.5 text-slate-700">
                  {formatPrice(tier.price)}
                </TableCell>
                <TableCell className="py-2.5">
                  <span className="font-semibold text-slate-900">
                    {formatPricePerKg(tier.price_per_kg)}
                  </span>
                </TableCell>
                <TableCell className="py-2.5">
                  {skuSimpleInv ? (
                    <StockBadge status={skuSimpleInv.stock_status} />
                  ) : (
                    <span className="text-xs text-slate-400">-</span>
                  )}
                </TableCell>
              </TableRow>
            )
          })}
        </TableBody>
      </Table>
    )
  }

  return (
    <Card className="border-0 shadow-lg shadow-slate-200/50 overflow-hidden max-w-3xl mx-auto">
      <CardHeader className="bg-slate-50/80 border-b border-slate-200 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <VendorBadge vendor={vendorName} />
            <span className="text-sm text-slate-500">
              {tiersBySku.size} variant{tiersBySku.size !== 1 ? 's' : ''}
            </span>
          </div>
          <div className="flex items-center gap-2">
            {mostRecentUpdate && (
              <span className="text-xs text-slate-400">
                Updated {formatLastUpdated(mostRecentUpdate)}
              </span>
            )}
            {priceTiers.length > 0 && (
              <RefreshButton
                vendorIngredientIds={[...new Set(priceTiers.map(t => t.vendor_ingredient_id))]}
                vendorName={vendorName}
                sku={priceTiers[0].sku}
                ingredientId={ingredientId}
              />
            )}
          </div>
        </div>
      </CardHeader>
      <CardContent className="p-0">
        {tiersBySku.size === 0 ? (
          <div className="p-8 text-center text-slate-500">
            No pricing data available
          </div>
        ) : isIOVendor ? (
          renderTieredLayout()
        ) : (
          renderFlatLayout()
        )}
      </CardContent>
    </Card>
  )
}

function LoadingSkeleton() {
  return (
    <div className="space-y-8">
      <div className="h-32 bg-slate-100 rounded-xl animate-pulse"></div>
      <div className="h-64 bg-slate-100 rounded-xl animate-pulse"></div>
      <div className="h-64 bg-slate-100 rounded-xl animate-pulse"></div>
    </div>
  )
}

export function ProductDetail() {
  const { ingredientId } = useParams<{ ingredientId: string }>()
  const navigate = useNavigate()
  const { data: detail, isLoading, error } = useIngredientDetail(
    ingredientId ? parseInt(ingredientId, 10) : undefined
  )

  if (isLoading) {
    return <LoadingSkeleton />
  }

  if (error) {
    return (
      <div className="p-12 text-center">
        <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-red-50 mb-4">
          <svg className="h-8 w-8 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
        </div>
        <h3 className="text-lg font-semibold text-slate-900 mb-1">Failed to load product</h3>
        <p className="text-muted-foreground">{error.message}</p>
        <Button variant="outline" className="mt-4" onClick={() => navigate(-1)}>
          Back to Products
        </Button>
      </div>
    )
  }

  if (!detail) {
    return (
      <div className="p-12 text-center">
        <h3 className="text-lg font-semibold text-slate-900 mb-1">Product not found</h3>
        <Button variant="outline" className="mt-4" onClick={() => navigate(-1)}>
          Back to Products
        </Button>
      </div>
    )
  }

  // Group price tiers by vendor
  const tiersByVendor = new Map<string, PriceTier[]>()
  for (const tier of detail.priceTiers) {
    if (!tiersByVendor.has(tier.vendor_name)) {
      tiersByVendor.set(tier.vendor_name, [])
    }
    tiersByVendor.get(tier.vendor_name)!.push(tier)
  }

  const vendorNames = Array.from(tiersByVendor.keys())

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="relative overflow-hidden rounded-xl hero-gradient hero-shimmer p-8">
        <div className="absolute inset-0 bg-grid-white/5 [mask-image:linear-gradient(0deg,transparent,black)]"></div>
        <div className="relative">
          {/* Back button */}
          <button
            onClick={() => navigate(-1)}
            className="flex items-center gap-2 text-slate-400 hover:text-white transition-colors mb-4"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
            </svg>
            <span className="text-sm">Back to Products</span>
          </button>

          <div className="flex items-start justify-between">
            <div>
              <h1 className="text-3xl font-bold text-white tracking-tight mb-2">
                {detail.name}
              </h1>
              {detail.category_name && (
                <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-white/10 text-slate-200 border border-white/20">
                  {detail.category_name}
                </span>
              )}
            </div>
            <div className="text-right">
              <p className="text-slate-400 text-sm">Available from</p>
              <p className="text-white font-semibold text-2xl">{vendorNames.length} vendor{vendorNames.length !== 1 ? 's' : ''}</p>
            </div>
          </div>
        </div>
      </div>

      {/* Vendor Cards */}
      {vendorNames.length > 0 ? (
        <div className="space-y-6">
          {vendorNames.map((vendorName) => (
            <VendorPricingCard
              key={vendorName}
              vendorName={vendorName}
              priceTiers={tiersByVendor.get(vendorName) || []}
              warehouseInventory={detail.warehouseInventory}
              simpleInventory={detail.simpleInventory}
              ingredientId={ingredientId ? parseInt(ingredientId, 10) : undefined}
            />
          ))}
        </div>
      ) : (
        <Card className="border-0 shadow-lg shadow-slate-200/50">
          <CardContent className="p-12 text-center">
            <div className="w-16 h-16 rounded-full bg-slate-100 flex items-center justify-center mx-auto mb-4">
              <svg className="h-8 w-8 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
              </svg>
            </div>
            <p className="font-medium text-slate-900 mb-1">No pricing data available</p>
            <p className="text-sm text-muted-foreground">This ingredient has no active vendor listings</p>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
