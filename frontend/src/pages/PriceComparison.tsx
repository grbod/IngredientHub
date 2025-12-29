import { useState } from 'react'
import { usePriceComparison } from '@/hooks/usePriceComparison'
import { useVendors } from '@/hooks/useVendors'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

function formatPrice(price: number | null) {
  if (price === null) return 'N/A'
  return `$${price.toFixed(2)}/kg`
}

export function PriceComparison() {
  const [search, setSearch] = useState('')
  const { data: vendors } = useVendors()
  const { data: comparisons, isLoading, error } = usePriceComparison(search || undefined)

  const vendorColors: Record<string, string> = {
    'IngredientsOnline': 'bg-blue-100 text-blue-800',
    'BulkSupplements': 'bg-green-100 text-green-800',
    'BoxNutra': 'bg-purple-100 text-purple-800',
    'TrafaPharma': 'bg-orange-100 text-orange-800',
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Price Comparison</h1>
        <p className="text-muted-foreground">Compare ingredient prices across vendors</p>
      </div>

      <div className="flex gap-4 items-center">
        <Input
          placeholder="Search ingredients..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="max-w-sm"
        />
        <div className="flex gap-2">
          {vendors?.map((v) => (
            <Badge
              key={v.vendor_id}
              variant="outline"
              className={vendorColors[v.name] || ''}
            >
              {v.name}
            </Badge>
          ))}
        </div>
      </div>

      {error ? (
        <div className="text-destructive">Error: {error.message}</div>
      ) : isLoading ? (
        <div className="text-muted-foreground">Loading price comparisons...</div>
      ) : comparisons?.length === 0 ? (
        <div className="text-muted-foreground">No ingredients found</div>
      ) : (
        <div className="grid gap-4">
          {comparisons?.map((item) => {
            const lowestPrice = item.vendors[0]?.best_price_per_kg

            return (
              <Card key={item.ingredient_id}>
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg">{item.ingredient_name}</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                    {item.vendors.map((vendor) => {
                      const isLowest = vendor.best_price_per_kg === lowestPrice && lowestPrice !== null
                      return (
                        <div
                          key={vendor.vendor_id}
                          className={`p-3 rounded-lg border ${
                            isLowest ? 'border-green-500 bg-green-50' : 'border-border'
                          }`}
                        >
                          <div className="flex items-center justify-between mb-2">
                            <Badge
                              variant="outline"
                              className={vendorColors[vendor.vendor_name] || ''}
                            >
                              {vendor.vendor_name}
                            </Badge>
                            {isLowest && (
                              <Badge variant="default" className="bg-green-600">
                                Best
                              </Badge>
                            )}
                          </div>
                          <div className="text-xl font-bold">
                            {formatPrice(vendor.best_price_per_kg)}
                          </div>
                          <div className="text-xs text-muted-foreground mt-1 truncate">
                            {vendor.product_name || vendor.sku || 'No product name'}
                          </div>
                          {vendor.min_order_qty && (
                            <div className="text-xs text-muted-foreground">
                              Min: {vendor.min_order_qty} kg
                            </div>
                          )}
                        </div>
                      )
                    })}
                  </div>
                </CardContent>
              </Card>
            )
          })}
        </div>
      )}
    </div>
  )
}
