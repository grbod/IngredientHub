import { useState } from 'react'
import { useProducts } from '@/hooks/useProducts'
import { useVendors } from '@/hooks/useVendors'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

function formatPrice(price: number | null) {
  if (price === null) return '-'
  return `$${price.toFixed(2)}`
}

function formatDate(dateStr: string | null) {
  if (!dateStr) return '-'
  return new Date(dateStr).toLocaleDateString()
}

export function Products() {
  const [search, setSearch] = useState('')
  const [vendorFilter, setVendorFilter] = useState<string>('all')
  const [page, setPage] = useState(0)
  const pageSize = 50

  const { data: vendors } = useVendors()
  const { data, isLoading, error } = useProducts({
    vendorId: vendorFilter !== 'all' ? Number(vendorFilter) : undefined,
    search: search || undefined,
    limit: pageSize,
    offset: page * pageSize,
  })

  const products = data?.data || []

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Products</h1>
        <p className="text-muted-foreground">Browse all scraped products</p>
      </div>

      <div className="flex gap-4 items-center">
        <Input
          placeholder="Search products..."
          value={search}
          onChange={(e) => {
            setSearch(e.target.value)
            setPage(0)
          }}
          className="max-w-sm"
        />
        <Select value={vendorFilter} onValueChange={(v) => { setVendorFilter(v); setPage(0) }}>
          <SelectTrigger className="w-[180px]">
            <SelectValue placeholder="All Vendors" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Vendors</SelectItem>
            {vendors?.map((v) => (
              <SelectItem key={v.vendor_id} value={v.vendor_id.toString()}>
                {v.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {error ? (
        <div className="text-destructive">Error: {error.message}</div>
      ) : isLoading ? (
        <div className="text-muted-foreground">Loading products...</div>
      ) : (
        <>
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Product</TableHead>
                  <TableHead>SKU</TableHead>
                  <TableHead>Vendor</TableHead>
                  <TableHead>Price/kg</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Last Seen</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {products.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center text-muted-foreground">
                      No products found
                    </TableCell>
                  </TableRow>
                ) : (
                  products.map((product) => {
                    const bestPrice = product.price_tiers?.reduce((best, tier) => {
                      if (!tier.price_per_kg) return best
                      if (!best || tier.price_per_kg < best) return tier.price_per_kg
                      return best
                    }, null as number | null)

                    return (
                      <TableRow key={product.vendor_ingredient_id}>
                        <TableCell className="max-w-[300px]">
                          <div className="truncate font-medium">
                            {product.raw_product_name || product.variant?.variant_name || '-'}
                          </div>
                          <div className="text-xs text-muted-foreground truncate">
                            {product.variant?.ingredient?.name}
                          </div>
                        </TableCell>
                        <TableCell className="font-mono text-sm">
                          {product.sku || '-'}
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline">{product.vendor?.name}</Badge>
                        </TableCell>
                        <TableCell className="font-medium">
                          {formatPrice(bestPrice)}
                        </TableCell>
                        <TableCell>
                          <Badge variant={product.status === 'active' ? 'default' : 'secondary'}>
                            {product.status || 'unknown'}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-muted-foreground">
                          {formatDate(product.last_seen_at)}
                        </TableCell>
                      </TableRow>
                    )
                  })
                )}
              </TableBody>
            </Table>
          </div>

          <div className="flex items-center justify-between">
            <div className="text-sm text-muted-foreground">
              Showing {page * pageSize + 1} - {page * pageSize + products.length} products
            </div>
            <div className="flex gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setPage((p) => Math.max(0, p - 1))}
                disabled={page === 0}
              >
                Previous
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setPage((p) => p + 1)}
                disabled={products.length < pageSize}
              >
                Next
              </Button>
            </div>
          </div>
        </>
      )}
    </div>
  )
}
