import { useVendorStats } from '@/hooks/useVendors'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'

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

function getStatusColor(lastScraped: string | null): 'default' | 'secondary' | 'destructive' {
  if (!lastScraped) return 'destructive'
  const date = new Date(lastScraped)
  const now = new Date()
  const diffHours = (now.getTime() - date.getTime()) / (1000 * 60 * 60)

  if (diffHours < 24) return 'default'
  if (diffHours < 72) return 'secondary'
  return 'destructive'
}

export function Dashboard() {
  const { data: stats, isLoading, error } = useVendorStats()

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-muted-foreground">Loading vendor stats...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-destructive">Error loading data: {error.message}</div>
      </div>
    )
  }

  const totalProducts = stats?.reduce((sum, v) => sum + v.productCount, 0) || 0
  const totalVariants = stats?.reduce((sum, v) => sum + v.variantCount, 0) || 0

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Dashboard</h1>
        <p className="text-muted-foreground">Overview of all vendor scrapers</p>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="pb-2">
            <CardDescription>Total Products</CardDescription>
            <CardTitle className="text-3xl">{totalProducts.toLocaleString()}</CardTitle>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardDescription>Size Variants</CardDescription>
            <CardTitle className="text-3xl">{totalVariants.toLocaleString()}</CardTitle>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardDescription>Active Vendors</CardDescription>
            <CardTitle className="text-3xl">{stats?.length || 0}</CardTitle>
          </CardHeader>
        </Card>
      </div>

      <div>
        <h2 className="text-lg font-semibold mb-4">Vendors</h2>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {stats?.map((vendor) => (
            <Card key={vendor.vendor_id}>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-lg">{vendor.name}</CardTitle>
                  <Badge variant={getStatusColor(vendor.lastScraped)}>
                    {vendor.status || 'active'}
                  </Badge>
                </div>
                <CardDescription>{vendor.pricing_model || 'Unknown pricing model'}</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Products</span>
                    <span className="font-medium">{vendor.productCount.toLocaleString()}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Variants</span>
                    <span className="font-medium">{vendor.variantCount.toLocaleString()}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Last Scraped</span>
                    <span className="font-medium">{formatDate(vendor.lastScraped)}</span>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    </div>
  )
}
