import { useMemo } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { useIngredients, useIngredientCount, type StockStatus } from '@/hooks/useIngredients'
import { useCategories } from '@/hooks/useCategories'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
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
import { Card } from '@/components/ui/card'

type SortOption = 'name-asc' | 'name-desc' | 'vendors-desc' | 'recent'

const sortOptions: { value: SortOption; label: string }[] = [
  { value: 'name-asc', label: 'Name (A-Z)' },
  { value: 'name-desc', label: 'Name (Z-A)' },
  { value: 'vendors-desc', label: 'Vendor count (most first)' },
  { value: 'recent', label: 'Recently updated' },
]

const vendorFilters = [
  { name: 'IngredientsOnline', color: 'bg-sky-500' },
  { name: 'BulkSupplements', color: 'bg-emerald-500' },
  { name: 'BoxNutra', color: 'bg-violet-500' },
  { name: 'TrafaPharma', color: 'bg-amber-500' },
]

function CategoryBadge({ category }: { category: string | null }) {
  if (!category) {
    return <span className="text-xs text-slate-400">—</span>
  }
  return (
    <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-slate-100 text-slate-600">
      {category}
    </span>
  )
}

const vendorStyles: Record<string, { bg: string; text: string; border: string }> = {
  'IngredientsOnline': { bg: 'bg-sky-50', text: 'text-sky-700', border: 'border-sky-200' },
  'BulkSupplements': { bg: 'bg-emerald-50', text: 'text-emerald-700', border: 'border-emerald-200' },
  'BoxNutra': { bg: 'bg-violet-50', text: 'text-violet-700', border: 'border-violet-200' },
  'TrafaPharma': { bg: 'bg-amber-50', text: 'text-amber-700', border: 'border-amber-200' },
}

function VendorBadge({ vendor }: { vendor: string }) {
  const style = vendorStyles[vendor] || { bg: 'bg-gray-50', text: 'text-gray-700', border: 'border-gray-200' }
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${style.bg} ${style.text} ${style.border}`}>
      {vendor}
    </span>
  )
}

function StockIndicator({ status }: { status: StockStatus }) {
  if (status === 'in_stock') {
    return (
      <span className="inline-flex items-center" title="In stock at one or more vendors">
        <span className="w-2.5 h-2.5 rounded-full bg-emerald-500" />
      </span>
    )
  }
  if (status === 'out_of_stock') {
    return (
      <span className="inline-flex items-center" title="Out of stock at all vendors">
        <span className="w-2.5 h-2.5 rounded-full bg-red-400" />
      </span>
    )
  }
  // Unknown status - no indicator
  return null
}

function SkeletonRow() {
  return (
    <TableRow>
      <TableCell><div className="h-2.5 w-2.5 bg-muted rounded-full animate-pulse"></div></TableCell>
      <TableCell><div className="h-4 bg-muted rounded animate-pulse w-3/4"></div></TableCell>
      <TableCell><div className="h-5 bg-muted rounded-full animate-pulse w-24"></div></TableCell>
      <TableCell><div className="h-5 bg-muted rounded-full animate-pulse w-28"></div></TableCell>
      <TableCell><div className="h-4 bg-muted rounded animate-pulse w-4"></div></TableCell>
    </TableRow>
  )
}

export function Products() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const pageSize = 50

  // Read state from URL query params
  const page = Math.max(0, parseInt(searchParams.get('page') || '0', 10))
  const search = searchParams.get('search') || ''
  const vendorFilter = searchParams.get('vendor') || ''
  const categoryFilter = searchParams.get('category') || ''
  const sortOption = (searchParams.get('sort') as SortOption) || 'name-asc'

  // Update URL when page changes
  const setPage = (newPage: number) => {
    const params = new URLSearchParams(searchParams)
    params.set('page', newPage.toString())
    setSearchParams(params, { replace: true })
  }

  // Update URL when search changes (resets page to 0)
  const handleSearchChange = (newSearch: string) => {
    const params = new URLSearchParams(searchParams)
    if (newSearch) {
      params.set('search', newSearch)
    } else {
      params.delete('search')
    }
    params.set('page', '0')
    setSearchParams(params, { replace: true })
  }

  // Update URL when vendor filter changes
  const handleVendorFilter = (vendor: string) => {
    const params = new URLSearchParams(searchParams)
    if (vendorFilter === vendor) {
      params.delete('vendor')
    } else {
      params.set('vendor', vendor)
    }
    params.set('page', '0')
    setSearchParams(params, { replace: true })
  }

  // Update URL when category filter changes
  const handleCategoryFilter = (categoryId: string) => {
    const params = new URLSearchParams(searchParams)
    if (categoryId && categoryId !== 'all') {
      params.set('category', categoryId)
    } else {
      params.delete('category')
    }
    params.set('page', '0')
    setSearchParams(params, { replace: true })
  }

  // Update URL when sort changes
  const handleSortChange = (sort: SortOption) => {
    const params = new URLSearchParams(searchParams)
    if (sort !== 'name-asc') {
      params.set('sort', sort)
    } else {
      params.delete('sort')
    }
    setSearchParams(params, { replace: true })
  }

  const { data: categories } = useCategories()
  const { data: totalCount } = useIngredientCount()
  const { data, isLoading, error } = useIngredients({
    search: search || undefined,
    limit: pageSize,
    offset: page * pageSize,
  })

  // Filter by vendor and category on client side (could be moved to API)
  const filteredIngredients = useMemo(() => {
    let result = data?.data || []

    if (vendorFilter) {
      result = result.filter(i => i.vendors?.includes(vendorFilter))
    }

    if (categoryFilter) {
      const catId = parseInt(categoryFilter, 10)
      result = result.filter(i => i.category_id === catId)
    }

    return result
  }, [data?.data, vendorFilter, categoryFilter])

  // Sort ingredients on client side
  const sortedIngredients = useMemo(() => {
    const sorted = [...filteredIngredients]

    switch (sortOption) {
      case 'name-asc':
        sorted.sort((a, b) => a.name.localeCompare(b.name))
        break
      case 'name-desc':
        sorted.sort((a, b) => b.name.localeCompare(a.name))
        break
      case 'vendors-desc':
        sorted.sort((a, b) => b.vendor_count - a.vendor_count)
        break
      case 'recent':
        // Since we don't have updated_at in the current schema,
        // fall back to ingredient_id (higher = newer)
        sorted.sort((a, b) => b.ingredient_id - a.ingredient_id)
        break
    }

    return sorted
  }, [filteredIngredients, sortOption])

  const ingredients = sortedIngredients

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="relative overflow-hidden rounded-xl hero-gradient hero-shimmer p-8">
        <div className="absolute inset-0 bg-grid-white/5 [mask-image:linear-gradient(0deg,transparent,black)]"></div>
        <div className="relative">
          <div className="flex items-center gap-3 mb-2">
            <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-white/10 backdrop-blur">
              <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4" />
              </svg>
            </div>
            <h1 className="text-3xl font-bold text-white tracking-tight">Products</h1>
          </div>
          <p className="text-slate-300 max-w-2xl">
            Browse {totalCount?.toLocaleString() || 0} ingredients across all vendors
          </p>
        </div>
        <div className="absolute right-8 top-1/2 -translate-y-1/2 opacity-10">
          <svg className="w-32 h-32 text-white" fill="currentColor" viewBox="0 0 24 24">
            <path d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4" />
          </svg>
        </div>
      </div>

      {/* Search and Filters */}
      <div className="space-y-3">
        <div className="relative">
          <div className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
          </div>
          <Input
            placeholder="Search ingredients..."
            value={search}
            onChange={(e) => handleSearchChange(e.target.value)}
            className="pl-12 h-12 text-base bg-white border-slate-200 focus:border-blue-400 focus:ring-blue-400 shadow-sm"
          />
          {search && (
            <button
              onClick={() => handleSearchChange('')}
              className="absolute right-4 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600"
            >
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          )}
        </div>

        {/* Filters Row */}
        <div className="flex flex-wrap items-center gap-4">
          {/* Vendor Filter Chips */}
          <div className="flex items-center gap-2">
            <span className="text-sm text-slate-500">Vendor:</span>
            {vendorFilters.map(v => (
              <button
                key={v.name}
                onClick={() => handleVendorFilter(v.name)}
                className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all ${
                  vendorFilter === v.name
                    ? 'bg-slate-900 text-white shadow-md'
                    : 'bg-white text-slate-600 border border-slate-200 hover:border-slate-300 hover:bg-slate-50'
                }`}
              >
                <span className={`w-2 h-2 rounded-full ${v.color}`}></span>
                {v.name}
              </button>
            ))}
            {vendorFilter && (
              <button
                onClick={() => handleVendorFilter(vendorFilter)}
                className="text-xs text-slate-500 hover:text-slate-700 underline ml-1"
              >
                Clear
              </button>
            )}
          </div>

          {/* Divider */}
          <div className="hidden sm:block h-6 w-px bg-slate-200" />

          {/* Category Filter */}
          <div className="flex items-center gap-2">
            <span className="text-sm text-slate-500">Category:</span>
            <Select
              value={categoryFilter || 'all'}
              onValueChange={handleCategoryFilter}
            >
              <SelectTrigger className="w-[180px] h-8 text-xs bg-white">
                <SelectValue placeholder="All categories" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All categories</SelectItem>
                {categories?.map(cat => (
                  <SelectItem key={cat.category_id} value={cat.category_id.toString()}>
                    {cat.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {/* Divider */}
          <div className="hidden sm:block h-6 w-px bg-slate-200" />

          {/* Sort */}
          <div className="flex items-center gap-2">
            <span className="text-sm text-slate-500">Sort:</span>
            <Select
              value={sortOption}
              onValueChange={(value) => handleSortChange(value as SortOption)}
            >
              <SelectTrigger className="w-[180px] h-8 text-xs bg-white">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {sortOptions.map(opt => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>

      {/* Table */}
      <Card className="border-0 shadow-lg shadow-slate-200/50 overflow-hidden">
        {error ? (
          <div className="p-12 text-center">
            <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-red-50 mb-4">
              <svg className="h-8 w-8 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              </svg>
            </div>
            <h3 className="text-lg font-semibold text-slate-900 mb-1">Failed to load products</h3>
            <p className="text-muted-foreground">{error.message}</p>
            <Button variant="outline" className="mt-4" onClick={() => window.location.reload()}>
              Try Again
            </Button>
          </div>
        ) : (
          <>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow className="bg-slate-50/80 hover:bg-slate-50/80 border-b border-slate-200">
                    <TableHead className="font-semibold text-slate-700 py-3 w-8"></TableHead>
                    <TableHead className="font-semibold text-slate-700 py-3">Ingredient Name</TableHead>
                    <TableHead className="font-semibold text-slate-700 py-3">Category</TableHead>
                    <TableHead className="font-semibold text-slate-700 py-3">Vendors</TableHead>
                    <TableHead className="font-semibold text-slate-700 py-3 w-10"></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {isLoading ? (
                    Array.from({ length: 10 }).map((_, i) => <SkeletonRow key={i} />)
                  ) : ingredients.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={5} className="h-40 text-center">
                        <div className="flex flex-col items-center justify-center">
                          <div className="w-16 h-16 rounded-full bg-slate-100 flex items-center justify-center mb-4">
                            <svg className="h-8 w-8 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
                            </svg>
                          </div>
                          <p className="font-medium text-slate-900 mb-1">No ingredients found</p>
                          <p className="text-sm text-muted-foreground">Try adjusting your search or filters</p>
                        </div>
                      </TableCell>
                    </TableRow>
                  ) : (
                    ingredients.map((ingredient, idx) => (
                      <TableRow
                        key={ingredient.ingredient_id}
                        className={`group transition-colors cursor-pointer ${idx % 2 === 0 ? 'bg-white' : 'bg-slate-50/30'} hover:bg-blue-50/50`}
                        onClick={() => navigate(`/products/${ingredient.ingredient_id}`)}
                      >
                        <TableCell className="py-3 w-8">
                          <StockIndicator status={ingredient.stock_status} />
                        </TableCell>
                        <TableCell className="py-3">
                          <p className="font-medium text-slate-900 group-hover:text-blue-600 transition-colors">
                            {ingredient.name}
                          </p>
                        </TableCell>
                        <TableCell className="py-3">
                          <CategoryBadge category={ingredient.category_name} />
                        </TableCell>
                        <TableCell className="py-3">
                          <div className="flex flex-wrap gap-1">
                            {ingredient.vendors && ingredient.vendors.length > 0 ? (
                              ingredient.vendors.map((vendor) => (
                                <VendorBadge key={vendor} vendor={vendor} />
                              ))
                            ) : (
                              <span className="text-xs text-slate-400">—</span>
                            )}
                          </div>
                        </TableCell>
                        <TableCell className="py-3">
                          <svg className="w-4 h-4 text-slate-300 group-hover:text-blue-500 transition-colors" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                          </svg>
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>

            {/* Pagination */}
            <div className="flex items-center justify-between px-6 py-3 border-t border-slate-200 bg-slate-50/50">
              <p className="text-sm text-slate-600">
                Showing <span className="font-semibold text-slate-900">{page * pageSize + 1}</span> to{' '}
                <span className="font-semibold text-slate-900">{page * pageSize + ingredients.length}</span>
                {totalCount !== undefined && (
                  <> of <span className="font-semibold text-slate-900">{totalCount.toLocaleString()}</span></>
                )}
              </p>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage(Math.max(0, page - 1))}
                  disabled={page === 0}
                  className="gap-1 h-8"
                >
                  <svg className="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
                  </svg>
                  Previous
                </Button>
                <div className="flex items-center gap-1 px-2">
                  <span className="text-sm font-medium text-slate-900 bg-white px-2.5 py-1 rounded border border-slate-200">{page + 1}</span>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage(page + 1)}
                  disabled={ingredients.length < pageSize}
                  className="gap-1 h-8"
                >
                  Next
                  <svg className="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                  </svg>
                </Button>
              </div>
            </div>
          </>
        )}
      </Card>
    </div>
  )
}
