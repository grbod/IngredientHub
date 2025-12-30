import { useNavigate, useSearchParams } from 'react-router-dom'
import { useIngredients, useIngredientCount } from '@/hooks/useIngredients'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Card, CardContent } from '@/components/ui/card'

function CategoryBadge({ category }: { category: string | null }) {
  if (!category) {
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-600 border border-gray-200">
        Uncategorized
      </span>
    )
  }
  return (
    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-slate-100 text-slate-700 border border-slate-200">
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

function SkeletonRow() {
  return (
    <TableRow>
      <TableCell><div className="h-4 bg-muted rounded animate-pulse w-3/4"></div></TableCell>
      <TableCell><div className="h-5 bg-muted rounded-full animate-pulse w-28"></div></TableCell>
      <TableCell><div className="h-5 bg-muted rounded-full animate-pulse w-32"></div></TableCell>
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

  // Update URL when page changes
  const setPage = (newPage: number) => {
    const params = new URLSearchParams(searchParams)
    params.set('page', newPage.toString())
    setSearchParams(params, { replace: true })
  }

  // Update URL when search changes (resets page to 0)
  const handleSearchChange = (newSearch: string) => {
    const params = new URLSearchParams()
    if (newSearch) {
      params.set('search', newSearch)
    }
    params.set('page', '0')
    setSearchParams(params, { replace: true })
  }

  const { data: totalCount } = useIngredientCount()
  const { data, isLoading, error } = useIngredients({
    search: search || undefined,
    limit: pageSize,
    offset: page * pageSize,
  })

  const ingredients = data?.data || []

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="relative overflow-hidden rounded-xl bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 p-8">
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

      {/* Search */}
      <Card className="border-0 shadow-lg shadow-slate-200/50">
        <CardContent className="p-6">
          <div className="relative">
            <div className="absolute left-4 top-1/2 -translate-y-1/2 text-muted-foreground">
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
            </div>
            <Input
              placeholder="Search ingredients..."
              value={search}
              onChange={(e) => handleSearchChange(e.target.value)}
              className="pl-11 h-12 text-base border-slate-200 focus:border-slate-400 focus:ring-slate-400"
            />
          </div>
        </CardContent>
      </Card>

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
                    <TableHead className="font-semibold text-slate-700 py-4">Ingredient Name</TableHead>
                    <TableHead className="font-semibold text-slate-700 py-4">Category</TableHead>
                    <TableHead className="font-semibold text-slate-700 py-4">Vendors</TableHead>
                    <TableHead className="font-semibold text-slate-700 py-4 w-12"></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {isLoading ? (
                    Array.from({ length: 10 }).map((_, i) => <SkeletonRow key={i} />)
                  ) : ingredients.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={4} className="h-40 text-center">
                        <div className="flex flex-col items-center justify-center">
                          <div className="w-16 h-16 rounded-full bg-slate-100 flex items-center justify-center mb-4">
                            <svg className="h-8 w-8 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
                            </svg>
                          </div>
                          <p className="font-medium text-slate-900 mb-1">No ingredients found</p>
                          <p className="text-sm text-muted-foreground">Try adjusting your search</p>
                        </div>
                      </TableCell>
                    </TableRow>
                  ) : (
                    ingredients.map((ingredient, idx) => (
                      <TableRow
                        key={ingredient.ingredient_id}
                        className={`group transition-colors cursor-pointer ${idx % 2 === 0 ? 'bg-white' : 'bg-slate-50/50'} hover:bg-blue-50/50`}
                        onClick={() => navigate(`/products/${ingredient.ingredient_id}`)}
                      >
                        <TableCell className="py-4">
                          <p className="font-medium text-slate-900 group-hover:text-blue-600 transition-colors">
                            {ingredient.name}
                          </p>
                        </TableCell>
                        <TableCell className="py-4">
                          <CategoryBadge category={ingredient.category_name} />
                        </TableCell>
                        <TableCell className="py-4">
                          <div className="flex flex-wrap gap-1">
                            {ingredient.vendors && ingredient.vendors.length > 0 ? (
                              ingredient.vendors.map((vendor) => (
                                <VendorBadge key={vendor} vendor={vendor} />
                              ))
                            ) : (
                              <span className="text-xs text-slate-400">-</span>
                            )}
                          </div>
                        </TableCell>
                        <TableCell className="py-4">
                          <svg className="w-4 h-4 text-slate-400 group-hover:text-blue-600 transition-colors" fill="none" stroke="currentColor" viewBox="0 0 24 24">
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
            <div className="flex items-center justify-between px-6 py-4 border-t border-slate-200 bg-slate-50/50">
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
                  onClick={() => setPage((p) => Math.max(0, p - 1))}
                  disabled={page === 0}
                  className="gap-1"
                >
                  <svg className="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
                  </svg>
                  Previous
                </Button>
                <div className="flex items-center gap-1 px-2">
                  <span className="text-sm text-slate-600">Page</span>
                  <span className="text-sm font-semibold text-slate-900 bg-white px-2 py-1 rounded border">{page + 1}</span>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage((p) => p + 1)}
                  disabled={ingredients.length < pageSize}
                  className="gap-1"
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
