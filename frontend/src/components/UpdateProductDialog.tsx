/**
 * Dialog for showing progress while updating a single product.
 *
 * Features:
 * - Spinner animation
 * - Timer counter showing elapsed time
 * - Progress steps with checkmarks
 * - Error state with retry option
 */

import { useState, useEffect, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { useUpdateProduct, UPDATE_PROGRESS_STEPS, getActiveStepIndex, type BatchUpdateResponse } from '@/hooks/useUpdateProduct'

interface UpdateProductDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  vendorIngredientIds: number[]
  vendorName: string
  sku: string | null
  ingredientId?: number  // For refetching the specific ingredient query
  onSuccess?: () => void
}

function formatElapsedTime(ms: number): string {
  const seconds = Math.floor(ms / 1000)
  const minutes = Math.floor(seconds / 60)
  const remainingSeconds = seconds % 60
  return `${minutes}:${remainingSeconds.toString().padStart(2, '0')}`
}

export function UpdateProductDialog({
  open,
  onOpenChange,
  vendorIngredientIds,
  vendorName,
  sku,
  ingredientId,
  onSuccess,
}: UpdateProductDialogProps) {
  const queryClient = useQueryClient()
  const [elapsedMs, setElapsedMs] = useState(0)
  const [isUpdating, setIsUpdating] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [successData, setSuccessData] = useState<BatchUpdateResponse | null>(null)
  const startTimeRef = useRef<number | null>(null)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const updateMutation = useUpdateProduct({
    onSuccess: (data) => {
      setIsUpdating(false)
      setSuccessData(data)
      // Stop timer
      if (timerRef.current) {
        clearInterval(timerRef.current)
        timerRef.current = null
      }
      // No auto-close - user must click Done
    },
    onError: (err) => {
      setIsUpdating(false)
      setError(err.message || 'Update failed')
      // Stop timer
      if (timerRef.current) {
        clearInterval(timerRef.current)
        timerRef.current = null
      }
    },
  })

  // Handle Done button click - refetch to ensure fresh data before closing
  const handleDone = async () => {
    // Use refetchQueries instead of invalidateQueries - this waits for the refetch to complete
    // so the modal will show fresh data when reopened
    await Promise.all([
      queryClient.refetchQueries({ queryKey: ['ingredient-detail'], type: 'all' }),
      queryClient.refetchQueries({ queryKey: ['price-comparison'], type: 'all' }),
      queryClient.refetchQueries({ queryKey: ['ingredients'], type: 'all' }),
    ])

    onOpenChange(false)
    onSuccess?.()
  }

  // Start update when dialog opens
  useEffect(() => {
    if (open && !isUpdating && !successData && !error) {
      startUpdate()
    }
  }, [open])

  // Cleanup on close
  useEffect(() => {
    if (!open) {
      // Reset state when dialog closes
      setElapsedMs(0)
      setIsUpdating(false)
      setError(null)
      setSuccessData(null)
      startTimeRef.current = null
      if (timerRef.current) {
        clearInterval(timerRef.current)
        timerRef.current = null
      }
    }
  }, [open])

  const startUpdate = () => {
    setError(null)
    setSuccessData(null)
    setIsUpdating(true)
    setElapsedMs(0)
    startTimeRef.current = Date.now()

    // Start timer
    timerRef.current = setInterval(() => {
      if (startTimeRef.current) {
        setElapsedMs(Date.now() - startTimeRef.current)
      }
    }, 100)

    // Trigger the mutation for all variants
    updateMutation.mutate(vendorIngredientIds)
  }

  const handleRetry = () => {
    startUpdate()
  }

  const handleClose = () => {
    if (!isUpdating) {
      onOpenChange(false)
    }
  }

  const activeStepIndex = getActiveStepIndex(elapsedMs, UPDATE_PROGRESS_STEPS)

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className="sm:max-w-md" onPointerDownOutside={(e) => isUpdating && e.preventDefault()}>
        <DialogHeader>
          <DialogTitle className="text-lg">
            Updating {vendorName}
          </DialogTitle>
        </DialogHeader>

        <div className="py-4">
          {/* SKU info */}
          {sku && (
            <p className="text-sm text-slate-500 mb-4">
              SKU: <span className="font-mono">{sku}</span>
            </p>
          )}

          {/* Progress section */}
          {isUpdating && (
            <div className="space-y-4">
              {/* Timer and spinner */}
              <div className="flex items-center justify-center gap-3">
                <div className="animate-spin rounded-full h-6 w-6 border-2 border-slate-300 border-t-blue-500" />
                <span className="text-2xl font-mono text-slate-700">
                  {formatElapsedTime(elapsedMs)}
                </span>
              </div>

              {/* Progress steps */}
              <div className="space-y-2 mt-4">
                {UPDATE_PROGRESS_STEPS.map((step, index) => {
                  const isComplete = index < activeStepIndex
                  const isActive = index === activeStepIndex
                  const isPending = index > activeStepIndex

                  return (
                    <div
                      key={step.id}
                      className={`flex items-center gap-3 py-1 transition-opacity ${
                        isPending ? 'opacity-40' : 'opacity-100'
                      }`}
                    >
                      {/* Status icon */}
                      <div className={`w-5 h-5 rounded-full flex items-center justify-center text-xs ${
                        isComplete
                          ? 'bg-green-500 text-white'
                          : isActive
                          ? 'bg-blue-500 text-white'
                          : 'bg-slate-200 text-slate-400'
                      }`}>
                        {isComplete ? (
                          <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" />
                          </svg>
                        ) : isActive ? (
                          <div className="w-2 h-2 bg-white rounded-full animate-pulse" />
                        ) : (
                          <span>{index + 1}</span>
                        )}
                      </div>

                      {/* Step label */}
                      <span className={`text-sm ${
                        isComplete ? 'text-green-600' : isActive ? 'text-blue-600' : 'text-slate-400'
                      }`}>
                        {isComplete ? step.doneLabel : step.label}
                      </span>
                    </div>
                  )
                })}
              </div>

              {/* Estimate message */}
              <p className="text-xs text-slate-400 text-center mt-4">
                This usually takes about 30 seconds
              </p>
            </div>
          )}

          {/* Success state */}
          {successData && (
            <div className="py-4">
              {/* Header */}
              <div className="text-center mb-4">
                <div className="w-12 h-12 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-3">
                  <svg className="w-6 h-6 text-green-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                  </svg>
                </div>
                <p className="text-green-600 font-medium text-lg">Update complete!</p>
                <p className="text-sm text-slate-600 mt-1">
                  {successData.message}
                </p>
                {successData.variants_failed > 0 && (
                  <p className="text-sm text-amber-600 mt-1">
                    {successData.variants_failed} variant{successData.variants_failed > 1 ? 's' : ''} failed to update
                  </p>
                )}
              </div>

              {/* Detailed changes list */}
              {successData.changes && successData.changes.length > 0 && (
                <div className="border-t border-slate-200 pt-4 mt-4">
                  <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3">
                    Change Details
                  </p>
                  <div className="space-y-2 max-h-48 overflow-y-auto">
                    {successData.changes.map((change, idx) => (
                      <div
                        key={change.vendor_ingredient_id || idx}
                        className="bg-slate-50 rounded-lg px-3 py-2 text-sm"
                      >
                        {/* SKU label */}
                        <div className="flex items-center gap-2 mb-1">
                          <code className="text-xs bg-slate-200 text-slate-700 px-1.5 py-0.5 rounded font-mono">
                            {change.sku || `ID:${change.vendor_ingredient_id}`}
                          </code>
                          {change.no_changes && (
                            <span className="text-xs text-slate-400">No changes</span>
                          )}
                        </div>

                        {/* Price change */}
                        {change.price && (
                          <div className="flex items-center gap-2 text-xs">
                            <span className="text-slate-500">Price:</span>
                            <span className="text-slate-600">
                              ${change.price.old?.toFixed(2) ?? '—'}
                            </span>
                            <svg className="w-3 h-3 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 8l4 4m0 0l-4 4m4-4H3" />
                            </svg>
                            <span className={change.price.new !== null && change.price.old !== null
                              ? change.price.new < change.price.old
                                ? 'text-green-600 font-medium'
                                : change.price.new > change.price.old
                                  ? 'text-red-600 font-medium'
                                  : 'text-slate-600'
                              : 'text-slate-600'
                            }>
                              ${change.price.new?.toFixed(2) ?? '—'}
                            </span>
                          </div>
                        )}

                        {/* Stock change */}
                        {change.stock_status && (
                          <div className="flex items-center gap-2 text-xs mt-1">
                            <span className="text-slate-500">Stock:</span>
                            <span className={`px-1.5 py-0.5 rounded ${
                              change.stock_status.old === 'in_stock'
                                ? 'bg-green-100 text-green-700'
                                : change.stock_status.old === 'out_of_stock'
                                  ? 'bg-red-100 text-red-700'
                                  : 'bg-slate-100 text-slate-600'
                            }`}>
                              {change.stock_status.old?.replace('_', ' ') || '—'}
                            </span>
                            <svg className="w-3 h-3 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 8l4 4m0 0l-4 4m4-4H3" />
                            </svg>
                            <span className={`px-1.5 py-0.5 rounded ${
                              change.stock_status.new === 'in_stock'
                                ? 'bg-green-100 text-green-700'
                                : change.stock_status.new === 'out_of_stock'
                                  ? 'bg-red-100 text-red-700'
                                  : 'bg-slate-100 text-slate-600'
                            }`}>
                              {change.stock_status.new?.replace('_', ' ') || '—'}
                            </span>
                          </div>
                        )}

                        {/* IO: Price tier changes */}
                        {change.price_tiers && Object.keys(change.price_tiers).length > 0 && (
                          <div className="mt-2">
                            <span className="text-xs text-slate-500 font-medium">Price Tiers:</span>
                            <div className="mt-1 space-y-0.5">
                              {Object.entries(change.price_tiers).map(([tier, prices]) => (
                                <div key={tier} className="flex items-center gap-2 text-xs ml-2">
                                  <span className="text-slate-600 w-16">{tier}:</span>
                                  <span className="text-slate-500">${prices.old?.toFixed(2) ?? '—'}</span>
                                  <svg className="w-3 h-3 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 8l4 4m0 0l-4 4m4-4H3" />
                                  </svg>
                                  <span className={
                                    prices.new !== null && prices.old !== null
                                      ? prices.new < prices.old
                                        ? 'text-green-600 font-medium'
                                        : prices.new > prices.old
                                          ? 'text-red-600 font-medium'
                                          : 'text-slate-600'
                                      : 'text-slate-600'
                                  }>
                                    ${prices.new?.toFixed(2) ?? '—'}
                                  </span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        {/* IO: Warehouse inventory changes */}
                        {change.inventory && Object.keys(change.inventory).length > 0 && (
                          <div className="mt-2">
                            <span className="text-xs text-slate-500 font-medium">Inventory:</span>
                            <div className="mt-1 space-y-0.5">
                              {Object.entries(change.inventory).map(([warehouse, qty]) => (
                                <div key={warehouse} className="flex items-center gap-2 text-xs ml-2">
                                  <span className="text-slate-600 w-16 capitalize">{warehouse}:</span>
                                  <span className="text-slate-500">{qty.old ?? 0} kg</span>
                                  <svg className="w-3 h-3 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 8l4 4m0 0l-4 4m4-4H3" />
                                  </svg>
                                  <span className={
                                    qty.new !== qty.old
                                      ? qty.new! > qty.old!
                                        ? 'text-green-600 font-medium'
                                        : 'text-amber-600 font-medium'
                                      : 'text-slate-600'
                                  }>
                                    {qty.new ?? 0} kg
                                  </span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Done button */}
              <div className="text-center mt-4">
                <Button onClick={handleDone}>
                  Done
                </Button>
              </div>
            </div>
          )}

          {/* Error state */}
          {error && (
            <div className="text-center py-4">
              <div className="w-12 h-12 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-3">
                <svg className="w-6 h-6 text-red-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </div>
              <p className="text-red-600 font-medium">Update failed</p>
              <p className="text-sm text-slate-500 mt-1 mb-4">{error}</p>
              <div className="flex gap-2 justify-center">
                <Button variant="outline" onClick={handleClose}>
                  Cancel
                </Button>
                <Button onClick={handleRetry}>
                  Retry
                </Button>
              </div>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}

/**
 * Refresh button component for triggering product updates.
 * Accepts an array of vendor ingredient IDs to batch update all variants.
 */
interface RefreshButtonProps {
  vendorIngredientIds: number[]
  vendorName: string
  sku: string | null
  ingredientId?: number  // For refetching the specific ingredient query after update
  className?: string
  variant?: 'default' | 'ghost'
  onSuccess?: () => void
}

export function RefreshButton({
  vendorIngredientIds,
  vendorName,
  sku,
  ingredientId,
  className = '',
  variant = 'default',
  onSuccess,
}: RefreshButtonProps) {
  const [dialogOpen, setDialogOpen] = useState(false)

  const baseStyles = 'inline-flex items-center justify-center rounded-md transition-colors'
  const variantStyles = variant === 'ghost'
    ? 'w-6 h-6 hover:bg-white/20'
    : 'w-7 h-7 bg-slate-100 hover:bg-slate-200 text-slate-500 hover:text-slate-700'

  return (
    <>
      <button
        onClick={(e) => {
          e.stopPropagation()
          setDialogOpen(true)
        }}
        className={`${baseStyles} ${variantStyles} ${className}`}
        title={`Refresh price & stock (${vendorIngredientIds.length} variant${vendorIngredientIds.length > 1 ? 's' : ''})`}
      >
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
          />
        </svg>
      </button>

      <UpdateProductDialog
        open={dialogOpen}
        onOpenChange={setDialogOpen}
        vendorIngredientIds={vendorIngredientIds}
        vendorName={vendorName}
        sku={sku}
        ingredientId={ingredientId}
        onSuccess={onSuccess}
      />
    </>
  )
}
