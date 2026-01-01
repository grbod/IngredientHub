/**
 * Hook for updating product prices and inventory.
 * Supports batch updates of multiple variants with detailed change tracking.
 */

import { useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { UpdateProductResponse } from '@/lib/api'

/**
 * Aggregated response for batch updates
 */
export interface BatchUpdateResponse {
  success: boolean
  message: string
  variants_updated: number
  variants_failed: number
  price_changes: number
  stock_changes: number
}

interface UseUpdateProductOptions {
  onSuccess?: (data: BatchUpdateResponse) => void
  onError?: (error: Error) => void
}

/**
 * Mutation hook for updating products by vendor ingredient IDs.
 *
 * Features:
 * - Batch updates multiple variants in parallel
 * - Auto-retry once on failure per ID
 * - Invalidates relevant queries on success
 * - Returns mutation state for UI feedback
 */
export function useUpdateProduct(options?: UseUpdateProductOptions) {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async (vendorIngredientIds: number[]): Promise<BatchUpdateResponse> => {
      // Update all variants in parallel
      const results = await Promise.allSettled(
        vendorIngredientIds.map(async (id) => {
          try {
            return await api.updateSingleProduct(id)
          } catch (error) {
            // Auto-retry once on failure (with brief delay)
            await new Promise(resolve => setTimeout(resolve, 1000))
            return await api.updateSingleProduct(id)
          }
        })
      )

      // Count successes and failures
      const fulfilled = results.filter(r => r.status === 'fulfilled') as PromiseFulfilledResult<UpdateProductResponse>[]
      const failures = results.filter(r => r.status === 'rejected').length

      if (failures === vendorIngredientIds.length) {
        // All failed
        const firstError = results.find(r => r.status === 'rejected') as PromiseRejectedResult
        throw new Error(firstError.reason?.message || 'All updates failed')
      }

      // Count price and stock changes from successful updates
      let priceChanges = 0
      let stockChanges = 0
      for (const result of fulfilled) {
        const changedFields = result.value.changed_fields || {}
        if ('price' in changedFields || 'price_per_kg' in changedFields) {
          priceChanges++
        }
        if ('stock_status' in changedFields) {
          stockChanges++
        }
      }

      // Build detailed message
      const parts: string[] = []
      if (priceChanges > 0) parts.push(`${priceChanges} price change${priceChanges > 1 ? 's' : ''}`)
      if (stockChanges > 0) parts.push(`${stockChanges} stock change${stockChanges > 1 ? 's' : ''}`)

      const message = parts.length > 0
        ? `Updated ${fulfilled.length} variant${fulfilled.length > 1 ? 's' : ''}: ${parts.join(', ')}`
        : `Updated ${fulfilled.length} variant${fulfilled.length > 1 ? 's' : ''} (no changes)`

      return {
        success: failures === 0,
        message,
        variants_updated: fulfilled.length,
        variants_failed: failures,
        price_changes: priceChanges,
        stock_changes: stockChanges,
      }
    },

    onSuccess: (data) => {
      // Note: We don't invalidate here anymore - the dialog handles refetch on close
      // This ensures data is fresh only after user acknowledges the update
      options?.onSuccess?.(data)
    },

    onError: (error: Error) => {
      options?.onError?.(error)
    },
  })
}

/**
 * Progress step definition for UI
 */
export interface ProgressStep {
  id: string
  label: string
  doneLabel: string
  estimatedDuration: number // milliseconds
}

/**
 * Default progress steps for the update dialog
 */
export const UPDATE_PROGRESS_STEPS: ProgressStep[] = [
  {
    id: 'fetch',
    label: 'Fetching latest price...',
    doneLabel: 'Price fetched',
    estimatedDuration: 8000,
  },
  {
    id: 'inventory',
    label: 'Checking inventory...',
    doneLabel: 'Inventory checked',
    estimatedDuration: 6000,
  },
  {
    id: 'save',
    label: 'Saving to database...',
    doneLabel: 'Saved',
    estimatedDuration: 4000,
  },
]

/**
 * Calculate which step should be active based on elapsed time
 */
export function getActiveStepIndex(elapsedMs: number, steps: ProgressStep[]): number {
  let cumulative = 0
  for (let i = 0; i < steps.length; i++) {
    cumulative += steps[i].estimatedDuration
    if (elapsedMs < cumulative) {
      return i
    }
  }
  return steps.length - 1
}
