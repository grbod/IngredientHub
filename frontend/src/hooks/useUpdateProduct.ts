/**
 * Hook for updating product prices and inventory.
 * Supports batch updates of multiple variants with detailed change tracking.
 */

import { useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { UpdateProductResponse } from '@/lib/api'

/**
 * Detailed change for a single variant
 */
export interface DetailedChange {
  sku: string | null
  vendor_ingredient_id: number
  price?: { old: number | null; new: number | null }
  stock_status?: { old: string | null; new: string | null }
  no_changes?: boolean
}

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
  changes: DetailedChange[]
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

      // Collect detailed changes from successful updates
      let priceChanges = 0
      let stockChanges = 0
      const changes: DetailedChange[] = []

      for (const result of fulfilled) {
        const { changed_fields, sku, vendor_ingredient_id } = result.value
        const changedFields = changed_fields || {}

        const hasPrice = 'price' in changedFields
        const hasStock = 'stock_status' in changedFields

        if (hasPrice) priceChanges++
        if (hasStock) stockChanges++

        // Build detailed change entry
        const change: DetailedChange = {
          sku: sku || null,
          vendor_ingredient_id: vendor_ingredient_id,
        }

        if (hasPrice) {
          change.price = {
            old: changedFields.price?.old ?? null,
            new: changedFields.price?.new ?? null,
          }
        }

        if (hasStock) {
          change.stock_status = {
            old: changedFields.stock_status?.old ?? null,
            new: changedFields.stock_status?.new ?? null,
          }
        }

        // Mark if no changes detected
        if (!hasPrice && !hasStock) {
          change.no_changes = true
        }

        changes.push(change)
      }

      // Build summary message
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
        changes,
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
