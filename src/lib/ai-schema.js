import { z } from 'zod'

// --- Valid categories and deal types for validation ---
export const VALID_CATEGORIES = new Set([
  'new',
  'in_progress',
  'completed',
  'not_interested',
  'likely_scam',
  'low_confidence',
])

export const VALID_DEAL_TYPES = new Set([
  'brand_collaboration',
  'sponsorship',
  'affiliate',
  'product_seeding',
  'ambassador',
  'content_partnership',
  'paid_placement',
  'other_business',
])

/**
 * AiThreadSchema — single source of truth for the AI thread output contract.
 *
 * Mirrors the exact coercion semantics of parseAndValidate() in src/lib/ai.js
 * (lines 243-266 pre-refactor). The schema is permissive: each field accepts
 * anything and coerces. A top-level transform applies is_deal-gated field
 * nullification.
 *
 * NOTE: thread_id remapping via threadOrder is applied OUTSIDE the schema.
 * Callers should resolve thread_id before passing objects to this schema.
 */
export const AiThreadSchema = z
  .object({
    thread_id: z.any().transform((v) => String(v || '')),
    is_deal: z.any().transform((v) => Boolean(v)),
    is_english: z.any().transform((v) => v !== false),
    language: z.any().transform((v) => v || null),
    ai_score: z.any().transform((v) => Math.min(10, Math.max(1, Math.round(Number(v) || 5)))),
    // Keep the RAW category around so the top-level transform can check
    // `r.category === 'likely_scam'` against the raw value (matching the
    // pre-refactor behavior where likely_scam is computed from raw category).
    category: z.any(),
    likely_scam: z.any(),
    ai_insight: z.any().transform((v) => String(v || '')),
    ai_summary: z.any().transform((v) => String(v || '').slice(0, 1000)),
    main_contact: z.any().transform((v) => v || null),
    deal_brand: z.any().transform((v) => v || null),
    deal_type: z.any(),
    deal_name: z.any().transform((v) => v || null),
    deal_value: z.any().transform((v) => {
      if (v == null) return null
      const n = Number(v)
      return Number.isFinite(n) ? n : null
    }),
    deal_currency: z.any().transform((v) => v || null),
  })
  .transform((r) => {
    const rawCategory = r.category
    const rawDealType = r.deal_type
    const isDeal = r.is_deal
    return {
      thread_id: r.thread_id,
      is_deal: isDeal,
      is_english: r.is_english,
      language: r.language,
      ai_score: r.ai_score,
      category: isDeal
        ? VALID_CATEGORIES.has(rawCategory)
          ? rawCategory
          : 'low_confidence'
        : null,
      likely_scam: Boolean(r.likely_scam) || rawCategory === 'likely_scam',
      ai_insight: r.ai_insight,
      ai_summary: r.ai_summary,
      main_contact: isDeal ? r.main_contact : null,
      deal_brand: isDeal ? r.deal_brand : null,
      deal_type: isDeal
        ? VALID_DEAL_TYPES.has(rawDealType)
          ? rawDealType
          : 'other_business'
        : null,
      deal_name: isDeal ? r.deal_name : null,
      deal_value: r.deal_value,
      deal_currency: r.deal_currency,
    }
  })

export const AiThreadArraySchema = z.array(AiThreadSchema)
