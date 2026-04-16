/**
 * Canonical scan_complete eligibility SQL (SpaceAndTime).
 * @see src/queries/scan-complete-eligible-users.sql
 * @see backend/src/services/dealsync-v2.sync.service.ts
 */

import rawSql from '../queries/scan-complete-eligible-users.sql'
import { sanitizeSchema } from './sql/sanitize.js'

const EMAIL_PLACEHOLDER = '{{EMAIL_CORE_SCHEMA}}'
const DEALSYNC_PLACEHOLDER = '{{DEALSYNC_SCHEMA}}'

/**
 * @param {string} emailCoreSchema
 * @param {string} dealsyncSchema
 * @returns {string}
 */
export function buildScanCompleteEligibilitySql(emailCoreSchema, dealsyncSchema) {
  const ec = sanitizeSchema(emailCoreSchema)
  const ds = sanitizeSchema(dealsyncSchema)
  return rawSql.replaceAll(EMAIL_PLACEHOLDER, ec).replaceAll(DEALSYNC_PLACEHOLDER, ds)
}
