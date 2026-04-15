import { sanitizeSchema, sanitizeId, sanitizeString } from './sanitize.js'

export const deals = {
  deleteByThreadIds: (schema, quotedThreadIds) => {
    const s = sanitizeSchema(schema)
    return `DELETE FROM ${s}.DEALS WHERE THREAD_ID IN (${quotedThreadIds.join(',')})`
  },

  upsert: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.DEALS (ID, USER_ID, THREAD_ID, EMAIL_THREAD_EVALUATION_ID, DEAL_NAME, DEAL_TYPE, CATEGORY, VALUE, CURRENCY, BRAND, IS_AI_SORTED, CREATED_AT, UPDATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (THREAD_ID) DO UPDATE SET EMAIL_THREAD_EVALUATION_ID = EXCLUDED.EMAIL_THREAD_EVALUATION_ID, DEAL_NAME = EXCLUDED.DEAL_NAME, DEAL_TYPE = EXCLUDED.DEAL_TYPE, CATEGORY = EXCLUDED.CATEGORY, VALUE = EXCLUDED.VALUE, CURRENCY = EXCLUDED.CURRENCY, BRAND = EXCLUDED.BRAND, UPDATED_AT = CURRENT_TIMESTAMP`
  },

  selectByThreadIds: (schema, quotedThreadIds) => {
    const s = sanitizeSchema(schema)
    return `SELECT ID, THREAD_ID, USER_ID, UPDATED_AT FROM ${s}.DEALS WHERE THREAD_ID IN (${quotedThreadIds.join(',')})`
  },

  findAffectedForBackfill: (schema, { startDate, cursorId, limit }) => {
    const s = sanitizeSchema(schema)
    const safeDate = sanitizeString(startDate)
    // Empty cursor means "start from the beginning" — use an empty string literal in SQL.
    const safeCursor = cursorId ? sanitizeId(cursorId) : ''
    return `SELECT ID, THREAD_ID, USER_ID FROM ${s}.DEALS WHERE (VALUE = 0 OR VALUE IS NULL) AND CREATED_AT >= '${safeDate}' AND ID > '${safeCursor}' ORDER BY ID LIMIT ${Number(limit)}`
  },

  backfillValue: (schema, { dealId, value, currency }) => {
    const s = sanitizeSchema(schema)
    const safeId = sanitizeId(dealId)
    const safeCurrency = sanitizeString(currency || 'USD')
    const numValue = Number.isFinite(value) && value >= 0 ? value : 0
    return `UPDATE ${s}.DEALS SET VALUE = ${numValue}, CURRENCY = '${safeCurrency}', UPDATED_AT = CURRENT_TIMESTAMP WHERE ID = '${safeId}' AND (VALUE = 0 OR VALUE IS NULL)`
  },
}

export const dealContacts = {
  deleteByDealIds: (schema, quotedDealIds) => {
    const s = sanitizeSchema(schema)
    return `DELETE FROM ${s}.DEAL_CONTACTS WHERE DEAL_ID IN (${quotedDealIds.join(',')})`
  },

  upsert: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.DEAL_CONTACTS (DEAL_ID, USER_ID, EMAIL, CONTACT_TYPE, CREATED_AT, UPDATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (DEAL_ID, USER_ID, EMAIL) DO UPDATE SET CONTACT_TYPE = EXCLUDED.CONTACT_TYPE, UPDATED_AT = CURRENT_TIMESTAMP`
  },
}

export const contacts = {
  upsert: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.CONTACTS (USER_ID, EMAIL, NAME, COMPANY_NAME, TITLE, PHONE_NUMBER, CREATED_AT, UPDATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (USER_ID, EMAIL) DO UPDATE SET NAME = EXCLUDED.NAME, COMPANY_NAME = EXCLUDED.COMPANY_NAME, TITLE = EXCLUDED.TITLE, PHONE_NUMBER = EXCLUDED.PHONE_NUMBER, UPDATED_AT = CURRENT_TIMESTAMP`
  },
}
