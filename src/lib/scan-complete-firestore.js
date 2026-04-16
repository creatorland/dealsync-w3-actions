/**
 * Firestore read-only check for users/{userId}.scanCompleteSentAt (dedupe before webhook POST).
 * Uses Firestore REST API + google-auth-library (no firebase-admin).
 */

import { GoogleAuth } from 'google-auth-library'

const FIRESTORE_SCOPE = 'https://www.googleapis.com/auth/datastore'

/**
 * @param {unknown} doc — parsed GET document JSON from Firestore REST
 * @returns {boolean} true if scanCompleteSentAt is present (any numeric value)
 */
export function firestoreDocumentHasScanCompleteSentAt(doc) {
  const field = doc?.fields?.scanCompleteSentAt
  if (!field || typeof field !== 'object') return false
  if (field.integerValue != null && field.integerValue !== '') return true
  if (field.doubleValue != null && Number.isFinite(Number(field.doubleValue))) return true
  return false
}

/**
 * @param {{ projectId: string, userId: string, credentials: Record<string, unknown> }} args
 * @returns {Promise<boolean>} true → skip webhook (already sent); false → eligible to POST
 */
export async function userHasScanCompleteSentAt({ projectId, userId, credentials }) {
  const auth = new GoogleAuth({
    credentials,
    scopes: [FIRESTORE_SCOPE],
  })
  const client = await auth.getClient()
  const { token } = await client.getAccessToken()
  if (!token) throw new Error('Firestore auth: no access token')

  const path = `projects/${encodeURIComponent(projectId)}/databases/(default)/documents/users/${encodeURIComponent(userId)}`
  const url = `https://firestore.googleapis.com/v1/${path}`

  const resp = await fetch(url, {
    method: 'GET',
    headers: { Authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(30000),
  })

  if (resp.status === 404) return false

  if (!resp.ok) {
    const text = await resp.text()
    throw new Error(`Firestore GET ${resp.status}: ${text}`)
  }

  const doc = await resp.json()
  return firestoreDocumentHasScanCompleteSentAt(doc)
}
