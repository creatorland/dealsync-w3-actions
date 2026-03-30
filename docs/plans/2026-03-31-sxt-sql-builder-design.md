# SxT SQL Builder — Modular Design

**Date:** 2026-03-31
**Goal:** Centralize all 60+ inline SQL statements into a `src/lib/sql/` module directory — pure functions that return SQL strings — so business logic never constructs SQL directly.

## Architecture

Split by table into per-file modules under `src/lib/sql/`:

| File              | Table(s)                       | Queries | Rationale                                       |
| ----------------- | ------------------------------ | ------- | ----------------------------------------------- |
| `deal-states.js`  | DEAL_STATES                    | 13      | Largest group: claims, selects, updates, sweeps |
| `batch-events.js` | BATCH_EVENTS                   | 2       | Separate lifecycle concern                      |
| `audits.js`       | AI_EVALUATION_AUDITS           | 2       | Checkpoint system                               |
| `evaluations.js`  | EMAIL_THREAD_EVALUATIONS       | 2       | AI eval storage                                 |
| `deals.js`        | DEALS, DEAL_CONTACTS, CONTACTS | 6       | Tightly coupled deal entity writes              |
| `index.js`        | barrel re-export               | 0       | Single import point                             |

Each function accepts typed params and returns a complete SQL string. Sanitization utilities (`sanitizeId`, `sanitizeString`, `sanitizeSchema`, `toSqlIdList`) stay in `queries.js` and are imported by sql modules.

## Naming Conventions

Every query function name must explicitly describe what it does. No implementation details (joins, batch), no ambiguity.

### Renames from original plan

| Old name                         | New name                            | Why                                                |
| -------------------------------- | ----------------------------------- | -------------------------------------------------- |
| `selectByBatch`                  | `selectEmailsByBatch`               | Says what it returns                               |
| `selectByBatchWithJoins`         | `selectEmailsWithEvalAndCreator`    | Describes the data, not the SQL technique          |
| `selectMetadataByBatch`          | `selectEmailAndThreadIdsByBatch`    | Exact columns returned                             |
| `updateStatusByIdsWithTimestamp` | _(merged into `updateStatusByIds`)_ | Always set UPDATED_AT — no reason for two variants |
| `touchBatch`                     | `refreshBatchTimestamp`             | Descriptive, not Unix jargon                       |
| `failOrphaned`                   | `markOrphanedAsFailed`              | Explicit verb                                      |
| `upsertValues` (batchEvents)     | `upsertBulk`                        | Distinguishes from single-row `upsert`             |

### Names kept as-is

`claimFilterBatch`, `claimClassifyBatch`, `selectDistinctThreadUsers`, `findStuckBatches`, `findDeadBatches`, `countByBatchAndStatus`, `countOrphaned`, `syncFromEmailMetadata`, `updateStatusByBatch` — all self-descriptive.

## Security Improvement

All `batchId` interpolations now go through `sanitizeId()` inside the builder. Previously ~15 call sites had unsanitized UUIDs.

## Constraints

- No behavior changes — all SQL output is identical to current inline SQL
- SxT limitations: no CTEs, no division operator, INTERVAL syntax, single-column LEFT JOIN only
- Node 24 ESM, Jest with `--experimental-vm-modules`, no new dependencies
- TDD: tests first for builders, then mechanical swap in each consumer
