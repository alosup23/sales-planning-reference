# Non-Functional Requirements

## Performance

- warm startup shell under `2 seconds`
- first planning view under `3 seconds`
- scope switch under `1 second`
- branch expand under `700 ms`
- leaf edit and recalculated patch under `500 ms`
- year-level leaf edit under `900 ms`
- aggregate splash under `1.5 seconds`
- growth-factor apply under `1 second`
- no full-grid reload after normal edits
- no hierarchy collapse or active-view purge after normal edits
- master-data list first page under `2 seconds` with server-side paging and filters

## Scalability

- UAT must support current workbook-seeded data volumes comfortably
- design must scale to larger product and inventory volumes without replacing the core model
- server-side paging and lazy loading are mandatory for large maintenance sets and deep planning hierarchies

## Security

- Entra sign-in on web
- private data persistence
- no public destructive endpoints
- restricted CORS
- audit trail for all writes
- role and scope authorization seams for production hardening
- separate authorization policy for master-data administration versus planning use
- explicit origin protection between CloudFront and backend origin
- no client-side ability to bypass lock state or mutate hidden measures indirectly

## Reliability

- no automatic reseed on startup
- all admin imports are explicit operations
- recovery path for failed import jobs
- rollback path for deployment cutover

## Data Integrity

- canonical facts shared across all planning views
- deterministic rounding
- replayable and auditable write actions
- reconciliation checks after imports and major edits
- `baseValue`, `growthFactor`, and `effectiveValue` remain consistent after save, reread, undo, redo, and restore
- explicit and implicit lock state must be consistent between read and mutate paths

## Observability

- structured logs
- request tracing
- performance dashboards
- alerting on failed jobs and degraded latency

## Cost Discipline

- keep UAT to one active deployment target at a time
- prefer smallest production-shaped services that still meet performance targets
- retain free-tier posture where possible without sacrificing interactive performance
