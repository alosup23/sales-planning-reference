# Phase Roadmap And Backlog

## Phase 1

Goal:

- deliver a fast, secure, UAT-ready planning platform on AWS
- include all data foundations required for future AI merchandising

### Wave 1: Contracts And Data Model

Repo modules:

- `apps/api/src/SalesPlanning.Api/Contracts`
- `apps/api/src/SalesPlanning.Api/Application`
- `apps/api/src/SalesPlanning.Api/Infrastructure/Postgres`
- `docs/*`

Key backlog:

- finalize PostgreSQL schema
- add new master-data domains
- add undo/redo journal model
- finalize import/export contracts

### Wave 2: Native PostgreSQL Path

Repo modules:

- `apps/api/src/SalesPlanning.Api/Infrastructure/Postgres`
- `apps/api/src/SalesPlanning.Api/Application`

Key backlog:

- replace SQLite compatibility path
- add native read/query repository
- add incremental aggregate maintenance

### Wave 3: Query / Command API

Repo modules:

- `apps/api/src/SalesPlanning.Api/Controllers`
- `apps/api/src/SalesPlanning.Api/Contracts`
- `apps/api/src/SalesPlanning.Api/Application`

Key backlog:

- introduce `v2` query endpoints
- introduce patch-returning command endpoints
- add undo/redo endpoints

### Wave 4: Frontend Grid Refactor

Repo modules:

- `apps/web/src/App.tsx`
- `apps/web/src/components/PlanningGrid.tsx`
- `apps/web/src/lib/api.ts`
- `apps/web/src/lib/types.ts`
- `apps/web/src/styles/app.css`

Key backlog:

- move to AG Grid SSRM
- lazy branch loading
- patch-based updates
- undo/redo UI
- finalize compact menu UX

### Wave 5: Master-Data Maintenance

Repo modules:

- `apps/web/src/*`
- `apps/api/src/SalesPlanning.Api/*`

Key backlog:

- Inventory Profile CRUD/import/export
- Pricing Policy CRUD/import/export
- Seasonality & Events CRUD/import/export
- Vendor Supply Profile CRUD/import/export

### Wave 6: Async Jobs And Deployment Cleanup

Repo modules:

- `apps/api/tools/*`
- `infra/aws/*`

Key backlog:

- async import/export jobs
- deployment cleanup runbook
- remove stale runtime paths after cutover

### Wave 7: Security Hardening

Repo modules:

- `apps/api/src/SalesPlanning.Api/Program.cs`
- `infra/aws/*`

Key backlog:

- strict backend auth
- role separation
- WAF and observability finalization

## Phase 2

Goal:

- deliver expert merchandising recommendations and decision support

### Wave 8: AI Data Services

Repo modules:

- new AI integration contracts
- planning and policy query services

Key backlog:

- build recommendation context APIs
- add evaluation datasets

### Wave 9: Recommendation Services

Key backlog:

- price recommendation engine
- markdown recommendation engine
- forecast guidance engine

### Wave 10: AI Review UX

Repo modules:

- `apps/web/src/*`

Key backlog:

- recommendation panels
- explainability UI
- approval / accept / reject flows
