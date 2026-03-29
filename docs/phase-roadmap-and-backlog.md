# Phase Roadmap And Backlog

## Phase 1

Goal:

- deliver a fast, secure, UAT-ready planning platform on AWS
- include all data foundations required for future AI merchandising

Current status:

- functional Phase 1 UAT scope is live
- Phase 1 core runtime is now on `CloudFront + ECS Fargate + RDS PostgreSQL + WAF`
- remaining Phase 1 hardening items are mainly performance and operations refinements rather than scope gaps

### Wave 1: Contracts And Data Model

Status:

- completed in UAT

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

Status:

- completed for the active interactive runtime path

Repo modules:

- `apps/api/src/SalesPlanning.Api/Infrastructure/Postgres`
- `apps/api/src/SalesPlanning.Api/Application`

Key backlog:

- add incremental aggregate maintenance
- remove remaining coarse recalculation behavior

### Wave 3: Query / Command API

Status:

- partially completed in UAT

Repo modules:

- `apps/api/src/SalesPlanning.Api/Controllers`
- `apps/api/src/SalesPlanning.Api/Contracts`
- `apps/api/src/SalesPlanning.Api/Application`

Key backlog:

- complete `v2` query endpoint split
- narrow remaining broad refresh paths
- keep command responses patch-oriented

### Wave 4: Frontend Grid Refactor

Status:

- partially completed in UAT

Repo modules:

- `apps/web/src/App.tsx`
- `apps/web/src/components/PlanningGrid.tsx`
- `apps/web/src/lib/api.ts`
- `apps/web/src/lib/types.ts`
- `apps/web/src/styles/app.css`

Key backlog:

- move to AG Grid SSRM
- keep lazy branch loading
- retain patch-based updates
- preserve compact menu UX

### Wave 5: Master-Data Maintenance

Status:

- completed in UAT for the current Phase 1 domains

Repo modules:

- `apps/web/src/*`
- `apps/api/src/SalesPlanning.Api/*`

Key backlog:

- Inventory Profile CRUD/import/export
- Pricing Policy CRUD/import/export
- Seasonality & Events CRUD/import/export
- Vendor Supply Profile CRUD/import/export

### Wave 6: Async Jobs And Deployment Cleanup

Status:

- not complete

Repo modules:

- `apps/api/tools/*`
- `infra/aws/*`

Key backlog:

- async import/export jobs
- deployment cleanup runbook
- remove stale runtime paths after cutover

### Wave 7: Security Hardening

Status:

- substantially complete for UAT, with a few remaining hardening steps

Repo modules:

- `apps/api/src/SalesPlanning.Api/Program.cs`
- `infra/aws/*`

Key backlog:

- strict backend auth
- role separation refinement
- ALB HTTPS origin completion
- observability finalization
- deletion of the parked rollback DB after acceptance

## Phase 2

Goal:

- deliver expert merchandising recommendations and decision support

Phase 2 design constraint:

- Phase 2 must reuse the existing Phase 1 master-data contracts, planning core, and import/export semantics
- no redesign of the transactional planning core is allowed unless required to improve model accuracy or performance

### Wave 8: AI Data Services

Prerequisites:

- stable Phase 1 reconciliation routines
- import/export job orchestration
- performance instrumentation on live planning transactions

Repo modules:

- new AI integration contracts
- planning and policy query services

Key backlog:

- build recommendation context APIs
- add evaluation datasets

### Wave 9: Recommendation Services

Prerequisites:

- approved business policy models for pricing and markdown recommendations
- evaluation datasets from actual sales, inventory, and sell-through history

Key backlog:

- price recommendation engine
- markdown recommendation engine
- forecast guidance engine
- explainability and confidence scoring
- category-aware heuristics for baby products

### Wave 10: AI Review UX

Prerequisites:

- recommendation APIs with structured outputs
- role-aware review workflow

Repo modules:

- `apps/web/src/*`

Key backlog:

- recommendation panels
- explainability UI
- approval / accept / reject flows
