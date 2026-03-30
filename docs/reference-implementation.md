# Reference Implementation Notes

## Current State

The current codebase is the live Phase 1 UAT reference implementation. It is functional, deployed, and aligned with the current AWS UAT operating model, but it is not yet the full final production target architecture.

Current runtime characteristics:

- frontend:
  - React 18
  - AG Grid Enterprise
  - React Query
- backend:
  - .NET 8 ASP.NET Core API on ECS Fargate
  - action-driven planning service
- persistence:
  - live UAT runtime on `Amazon RDS for PostgreSQL`
  - live database currently hosted in true private subnets
- edge and access:
  - `S3 + CloudFront` for the web build
  - `AWS WAF` attached to CloudFront
  - origin-protected ALB for ECS
  - Microsoft Entra-backed API authorization

## Current Backend Shape

The API already contains the core planning concepts:

- manual edits validate locks and write coordinates
- splash actions allocate only to unlocked targets
- rollups and derived measure recalculation exist in the service layer
- audit entries capture action metadata and deltas
- workbook-compatible import/export paths exist for all Phase 1 master-data domains

Important current limitation:

- the current planning service and repository patterns are still too coarse for the final performance target because recalculation remains broader than the desired delta-only model

## Current Frontend Shape

The React app is organized around:

- API contracts in `src/lib`
- grid rendering in `src/components`
- app-level orchestration in `src/App.tsx`

The current grid already supports:

- server-side tree rows for `Store -> Department -> Class -> Subclass`
- alternate department-first projections
- grouped `Year -> Month` columns
- aggregate row banding
- lock, splash, growth-factor, workbook, and undo/redo flows
- async job progress for workbook and master-data import/export
- manual reconciliation job execution with downloadable reports

Important current limitation:

- the server-side row model and server-composed department view are now live, but some planning recalculation paths are still broader than the final delta-only target and the async job manager is not yet externally durable

## Target Direction

The source-of-truth target design is now documented in:

- [docs/uat-product-requirements.md](/Users/aloysius/Documents/New%20project/docs/uat-product-requirements.md)
- [docs/aws-target-state-architecture.md](/Users/aloysius/Documents/New%20project/docs/aws-target-state-architecture.md)
- [docs/current-uat-aws-configuration.md](/Users/aloysius/Documents/New%20project/docs/current-uat-aws-configuration.md)
- [docs/api-endpoints-and-transaction-flows.md](/Users/aloysius/Documents/New%20project/docs/api-endpoints-and-transaction-flows.md)
- [docs/master-data-file-formats.md](/Users/aloysius/Documents/New%20project/docs/master-data-file-formats.md)
- [docs/non-functional-requirements.md](/Users/aloysius/Documents/New%20project/docs/non-functional-requirements.md)
- [docs/calculation-and-reconciliation-spec.md](/Users/aloysius/Documents/New%20project/docs/calculation-and-reconciliation-spec.md)
- [docs/ai-phase-2-merchandising-architecture.md](/Users/aloysius/Documents/New%20project/docs/ai-phase-2-merchandising-architecture.md)
- [docs/phase-roadmap-and-backlog.md](/Users/aloysius/Documents/New%20project/docs/phase-roadmap-and-backlog.md)
- [docs/user-guide.md](/Users/aloysius/Documents/New%20project/docs/user-guide.md)
- [docs/training-process-overview.md](/Users/aloysius/Documents/New%20project/docs/training-process-overview.md)
- [docs/current-limitations-and-recommendations.md](/Users/aloysius/Documents/New%20project/docs/current-limitations-and-recommendations.md)

The next implementation waves should focus on:

1. delta-based recalculation instead of broader working-set recalculation
2. durable async job persistence and scheduled reconciliation orchestration
3. ALB HTTPS origin linkage and tighter private networking for ECS
4. production-grade observability and alerting
5. Phase 2 AI service boundaries and recommendation workflows
