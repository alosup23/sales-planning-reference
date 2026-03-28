# Reference Implementation Notes

## Current State

The current codebase is the working UAT reference implementation, not the final target architecture.

Current runtime characteristics:

- frontend:
  - React 18
  - AG Grid Enterprise
  - React Query
- backend:
  - .NET 8 ASP.NET Core API
  - action-driven planning service
- persistence:
  - current working UAT runtime on `s3-sqlite`
  - PostgreSQL migration path partially added in code

## Current Backend Shape

The API already contains the core planning concepts:

- manual edits validate locks and write coordinates
- splash actions allocate only to unlocked targets
- rollups and derived measure recalculation exist in the service layer
- audit entries capture action metadata and deltas
- Store Profile and Product Profile maintenance use workbook-compatible import/export paths

Important current limitation:

- the present planning service and repository patterns are still too coarse for the final performance target because they remain closer to whole-slice orchestration than patch-oriented query/command behavior

## Current Frontend Shape

The React app is organized around:

- API contracts in `src/lib`
- grid rendering in `src/components`
- app-level orchestration in `src/App.tsx`

The current grid already supports:

- tree rows for `Store -> Department -> Class -> Subclass`
- alternate department-first projections
- grouped `Year -> Month` columns
- aggregate row banding
- lock, splash, growth-factor, and workbook flows

Important current limitation:

- the frontend still refreshes too broadly after commands and still uses the client-side row model instead of the target server-side row model

## Target Direction

The source-of-truth target design is now documented in:

- [docs/uat-product-requirements.md](/Users/aloysius/Documents/New%20project/docs/uat-product-requirements.md)
- [docs/aws-target-state-architecture.md](/Users/aloysius/Documents/New%20project/docs/aws-target-state-architecture.md)
- [docs/master-data-file-formats.md](/Users/aloysius/Documents/New%20project/docs/master-data-file-formats.md)
- [docs/non-functional-requirements.md](/Users/aloysius/Documents/New%20project/docs/non-functional-requirements.md)
- [docs/calculation-and-reconciliation-spec.md](/Users/aloysius/Documents/New%20project/docs/calculation-and-reconciliation-spec.md)
- [docs/ai-phase-2-merchandising-architecture.md](/Users/aloysius/Documents/New%20project/docs/ai-phase-2-merchandising-architecture.md)
- [docs/phase-roadmap-and-backlog.md](/Users/aloysius/Documents/New%20project/docs/phase-roadmap-and-backlog.md)

The next implementation waves should focus on:

1. native PostgreSQL repository, not PostgreSQL plus SQLite mirror
2. command responses that return changed-cell patches only
3. AG Grid Server-Side Row Model with lazy branch loading
4. async admin jobs for imports, exports, and year generation
5. restored backend authorization after the runtime is stabilized
