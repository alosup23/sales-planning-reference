# AWS Demo / UAT Deployment

This document describes the current AWS UAT deployment status and the intended migration direction toward the target AWS architecture.

The detailed target-state architecture now lives in:

- [docs/aws-target-state-architecture.md](/Users/aloysius/Documents/New%20project/docs/aws-target-state-architecture.md)
- [docs/uat-product-requirements.md](/Users/aloysius/Documents/New%20project/docs/uat-product-requirements.md)
- [docs/master-data-file-formats.md](/Users/aloysius/Documents/New%20project/docs/master-data-file-formats.md)
- [docs/phase-roadmap-and-backlog.md](/Users/aloysius/Documents/New%20project/docs/phase-roadmap-and-backlog.md)

## Current live UAT runtime

- Frontend: static Vite build uploaded to `sales-planning-demo-web-427304877733-ap-southeast-5-an`
- API: `.NET 8` ASP.NET Core API currently hosted on AWS Lambda behind HTTP API
- Persistence:
  - current working live mode: `S3-backed SQLite`
  - in-progress migration target: `Amazon RDS for PostgreSQL`
- Identity:
  - current UAT access gate: Microsoft Entra sign-in on the web app
  - next security milestone after PostgreSQL stabilization: restore clean backend API authorizer / audience validation

## UAT cost guardrails

- Region target for future clean AWS posture: `ap-southeast-1` or another approved Southeast Asia region
- During UAT keep:
  - `1` database instance
  - `1` interactive API service instance where possible
  - storage autoscaling disabled where free-tier guardrails matter
  - public DB exposure disabled

Note:
- Exact free-tier eligibility depends on the AWS account plan in effect at deployment time.
- This runbook keeps the settings aligned with the account-safe guardrails requested for UAT, but it does not guarantee zero cost outside AWS free-tier eligibility.

## PostgreSQL migration assets already added

- SQL migration scripts under:
  - `apps/api/src/SalesPlanning.Api/Infrastructure/Postgres/Migrations`
- PostgreSQL repository and connection resolver
- PostgreSQL admin import / migration tool

## Target PostgreSQL operating model

- PostgreSQL schema is created from SQL migration scripts in:
  - `apps/api/src/SalesPlanning.Api/Infrastructure/Postgres/Migrations`
- Startup seeding must remain removed from the database bootstrap path.
- `seed_runs` is used as the seed/version ledger for managed imports and cutover loads.
- `planning_data_state` tracks the authoritative persisted data version.

## Admin tooling

Use the PostgreSQL admin tool for:

- schema migrations
- one-time SQLite cutover imports
- future controlled migration and admin operations

- project:
  - `apps/api/tools/SalesPlanning.PostgresAdmin`

Commands:

1. Apply migrations
   - `dotnet run --project apps/api/tools/SalesPlanning.PostgresAdmin -- migrate "<connection-string>"`

2. Import an existing SQLite snapshot into PostgreSQL
   - `dotnet run --project apps/api/tools/SalesPlanning.PostgresAdmin -- import-sqlite "<connection-string>" "<sqlite-db-path>" "<seed-key>" "<source-name>"`

Recommended seed key for the live cutover:
- `live-s3-sqlite-cutover-20260328`

## Important current status

At the time of this document update:

- the working live app remains on `s3-sqlite`
- the PostgreSQL migration code path has been added, but the full live cutover is not yet complete
- the final target runtime for performance is no longer Lambda for interactive planning traffic

Recommended target interactive runtime:

- `ECS Fargate` for the interactive API
- `RDS PostgreSQL` for persistence
- optional `ElastiCache Redis` for hot metadata and aggregate caches

## Recommended cutover sequence

1. Finalize PostgreSQL schema and native repository behavior.
2. Run one-time import into PostgreSQL out of band.
3. Validate row counts, aggregates, and workbook compatibility.
4. Deploy interactive API on `ECS Fargate`.
5. Point CloudFront or API routing to the new interactive API.
6. Retest:
   - health
   - store scopes
   - scoped grid load
   - branch expansion
   - edit / splash / lock
   - store/product maintenance
   - import/export

## Hydration guidance

The current persisted SQLite object is roughly `78 MB`, which is too large to treat as a trivial startup payload.

Recommended UX strategy:
- hydrate only:
  - authentication/session shell
  - store scopes
  - selected scope grid slice
- keep department scope incremental even on PostgreSQL
- avoid forcing full department-across-all-stores expansion on startup

The PostgreSQL cutover improves authoritative persistence and query flexibility, but it does not remove browser payload limits. The target architecture therefore keeps the planning UI on an incremental, server-driven loading model.
