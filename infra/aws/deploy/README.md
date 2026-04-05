# AWS Demo / UAT Deployment

This document describes the current live AWS UAT deployment and the remaining steps required to move from the working Phase 1 UAT platform to a fully hardened production operating model.

The detailed target-state architecture now lives in:

- [docs/aws-target-state-architecture.md](/Users/aloysius/Documents/New%20project/docs/aws-target-state-architecture.md)
- [docs/uat-product-requirements.md](/Users/aloysius/Documents/New%20project/docs/uat-product-requirements.md)
- [docs/master-data-file-formats.md](/Users/aloysius/Documents/New%20project/docs/master-data-file-formats.md)
- [docs/phase-roadmap-and-backlog.md](/Users/aloysius/Documents/New%20project/docs/phase-roadmap-and-backlog.md)

## Current live UAT runtime

- Frontend: static Vite build uploaded to `sales-planning-demo-web-427304877733-ap-southeast-5-an`
- Public app endpoint:
  - `https://d22xc0mfhkv9bk.cloudfront.net`
- API: `.NET 8` ASP.NET Core API on `ECS Fargate`
- Persistence:
  - live working mode: `Amazon RDS for PostgreSQL`
  - active DB instance:
    - `sales-planning-demo-pg-private`
  - active DB subnet group:
    - `sales-planning-demo-rds-private-subnets`
- Identity:
  - Microsoft Entra sign-in on the web app
  - backend API authorization enabled
- Edge protection:
  - `AWS WAF` attached to CloudFront
  - ALB ingress restricted to the CloudFront origin-facing prefix list
  - CloudFront origin header validation enabled on the ALB listener

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
- ECS deployment template:
  - `infra/aws/cloudformation/interactive-api-ecs.yml`
- private-subnet RDS network template:
  - `infra/aws/cloudformation/rds-private-subnets.yml`
- CloudFront WAF template:
  - `infra/aws/cloudformation/cloudfront-waf.yml`

## Target PostgreSQL operating model

- PostgreSQL schema is created from SQL migration scripts in:
  - `apps/api/src/SalesPlanning.Api/Infrastructure/Postgres/Migrations`
- Startup seeding must remain removed from the database bootstrap path.
- `seed_runs` is used as the seed/version ledger for managed imports and cutover loads.
- `planning_data_state` tracks the authoritative persisted data version.

## Admin tooling

Use the PostgreSQL admin tool for:

- schema migrations
- future controlled migration and admin operations

- project:
  - `apps/api/tools/SalesPlanning.PostgresAdmin`

Commands:

1. Apply migrations
   - `dotnet run --project apps/api/tools/SalesPlanning.PostgresAdmin -- migrate "<connection-string>"`

2. Inspect PostgreSQL planning storage
   - `dotnet run --project apps/api/tools/SalesPlanning.PostgresAdmin -- stats "<connection-string>"`

## Important current status

At the time of this document update:

- the live app is running on `ECS Fargate + RDS PostgreSQL`
- the active RDS instance is in true private subnets
- the previous RDS instance remains only as a temporary stopped rollback buffer
- CloudFront WAF is deployed
- ALB HTTPS origin completion remains deferred until Route 53 + ACM are in place

Recommended target interactive runtime:

- `ECS Fargate` for the interactive API
- `RDS PostgreSQL` for persistence
- optional `ElastiCache Redis` for hot metadata and aggregate caches

## Remaining hardening sequence

1. Add Route 53 hosted zone and ACM certificates.
2. Move CloudFront-to-ALB origin traffic to HTTPS.
3. Move ECS tasks into private subnets if the UAT cost model and NAT/VPC endpoint plan permit it.
4. Convert workbook import/export to async jobs.
5. Add reconciliation routines and operational dashboards.
6. Retire the stopped rollback DB once acceptance is complete.

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
