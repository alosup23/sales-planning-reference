# AWS Demo / UAT Deployment

This deployment path moves the persisted planning store from the prior S3-backed SQLite model to `Amazon RDS for PostgreSQL` in `ap-southeast-5` while keeping the current planning engine and UI behavior intact.

## Runtime architecture

- Frontend: static Vite build uploaded to `sales-planning-demo-web-427304877733-ap-southeast-5-an`
- API: `.NET 8` ASP.NET Core API hosted on AWS Lambda behind HTTP API
- Persistence:
  - authoritative store: `Amazon RDS for PostgreSQL`
  - runtime compatibility cache: ephemeral SQLite file in Lambda `/tmp`
- Identity:
  - current UAT access gate: Microsoft Entra sign-in on the web app
  - next security milestone after PostgreSQL stabilization: restore clean backend API authorizer / audience validation

## Free-tier guardrails used for this UAT path

- Region: `ap-southeast-5`
- Database template equivalent: `Free Tier`
- Instance count: `1`
- Instance class: `db.t3.micro`
- Storage: `20 GB`
- Storage autoscaling: `disabled` by omission of max allocated storage
- Public access: `disabled`

Note:
- Exact free-tier eligibility depends on the AWS account plan in effect at deployment time.
- This runbook keeps the settings aligned with the account-safe guardrails requested for UAT, but it does not guarantee zero cost outside AWS free-tier eligibility.

## Infrastructure created for PostgreSQL UAT

- Default VPC: `vpc-0c29c4e611198869c`
- Lambda security group: `sg-072a7438e07397e55`
- RDS security group: `sg-02e38011d0de1a5d5`
- Secrets Manager VPC endpoint security group: `sg-0b1c1688d6334f924`
- DB subnet group: `sales-planning-demo-rds-subnets`
- Secrets Manager interface endpoint: `vpce-00bd3a5f4489815a1`
- RDS instance identifier: `sales-planning-demo-pg`

## PostgreSQL operating model

- PostgreSQL schema is created from SQL migration scripts in:
  - `apps/api/src/SalesPlanning.Api/Infrastructure/Postgres/Migrations`
- Startup seeding is intentionally removed from the database bootstrap path.
- `seed_runs` is used as the seed/version ledger for managed imports and cutover loads.
- `planning_data_state` tracks the authoritative persisted data version.

## Admin tooling

Use the PostgreSQL admin tool for migrations and one-time SQLite cutover imports:

- project:
  - `apps/api/tools/SalesPlanning.PostgresAdmin`

Commands:

1. Apply migrations
   - `dotnet run --project apps/api/tools/SalesPlanning.PostgresAdmin -- migrate "<connection-string>"`

2. Import an existing SQLite snapshot into PostgreSQL
   - `dotnet run --project apps/api/tools/SalesPlanning.PostgresAdmin -- import-sqlite "<connection-string>" "<sqlite-db-path>" "<seed-key>" "<source-name>"`

Recommended seed key for the live cutover:
- `live-s3-sqlite-cutover-20260328`

## Lambda deployment settings

The Lambda template now supports PostgreSQL mode with:

- `PlanningStorageMode=postgres`
- `PlanningPostgresSecretArn=<secret-arn>`
- `PlanningDbPath=/tmp/sales-planning-demo/planning.db`
- `LambdaSubnetIds=subnet-0829b0ba1df103016,subnet-0fa1876970692d5ce,subnet-0bccfffc49dfea45c`
- `LambdaSecurityGroupIds=sg-072a7438e07397e55`

The current template keeps:
- `PlanningSecurityMode=disabled`

That is deliberate for the PostgreSQL cutover wave. Backend API hardening should be restored after the database migration is stable.

## Cutover sequence

1. Provision RDS, subnet group, VPC endpoint, and security groups.
2. Wait until the RDS instance is `available`.
3. Obtain the managed master secret ARN from the RDS instance.
4. Update the Lambda execution role with:
   - `AWSLambdaVPCAccessExecutionRole`
   - `secretsmanager:GetSecretValue` for the PostgreSQL secret
5. Migrate schema into PostgreSQL.
6. Import the current authoritative SQLite dataset into PostgreSQL.
7. Update Lambda environment to PostgreSQL mode and attach VPC config.
8. Redeploy Lambda code.
9. Smoke-test:
   - health
   - store scopes
   - scoped grid load
   - store-first and department-first views
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

The PostgreSQL cutover improves authoritative persistence and query flexibility, but it does not remove browser and Lambda payload limits.
