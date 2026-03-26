# AWS Demo / UAT Deployment

This deployment path is designed for a low-cost AWS test environment in `ap-southeast-5` (Malaysia) while preserving the validated planning engine and persisted data.

## Runtime architecture

- Frontend: static Vite build uploaded to `sales-planning-demo-web-427304877733-ap-southeast-5-an`
- API: `.NET 8` ASP.NET Core API hosted on AWS Lambda behind HTTP API
- Persistence: SQLite database synchronized to `s3://sales-planning-demo-files-427304877733-ap-southeast-5-an/planning/demo-planning.db`
- Identity: Microsoft Entra single-tenant sign-in on the web app with API JWT validation

## Why this path

- Keeps the current planning rules and tests intact
- Uses cloud storage for persistence without immediately rewriting the repository to DynamoDB or PostgreSQL
- Keeps costs low for test/UAT while still demonstrating persisted planning, store profile maintenance, and protected API access

## Deployment prerequisites

- Admin or delegated AWS console/CloudShell access in account `427304877733`
- Existing role `sales-planning-demo-lambda-exec`
- Existing S3 buckets:
  - `sales-planning-demo-web-427304877733-ap-southeast-5-an`
  - `sales-planning-demo-files-427304877733-ap-southeast-5-an`

## First API deployment from AWS CloudShell

1. Upload or clone this repository into CloudShell.
2. Change directory into:
   - `apps/api/src/SalesPlanning.Api`
3. Install the Lambda deployment tool if needed:
   - `dotnet tool install -g Amazon.Lambda.Tools`
4. Deploy the stack:
   - `dotnet lambda deploy-serverless`
5. Accept or review the defaults:
   - stack name: `sales-planning-demo-api`
   - region: `ap-southeast-5`
   - storage bucket: `sales-planning-demo-files-427304877733-ap-southeast-5-an`
   - allowed origin: `https://d22xc0mfhkv9bk.cloudfront.net`
   - Entra tenant/client IDs matching the SPA registration

## Frontend build and publish

1. Build the web app with the deployed API URL:
   - `VITE_API_BASE_URL=https://<api-id>.execute-api.ap-southeast-5.amazonaws.com/api/v1 npm run build`
2. Upload the `apps/web/dist` contents into:
   - `sales-planning-demo-web-427304877733-ap-southeast-5-an`
3. Enable static website hosting for the web bucket if not already enabled.

## Operational notes

- This is a production-ready demo/UAT pattern, not the final production persistence architecture.
- The recommended production target remains managed PostgreSQL plus S3 object storage.
- The API is now protected by Entra token validation and is intended to be called only from the CloudFront frontend origin.
- The SQLite-over-S3 persistence model is still single-writer oriented, so this deployment should remain low-concurrency UAT/demo only.
