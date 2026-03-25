# AWS IAM Setup For Sales Planning Demo

This folder contains least-privilege starter policies for a low-cost AWS test/UAT deployment of the Sales Planning demo in `ap-southeast-5` (Asia Pacific - Malaysia).

## Recommended setup

Create these identities while signed in with an administrative account in the AWS console:

1. Customer managed policy: `SalesPlanningDemoDeployerPolicy`
2. IAM role: `sales-planning-demo-lambda-exec`
3. Customer managed policy: `SalesPlanningDemoLambdaExecutionPolicy`
4. IAM user: `sales-planning-demo-deployer`

## Purpose of each identity

- `sales-planning-demo-deployer`
  - Used only to deploy and update the demo stack.
  - Should have programmatic access only.
  - Should not have console access unless specifically needed.
  - Assumes bootstrap resources are created once by an admin user.

- `sales-planning-demo-lambda-exec`
  - Runtime execution role for Lambda functions.
  - Trusted principal must be `lambda.amazonaws.com`.
  - Used by the deployed API only, never by a human user.

## Resource naming convention

Use this resource prefix consistently:

- `sales-planning-demo-`

Suggested resource names:

- S3 frontend bucket: `sales-planning-demo-web-427304877733-ap-southeast-5-an`
- S3 documents/import-export bucket: `sales-planning-demo-files-427304877733-ap-southeast-5-an`
- DynamoDB table: `sales-planning-demo-planning`
- Lambda function: `sales-planning-demo-api`
- API name: `sales-planning-demo-api`

## Placeholders to replace

Before creating the policies, replace these placeholders:

- `<ACCOUNT_ID>` with your 12-digit AWS account ID
- `<WEB_BUCKET_NAME>` with `sales-planning-demo-web-427304877733-ap-southeast-5-an`
- `<FILES_BUCKET_NAME>` with `sales-planning-demo-files-427304877733-ap-southeast-5-an`

## Console creation order

1. Create the Lambda execution managed policy from `sales-planning-demo-lambda-execution-policy.json`
2. Create the Lambda execution role from `sales-planning-demo-lambda-trust-policy.json`
3. Attach:
   - `AWSLambdaBasicExecutionRole`
   - `SalesPlanningDemoLambdaExecutionPolicy`
4. Create the deployer managed policy from `sales-planning-demo-deployer-policy.json`
5. Create the IAM user `sales-planning-demo-deployer`
6. Enable programmatic access only
7. Attach `SalesPlanningDemoDeployerPolicy`
8. Store the generated access key securely in a password manager or AWS Secrets Manager

## Bootstrap resources to create once as admin

Before using the limited deployer user, create these resources once in `ap-southeast-5`:

- DynamoDB table: `sales-planning-demo-planning`
- S3 bucket: `sales-planning-demo-web-427304877733-ap-southeast-5-an`
- S3 bucket: `sales-planning-demo-files-427304877733-ap-southeast-5-an`
- Lambda execution role: `sales-planning-demo-lambda-exec`

The current demo persistence path stores the planning SQLite database object in the files bucket. The DynamoDB table can remain available for future metadata or distributed lock expansion, but it is not required by the current demo runtime.

After bootstrap, the limited deployer user can update Lambda code/configuration, update API Gateway integration, and upload web assets and file artifacts.

## Scope notes

- These policies are intended for a demo/UAT deployment only.
- They deliberately avoid broad administrative access.
- They are scoped for Malaysia region where possible.
- Global services such as IAM and S3 bucket naming still require account-level scope in places.

## Free-tier and low-cost notes

- Lambda, API Gateway, DynamoDB, S3, and static hosting can fit within AWS free-eligible usage for a modest demo/UAT environment, depending on your account's current free-plan or credit eligibility and actual traffic.
- Do not assume zero cost for sustained multi-user usage.
- For production scale, move the authoritative planning store to PostgreSQL on managed database infrastructure.
