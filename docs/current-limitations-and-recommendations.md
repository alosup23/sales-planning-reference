# Current Limitations And Recommendations

## Purpose

This document records the known current limitations of the live Phase 1 UAT platform and the recommended next steps before production and before Phase 2 AI rollout.

## 1. Recalculation Efficiency

Current limitation:

- edit, splash, and growth-factor processing now use targeted server-side working sets, but the highest-volume aggregate paths still do more recalculation work than the final delta-only target model

Recommendation:

- move recalculation fully to strict impacted-ancestor scope only
- add persisted aggregate projections for the hottest summary paths
- keep measuring leaf-edit, splash, and growth-factor latency under larger UAT data volumes

## 2. ECS Network Posture

Current limitation:

- the active ECS tasks still run in the current public subnets for UAT simplicity

Recommendation:

- move ECS tasks into private subnets with NAT or the required VPC endpoint design once the cost and operational tradeoffs are approved

## 3. CloudFront To ALB Origin Encryption

Current limitation:

- CloudFront still uses `http-only` to the ALB origin

Reason:

- ALB HTTPS completion requires a Route 53 hostname and ACM certificate in the ALB region

Recommendation:

- complete Route 53 and ACM setup
- move CloudFront origin protocol policy to HTTPS-only

## 4. Credential Delivery Model

Current limitation:

- to avoid ECS startup stalls on runtime secret resolution, the current live ECS stack injects the PostgreSQL username and password directly into task environment variables during CloudFormation deployment

Recommendation:

- for production, replace deployment-time DB credential injection with a private Secrets Manager or SSM retrieval path
- if Secrets Manager remains the target, add the required private endpoint or equivalent network path so startup never depends on public egress

## 5. Rollback Database

Current limitation:

- the prior DB instance remains as a temporary stopped rollback copy

Recommendation:

- snapshot and delete it after UAT acceptance to avoid auto-restart and cost drift

## 6. Async Job And Reconciliation Operations

Current state:

- import, export, and reconciliation jobs are now durable in PostgreSQL
- reconciliation scheduling, report persistence, and retention are operationalized inside the API service

Current limitation:

- the scheduler still runs as an in-service background worker rather than as a fully external orchestrator

Recommendation:

- for production, consider moving scheduled reconciliation triggering to EventBridge or an equivalent external scheduler
- add dashboards and alarms for:
  - API latency
  - command latency
  - async job backlog
  - import failure rate
  - reconciliation failures

## 7. Phase 2 Readiness

Current strength:

- Phase 1 now captures the data foundations required for Phase 2 recommendation workflows

Remaining work:

- recommendation data services
- policy-aware recommendation engines
- explainability UX
- human review and approval workflow

## 8. Priority Recommendation Order

1. Delta recalculation and persisted aggregate optimization
2. Private-subnet ECS with private secret retrieval
3. ALB HTTPS origin completion
4. Production observability and alerting
5. Retire the rollback DB
6. Phase 2 recommendation APIs and AI review workflow
