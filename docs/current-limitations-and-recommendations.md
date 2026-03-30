# Current Limitations And Recommendations

## Purpose

This document records the known current limitations of the live Phase 1 UAT platform and the recommended next steps before production and before Phase 2 AI rollout.

## 1. Recalculation Efficiency

Current limitation:

- the grid now uses AG Grid Server-Side Row Model and the department view is server-composed, but some planning recalculation paths still touch broader working sets than the desired final delta-only model

Recommendation:

- move recalculation fully to strict impacted-ancestor scope only
- add persisted aggregate projections for the hottest summary paths
- continue measuring edit, splash, and growth-factor latency under larger UAT volumes

## 3. CloudFront To ALB Origin Encryption

Current limitation:

- CloudFront still uses `http-only` to the ALB origin

Reason:

- ALB HTTPS completion requires a Route 53 hostname and ACM certificate in the ALB region

Recommendation:

- complete Route 53 and ACM setup
- move CloudFront origin protocol policy to HTTPS-only

## 4. ECS Network Posture

Current limitation:

- the active ECS tasks still run in the current public subnets for UAT simplicity

Recommendation:

- evaluate a private-subnet ECS deployment with the required NAT or VPC endpoint design once cost and operational tradeoffs are approved

## 5. Rollback Database

Current limitation:

- the prior DB instance remains as a temporary stopped rollback copy

Recommendation:

- snapshot and delete it after UAT acceptance to avoid auto-restart and cost drift

## 6. Data Protection Keys

Current limitation:

- ASP.NET Data Protection keys are still container-local

Recommendation:

- persist keys outside the container if future features depend on stable encrypted cookie continuity or key reuse across tasks

## 2. Async Job Durability And Reconciliation Operations

Current limitation:

- import, export, and reconciliation now run through async background jobs with progress and downloadable outputs, but the current UAT job manager is in-memory inside the ECS task
- reconciliation is fully runnable on demand and is executed after imports, but it is not yet scheduled through an external durable scheduler

Recommendation:

- persist job state outside the API task for production
- add scheduled reconciliation jobs
- add dashboards and alarms for:
  - API latency
  - query failure rate
  - import failure rate
  - command latency
  - DB connectivity

## 3. CloudFront To ALB Origin Encryption

Current limitation:

- CloudFront still uses `http-only` to the ALB origin

Reason:

- ALB HTTPS completion requires a Route 53 hostname and ACM certificate in the ALB region

Recommendation:

- complete Route 53 and ACM setup
- move CloudFront origin protocol policy to HTTPS-only

## 4. ECS Network Posture

Current limitation:

- the active ECS tasks still run in the current public subnets for UAT simplicity

Recommendation:

- evaluate a private-subnet ECS deployment with the required NAT or VPC endpoint design once cost and operational tradeoffs are approved

## 5. Rollback Database

Current limitation:

- the prior DB instance remains as a temporary stopped rollback copy

Recommendation:

- snapshot and delete it after UAT acceptance to avoid auto-restart and cost drift

## 6. Data Protection Keys

Current limitation:

- ASP.NET Data Protection keys are still container-local

Recommendation:

- persist keys outside the container if future features depend on stable encrypted cookie continuity or key reuse across tasks

## 7. Phase 2 Readiness

Current strength:

- Phase 1 already captures the data foundations required for Phase 2 recommendation workflows

Remaining work:

- recommendation data services
- policy-aware recommendation engines
- explainability UX
- human review and approval workflow

## 8. Priority Recommendation Order

1. Delta recalculation and tighter performance optimization
2. Durable async job persistence and scheduled reconciliation
3. ALB HTTPS origin completion
4. Production observability and alerting
5. Private-subnet ECS review
6. Phase 2 recommendation APIs and AI review workflow
