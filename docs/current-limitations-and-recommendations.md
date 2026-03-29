# Current Limitations And Recommendations

## Purpose

This document records the known current limitations of the live Phase 1 UAT platform and the recommended next steps before production and before Phase 2 AI rollout.

## 1. Interactive Grid Performance

Current limitation:

- the grid still uses the client-side row model
- some recalculation paths remain broader than the desired delta-only model

Recommendation:

- move to AG Grid Server-Side Row Model
- complete visible-block query delivery
- move recalculation to strict impacted-ancestor scope only

## 2. Import / Export Processing

Current limitation:

- workbook imports and exports are still synchronous HTTP operations

Recommendation:

- move workbook processing to queued background jobs
- add progress tracking and exception download history

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

## 7. Reconciliation And Observability

Current limitation:

- reconciliation is specified, but not yet implemented as a fully operational scheduled routine
- observability is still lighter than a production standard

Recommendation:

- add scheduled reconciliation jobs
- add dashboards and alarms for:
  - API latency
  - query failure rate
  - import failure rate
  - command latency
  - DB connectivity

## 8. Phase 2 Readiness

Current strength:

- Phase 1 already captures the data foundations required for Phase 2 recommendation workflows

Remaining work:

- recommendation data services
- policy-aware recommendation engines
- explainability UX
- human review and approval workflow

## 9. Priority Recommendation Order

1. Delta recalculation and tighter performance optimization
2. AG Grid SSRM
3. Async import / export job pipeline
4. ALB HTTPS origin completion
5. Reconciliation jobs and production observability
6. Phase 2 recommendation APIs and AI review workflow
