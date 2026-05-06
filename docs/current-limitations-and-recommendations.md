# Current Limitations And Recommendations

## Purpose

This document records the known current limitations of the live Phase 1 UAT platform and the recommended next steps before production and before Phase 2 AI rollout.

## 1. Recalculation Efficiency

Current limitation:

- edit, splash, and growth-factor processing now use targeted server-side working sets, but the highest-volume aggregate paths still do more recalculation work than the final delta-only target model
- the latest focused live replay has removed the prior store-rollup, zero-total-cost `GP%`, and aggregate month `GP%` restore failures, but the full authenticated matrix should still be rerun after each material planning-engine change

Recommendation:

- move recalculation fully to strict impacted-ancestor scope only
- add persisted aggregate projections for the hottest summary paths
- keep measuring leaf-edit, splash, and growth-factor latency under larger UAT data volumes
- add an explicit replayable calculation engine contract for:
  - year leaf edits
  - rate-driven splashes
  - growth-factor restore after reread, undo, redo, and save

## 1.1 Growth-Factor And Year-Edit Accuracy

Current state:

- persistent `baseValue × growthFactor` semantics are now materially more stable across reread, save, and live year-growth replay than in the earlier UAT builds

Current limitation:

- full authenticated matrix replay should still be rerun after each material planning-engine change to confirm there is no remaining year-level drift outside the focused replay set

Recommendation:

- separate editable user intent from derived recalculation state in the persistence model
- persist and test `baseValue`, `growthFactor`, and `effectiveValue` as first-class fields with replay-safe undo and redo
- add dedicated reconciliation checks for year edits and year growth-factor restore

## 1.2 Derived Measure Rule Drift

Current state:

- the focused live `GP%` forward-and-restore replay now returns the expected aggregate value on the target path, including the previously noisy `MKAP` scenario

Current limitation:

- broader authenticated replay coverage is still required after planning-engine changes so equivalent-target and restore paths remain accurate across other branches, stores, and lock combinations

Recommendation:

- encode all measure rules in a single explicit rule engine or measure-strategy layer
- prohibit ambiguous fallback behavior inside ad hoc service methods
- add measure-by-measure contract tests across leaf month, leaf year, aggregate month, and aggregate year
- keep instrumented forward-and-restore replay for `GP%` so total revenue, total costs, and feasible equivalent targets are captured in logs whenever regressions reappear

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

1. Normalize all derived-measure edit and splash rules into one explicit rule engine
2. Delta recalculation and persisted aggregate optimization
3. Full authenticated regression matrix after each material planning-engine change
4. Complete master-data admin UX separation and CRUD consistency
5. Private-subnet ECS with private secret retrieval
6. ALB HTTPS origin completion
7. Production observability and alerting
8. Retire the rollback DB
9. Phase 2 recommendation APIs and AI review workflow

## 9. Architecture, UX, Performance, And Security Review

Architecture improvement opportunities:

- split planning query, planning command, and master-data admin concerns more cleanly
- isolate recalculation, allocation, and derived-measure solving behind explicit strategy interfaces
- reduce dependence on UI-side synthetic aggregate recomputation for authoritative behavior

UX improvement opportunities:

- move all master-data maintenance to a dedicated top-level section
- provide clearer inline explanations for preserved versus derived measures during edits and splashes
- show lock legends for explicit and implicit lock coloring
- provide a visible save state, dirty state, and reconciliation status area

Performance improvement opportunities:

- add Redis or equivalent cache only for safe read projections, not for write authority
- reduce temp-table churn and redundant rereads on draft and growth-factor paths
- instrument branch-level recalculation fan-out and patch size at runtime

Security improvement opportunities:

- complete HTTPS-only CloudFront-to-origin transport
- move deployment-time DB credentials to private secret retrieval
- add stronger tenant, role, and master-data admin authorization boundaries
- add tamper-evident audit export and retention policy documentation
