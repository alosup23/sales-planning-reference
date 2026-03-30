# Issue History And Resolution

## Purpose

This document records the major issues encountered through the UAT build history, the implemented resolution, and whether the fix is confirmed in code.

## Issue Register

| ID | Issue | Resolution | Confirmed In Code |
|---|---|---|---|
| 1 | Lambda cold-start initialization caused planning startup timeouts | startup initialization was reduced, redundant SQLite init was skipped, and the interactive API was later moved to ECS Fargate | Yes |
| 2 | `/planning-store-scopes` timed out during cold start | lightweight store-root lookup and startup path reduction were added | Yes |
| 3 | `/grid-slices` scanned too much data and timed out | slice queries were constrained to visible cells and later moved to server-composed grid views | Yes |
| 4 | Frontend called the wrong API origin and received HTML instead of JSON | production API base URL was pinned and CloudFront bundles were rebuilt | Yes |
| 5 | Store and department hierarchy navigation was clumsy and scope selection was manual | click-to-scope behavior, lazy branch loading, and `All Stores` support were added | Yes |
| 6 | Department view expansion logic was inconsistent across layouts | server-composed Department view and explicit layout-driven expansion rules were added | Yes |
| 7 | Department view could leak Store-scope state from the Store view | cache keys and scope handling were separated so Department view loads across all stores | Yes |
| 8 | Undo and redo did not satisfy the required depth | application-backed undo and redo with a depth of `30` was implemented | Yes |
| 9 | Planning commands forced broad client refreshes | server patch responses were introduced and the active grid view updates in place | Yes |
| 10 | Store edits did not reliably appear in Department view | inactive planning views now reload from the canonical server model after edits | Yes |
| 11 | Leaf edits on Postgres produced server errors because audit delta keys were missing | audit delta and command delta identifiers were fixed in the Postgres write path | Yes |
| 12 | Leaf edits later hit `504` because Postgres writes were too chatty | write paths were converted to chunked set-based inserts and prepared command reuse | Yes |
| 13 | Postgres still relied on a SQLite mirror on the read path | direct native Postgres reads replaced the SQLite mirror for the interactive read path | Yes |
| 14 | Remaining structural mutations still used the compatibility path | add row, delete row, ensure year, and delete year were routed to native Postgres | Yes |
| 15 | Backend auth was disabled in UAT | Entra scope-based API auth was restored and validated with protected-route checks | Yes |
| 16 | Lambda remained the bottleneck for interactive planning | the interactive API was moved to ECS Fargate behind CloudFront and ALB | Yes |
| 17 | ALB could be bypassed directly | ingress was restricted to CloudFront and a secret origin header was enforced | Yes |
| 18 | RDS was public | a private-subnet replacement RDS instance was restored and cut over | Yes |
| 19 | CloudFront lacked WAF protection | WAF was attached to the CloudFront distribution | Yes |
| 20 | Startup could hang on live planning-slice load | startup scope loading was simplified and undo/redo loading was delayed until a slice exists | Yes |
| 21 | ECS startup stalled on runtime secret resolution | the stack was corrected to avoid ECS secret injection, and the next revision uses direct DB env values for startup | In progress in deployment |
| 22 | Import and export were synchronous and could block the UI | async job APIs with progress reporting were added | Yes |
| 23 | Async jobs only lived in memory inside the ECS task | async job payloads, status, progress, outputs, and retention were moved into PostgreSQL | Yes |
| 24 | Reconciliation was only partly operationalized | durable reconciliation jobs, schedules, and report persistence were added | Yes |
| 25 | ASP.NET Data Protection keys were container-local | Data Protection key storage was moved into PostgreSQL | Yes |
| 26 | ECS rollout policy allowed unsafe churn and drift | circuit breaker rollback and `MinimumHealthyPercent=100` were added to the stack template | Yes |
| 27 | CloudFormation and ECS drift accumulated during failed rollouts | controlled rollback and service normalization steps were added; the current corrected stack rollout is still being finalized | In progress in deployment |
| 28 | Department/server-side composition and SSRM were not yet implemented | Department view is now server-composed and AG Grid runs on SSRM/server-side blocks | Yes |
| 29 | Reconciliation and async progress were not fully documented | runtime and endpoint docs were updated to reflect the final Phase 1 design | Yes |
| 30 | Several UAT defects risked falling through between frontend and backend fixes | regression coverage was expanded across planning, maintenance, and workbook roundtrips | Yes |

## Still Open During This Document Revision

- The corrected ECS stack deployment that combines:
  - durable PostgreSQL-backed jobs
  - durable reconciliation scheduling
  - PostgreSQL-backed Data Protection keys
  - direct DB credential startup for ECS
  is still being finalized in AWS at the time of this document update.

## Review Notes

- All user-reported defects from the issue history were either fixed in code or converted into explicit tracked deployment follow-up items.
- The remaining high-priority items are now architectural and operational rather than missing feature work:
  - stricter delta-only recalculation
  - private-subnet ECS
  - HTTPS from CloudFront to ALB
  - production-grade observability and alarms
