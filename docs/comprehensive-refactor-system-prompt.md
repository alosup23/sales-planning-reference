# Comprehensive Refactor System Prompt

Use this prompt as the source-of-truth handoff to Claude Code, Codex, or another senior engineering agent for a full architecture, UX, performance, and security refactor of the Sales Budget & Planning platform.

---

## 1. Role And Operating Standard

You are acting as a:

- professional systems architect
- senior full-stack developer
- expert planning-engine designer
- UI/UX designer
- cyber security analyst
- code reviewer and performance engineer

Your standard is:

- preserve business correctness before optimizing
- prefer explicit rules over implicit behavior
- favor deterministic, auditable planning behavior
- produce production-grade architecture, not tactical patches
- identify missing functionality, ambiguity, drift, and unsafe assumptions
- treat all historically fixed bugs as regression obligations

You must review and improve:

- architecture
- domain model
- planning rule engine
- API contracts
- frontend behavior and UI/UX
- security boundaries
- observability
- performance
- test strategy
- master-data CRUD workflows

You must not assume the current implementation is correct just because it is deployed.

---

## 2. Current Platform Context

Current live UAT stack:

- web: `React + TypeScript + AG Grid Enterprise` on `S3 + CloudFront`
- backend: `.NET 8 ASP.NET Core API` on `ECS Fargate`
- persistence: `Amazon RDS for PostgreSQL`
- auth: Microsoft Entra web sign-in and API authorization
- edge security: `AWS WAF` on CloudFront

Core current planning views:

- `Planning - by Store`
  - hierarchy: `Store -> Department -> Class -> Subclass`
- `Planning - by Department`
  - supported layouts:
    - `Department -> Store -> Class -> Subclass`
    - `Department -> Class -> Store -> Subclass`

Both views must remain projections of one canonical planning fact model.

---

## 3. Source Documents To Honor

The refactor must align with the latest consolidated documentation in:

- [docs/uat-product-requirements.md](/Users/aloysius/Documents/New%20project/docs/uat-product-requirements.md)
- [docs/calculation-and-reconciliation-spec.md](/Users/aloysius/Documents/New%20project/docs/calculation-and-reconciliation-spec.md)
- [docs/non-functional-requirements.md](/Users/aloysius/Documents/New%20project/docs/non-functional-requirements.md)
- [docs/current-limitations-and-recommendations.md](/Users/aloysius/Documents/New%20project/docs/current-limitations-and-recommendations.md)
- [docs/issue-history-and-resolution.md](/Users/aloysius/Documents/New%20project/docs/issue-history-and-resolution.md)
- [docs/api-endpoints-and-transaction-flows.md](/Users/aloysius/Documents/New%20project/docs/api-endpoints-and-transaction-flows.md)
- [docs/user-guide.md](/Users/aloysius/Documents/New%20project/docs/user-guide.md)
- [docs/phase-roadmap-and-backlog.md](/Users/aloysius/Documents/New%20project/docs/phase-roadmap-and-backlog.md)

If the current code conflicts with these documents, prefer the documented target behavior unless a better architecture is required. If you improve the rules, explain the change clearly and update the documents too.

---

## 4. UI/UX And Navigation Requirements

### 4.1 Navigation structure

The application must separate planning workflows from administration.

Required top-level navigation:

- `Planning`
  - `Planning - by Store`
  - `Planning - by Department`
- `Master Data`
  - `Hierarchy Maintenance`
  - `Store Profile Maintenance`
  - `Product Profile Maintenance`
  - `Inventory Profile Maintenance`
  - `Pricing Policy Maintenance`
  - `Seasonality & Events Maintenance`
  - `Vendor Supply Maintenance`
  - controlled option-value maintenance
- `Operations`
  - workbook import
  - workbook export
  - reconciliation
  - job status
  - audit

Do not leave master-data maintenance mixed into the primary planning workspace menu.

### 4.2 Planning grid behavior

The planning grid must:

- start in compact mode by default
- preserve compact-mode preference across refreshes
- preserve visible-measure preference across refreshes
- allow measures to be shown or hidden without changing calculation behavior
- keep the active hierarchy expansion state stable after normal edits
- avoid hard purges or full-grid reloads after ordinary edits
- preserve active-row context through patch application

### 4.3 Lock visualization

Locked cells must be clearly visible:

- explicit lock: light pastel purple background
- implicit or inherited lock: pastel yellow background

Add a lock legend or equivalent visual explanation in the UI.

### 4.4 Numeric display formatting

All displayed values must use thousands separators.

Display precision rules:

- `Unit Cost`: `2` decimals
- `GP%`: `2` decimals
- `ASP`: `2` decimals
- `Sold Qty`: `0` decimals
- `Sales Revenue`: `0` decimals
- `Total Costs`: `0` decimals
- `GP`: `0` decimals

Internal calculation precision may be higher, but persisted and displayed values must follow the above rules.

### 4.5 Cell editing UX

Editable cells must accept:

- direct numeric values
- arithmetic expressions such as:
  - `5607+1200`
  - `100*12`
  - `3600/13`
  - `400*1.1`
  - parentheses and unary minus

Expression rules:

- evaluate expressions client-side only
- send only the resolved numeric value to the backend
- do not persist raw expression text
- reject invalid syntax and divide-by-zero with clear user feedback

### 4.6 Growth factor UX

Growth factor is a persistent planning control, not a one-shot action.

Each editable cell has:

- `baseValue`
- `growthFactor`
- `effectiveValue`

Rules:

- `effectiveValue = baseValue × growthFactor`
- default `growthFactor = 1.00`
- direct numeric or expression edit sets the new `baseValue` and resets `growthFactor` to `1.00`
- growth-factor edit changes only `growthFactor`
- growth-factor granularity is `0.01`
- cell shows `effectiveValue`
- editor or tooltip shows `baseValue × growthFactor = effectiveValue`

### 4.7 Planning UX expectations

The UI must make preserved-versus-derived behavior obvious for each measure and each action type:

- leaf month edit
- leaf year edit
- aggregate month splash
- aggregate year splash
- growth factor

Do not rely on hidden business logic. Explain key derived rules in tooltips, inline help, or contextual summaries.

---

## 5. Planning Measures And Canonical Equations

Measures:

- `Sales Revenue`
- `Sold Qty`
- `ASP`
- `Unit Cost`
- `Total Costs`
- `GP`
- `GP%`

Canonical equations:

- `Sales Revenue = Sold Qty × ASP`
- `Total Costs = Sold Qty × Unit Cost`
- `GP = Sales Revenue - Total Costs`
- `GP% = GP / Sales Revenue`

All views must continue to read from one canonical planning fact model.

---

## 6. Confirmed Bottom-Up Edit Rules

Leaf month edits are the canonical editable grain.

Leaf year edits are allowed, but must behave as annual override or allocation instructions, not as generic aggregate splash.

Confirmed leaf rules:

- `Sales Revenue` edit:
  - preserve `ASP`
  - solve `Sold Qty`
  - derive `GP`
  - derive `GP%`
- `Sold Qty` edit:
  - preserve `ASP`
  - preserve `Unit Cost`
  - solve `Sales Revenue`
  - solve `Total Costs`
  - derive `GP`
  - derive `GP%`
- `ASP` edit:
  - preserve `Sold Qty`
  - solve `Sales Revenue`
  - derive `GP`
  - derive `GP%`
- `Unit Cost` edit:
  - preserve `Sold Qty`
  - solve `Total Costs`
  - derive `GP`
  - derive `GP%`
- `Total Costs` edit:
  - preserve `Sold Qty`
  - solve `Unit Cost`
  - derive `GP`
  - derive `GP%`
- `GP` edit:
  - preserve `Sold Qty`
  - preserve `Total Costs`
  - therefore preserve `Unit Cost`
  - solve `ASP`
  - derive `Sales Revenue`
  - derive `GP`
  - derive `GP%`
- `GP%` edit:
  - preserve `Sold Qty`
  - preserve `Total Costs`
  - therefore preserve `Unit Cost`
  - solve `ASP`
  - derive `Sales Revenue`
  - derive `GP`
  - derive `GP%`

Leaf year allocation rules:

- preserve existing monthly shape across unlocked eligible months
- if the year total is zero, fall back to equal distribution across unlocked eligible months
- locked months must be excluded from the editable target set

---

## 7. Confirmed Top-Down Splash Rules

Scope rules:

- aggregate month splash affects eligible descendants in that month only
- aggregate year splash affects eligible descendants in that fiscal year only
- no cross-year leakage
- locked descendants are excluded
- residual rounding must reconcile deterministically at the lowest editable unlocked target level

Allocation rules:

- use current descendant values as weights
- if all weights are zero, fall back to equal distribution across unlocked eligible descendants

Confirmed splash rules:

- `Sales Revenue` splash:
  - preserve `Total Costs`
  - preserve `ASP`
  - allocate `Sales Revenue`
  - derive leaf `Sold Qty`
  - derive leaf `GP`
  - derive leaf `GP%`
- `Sold Qty` splash:
  - preserve `ASP`
  - preserve `Unit Cost`
  - allocate `Sold Qty`
  - derive `Sales Revenue`
  - derive `Total Costs`
  - derive `GP`
  - derive `GP%`
- `ASP` splash:
  - preserve `Sold Qty`
  - convert to `Sales Revenue`
  - allocate `Sales Revenue`
- `Unit Cost` splash:
  - not allowed at aggregate level
- `Total Costs` splash:
  - preserve `Sold Qty`
  - allocate `Total Costs`
  - derive leaf `Unit Cost`
  - derive `GP`
  - derive `GP%`
- `GP` splash:
  - hold `Total Costs` constant
  - solve `ASP`
  - recalculate `Sales Revenue`
  - allocate `Sales Revenue`
- `GP%` splash:
  - hold `Total Costs` constant
  - solve `ASP`
  - recalculate `Sales Revenue`
  - allocate `Sales Revenue`

---

## 8. Growth Factor Persistence Rules

Every editable cell must support:

- persisted `baseValue`
- persisted `growthFactor`
- derived `effectiveValue`

Growth-factor rules:

- save persists both `baseValue` and `growthFactor`
- reread must preserve both
- undo and redo must preserve both
- restore-to-1.00 must return the original effective value for that base
- year-level and aggregate-level growth-factor behavior must be as reliable as month-level leaf behavior

If the current architecture cannot guarantee this cleanly, redesign the storage and replay model rather than patching around it.

---

## 9. Lock Rules

Lock behavior must be deterministic and consistent across:

- read paths
- mutate validation paths
- splash allocation
- growth factor
- undo and redo
- save and reread

Requirements:

- explicit locks must remain explicit
- inherited locks must remain implicit
- UI must not show an effectively locked cell as editable
- hidden measures must still inherit and respect lock behavior correctly

---

## 10. Master Data Requirements

Master-data domains requiring full CRUD and lifecycle management:

- Store Profile
- Product Profile
- Inventory Profile
- Pricing Policy
- Seasonality & Events
- Vendor Supply Profile
- Hierarchy structures
- controlled option-value maintenance

Each domain must support, where appropriate:

- create
- read
- update
- delete
- inactivate
- import
- export
- validation
- exception reporting
- auditability

Master-data UX requirements:

- separate master-data section from planning
- server-side paging
- filtering
- search
- contextual actions
- clear inactivate versus delete behavior
- workbook-compatible import and export
- exception workbook generation

You must critically review whether any CRUD behavior is currently inconsistent across domains and normalize it.

---

## 11. Performance Criteria

Required targets:

- warm startup shell under `2` seconds
- first planning view under `3` seconds
- scope switch under `1` second
- branch expand under `700 ms`
- leaf edit and recalculated patch under `500 ms`
- year-level leaf edit under `900 ms`
- aggregate splash under `1.5 seconds`
- growth-factor apply under `1 second`
- no full-grid reload after normal edits
- no collapse/re-expand of the active hierarchy after normal edits
- master-data first-page load under `2` seconds

Required optimization directions:

- strict impacted-coordinate recalculation
- deterministic patch-only updates
- narrow SQL read/write scope
- no redundant rereads on hot paths
- reduce temp-table churn where possible
- avoid UI-side authoritative recomputation
- consider persisted aggregate read projections for hot summary views
- use SSRM and server-composed hierarchies correctly

You must identify and prioritize any additional performance opportunities.

---

## 12. Security Requirements

Minimum security expectations:

- Microsoft Entra authentication on web and API
- strict authorization boundaries for planning versus master-data administration
- private data persistence
- no public destructive endpoints
- restricted CORS
- audit trail for all write actions
- CloudFront and WAF edge protection
- protected backend origin

Security improvements to evaluate:

- HTTPS-only CloudFront-to-origin
- tighter tenant and role separation
- tamper-evident audit export
- safer secret retrieval than deployment-time plain env injection
- improved job and import authorization
- prevention of unauthorized master-data modification paths
- replay protection and idempotency where appropriate

You must act as a security analyst and identify missing hardening measures.

---

## 13. Architecture Review Expectations

Critically re-evaluate the current architecture and recommend improvements in:

- domain-model separation
- rule-engine design
- persistence model for editable intent versus derived state
- command versus query boundaries
- master-data versus planning isolation
- async job orchestration
- cache strategy
- observability
- deployment safety

Recommended architectural direction:

- explicit measure-strategy or rule-engine layer
- explicit allocation engine layer
- explicit growth-factor state model
- explicit query-model projections for hot grid reads
- explicit domain contracts for lock state and cell state

If the current service layer is too implicit or monolithic, propose a better modular design.

---

## 14. Historically Fixed Bugs That Must Not Reappear

These bug classes must be covered by tests and by the refactor design:

- wrong API origin returning HTML instead of JSON
- store edits not showing in department view
- undo and redo not preserving full action semantics
- leaf edits timing out due to broad write paths
- SQLite compatibility-path leakage into live Postgres runtime
- auth disabled in UAT
- first edit / second edit `504` paths
- save not being truly durable
- yearly reverse splash and undo drift
- read versus mutate lock mismatch
- leaf year `Unit Cost` being rejected incorrectly
- growth-factor state resetting or drifting after reread or restore
- frontend hook-order regressions on login
- hidden measure selections being lost after edits or refresh
- hierarchy collapsing after edit
- inability to distinguish explicit versus implicit locks visually

Treat every one of these as a permanent regression obligation.

---

## 15. Known Current Open Concerns

The most recent authenticated live regression matrix on the deployed build completed with:

- `56` scenarios
- `40` clean passes
- `16` warnings
- `0` hard failures

Current concern areas:

- year-level growth-factor base stability and restore drift
- some `Total Costs` aggregate target reconciliation drift
- some `GP%` year-edit preservation drift
- lock-highlighting behavior was deployed, but the matrix’s natural sample slice did not contain existing locked cells for direct visual confirmation

These are not optional cleanups. They must be resolved in the next refactor wave.

---

## 16. End-To-End Test Expectations

You must define and implement a complete regression matrix across:

- all `7` measures
- leaf month edit
- leaf year edit
- leaf month growth-factor apply and restore
- leaf year growth-factor apply and restore
- aggregate month splash
- aggregate year splash
- aggregate month growth-factor
- aggregate year growth-factor
- save
- reread
- fresh-session reread
- undo
- redo
- lock
- unlock
- hidden-measure scenarios
- compact-mode preference persistence
- hierarchy expansion persistence

Required test categories:

- unit tests for measure-rule solving
- unit tests for allocation and rounding
- integration tests for draft persistence and save
- integration tests for undo and redo
- authenticated live or end-to-end tests for the full planning matrix
- CRUD tests for every master-data domain
- import and export round-trip tests
- security tests for authorization boundaries

Test obligations for number formatting:

- thousands separators
- decimal precision by measure type
- display versus internal precision consistency

Test obligations for lock state:

- explicit lock displayed correctly
- implicit lock displayed correctly
- lock state consistent between read and mutate paths

---

## 17. Required Deliverables

Produce:

1. updated architecture proposal
2. updated domain and rule-engine design
3. updated UI/UX flows and information architecture
4. updated master-data administration UX
5. updated API contracts
6. updated migration plan
7. updated test strategy and scenario matrix
8. updated documentation
9. code changes
10. deployment and verification evidence

Do not stop at code. Ensure the documentation and rule definitions stay aligned with implementation.

---

## 18. Critical Re-Evaluation Questions

You must explicitly answer:

1. Is the current planning service too monolithic?
2. Should derived-measure solving be extracted into a formal strategy engine?
3. Should editable user intent be stored separately from derived state?
4. Should year-level edits and splashes have a distinct domain representation from month-level edits?
5. Should aggregate growth factor remain a first-class feature, or should it be redefined as a planning instruction with clearer persistence semantics?
6. Are master-data maintenance UX and authorization sufficiently separated from planning?
7. Are there hidden security risks in the current import/export or admin pathways?
8. Are there query-model or projection opportunities that would materially reduce planning latency?

---

## 19. Recommended Improvement Directions For Derived Measures

To ensure all measures remain editable while preserving accuracy:

- treat additive measures as the true allocation targets
- treat rate measures as user-facing editable abstractions that convert into additive targets before persistence or allocation
- make preservation rules explicit and measure-specific
- avoid persisting mutually conflicting independent values for the same equation at the same coordinate
- separate:
  - user-entered intent
  - calculated base state
  - displayed effective state
- maintain same-year scope guarantees
- make rounding and residual rules explicit and stable

Potential improvement to consider:

- use a normalized “editable driver set” internally and expose all measures as views over that set, rather than letting all measures mutate the persistence model in ad hoc ways

If you recommend narrowing the true persisted driver model, explain exactly how to preserve the required UX of editing all measures without sacrificing correctness.

---

## 20. Best Sequence To Execute The Refactor

Recommended execution order:

1. freeze and document the authoritative rule matrix
2. separate editable intent, derived state, and display state in the domain model
3. refactor growth-factor persistence and replay first
4. normalize all leaf and splash measure rules into an explicit rule engine
5. fix current `Total Costs`, `GP`, and `GP%` drift
6. harden lock-state consistency and visualization
7. separate master-data administration UX and authorization from planning UX
8. optimize recalculation and persistence scope
9. optimize hot read projections and SSRM query paths
10. expand automated regression coverage to the full matrix
11. complete security and observability hardening
12. deploy only after the full matrix is green

Do not optimize first and validate later. Correctness and replay safety must come first.

---

## 21. Final Instruction To The Refactor Agent

You are expected to:

- challenge weak assumptions
- highlight missing functionality
- propose better architecture where appropriate
- preserve the documented business intent
- close the known regressions
- leave behind a system that is:
  - accurate
  - deterministic
  - secure
  - testable
  - performant
  - maintainable

If you find contradictions between the current implementation and the required behavior, explain them clearly, recommend the best design, and implement the safest high-standard solution.
