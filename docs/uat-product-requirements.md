# Sales Budget & Planning UAT Product Requirements

## 1. Purpose

This document defines the clean-slate UAT requirements for the Sales Budget & Planning application.

The design must:

- preserve compatibility with the existing Excel-based import and export formats
- optimize for interactive planning performance
- maximize usable planning grid space
- support Responsive Web Design (RWD)
- remain cost-aware for UAT while being production-ready in architecture
- support later migration to a hardened production environment without redesigning the planning model
- bring all Phase 2 data foundations into Phase 1 so later AI capabilities can be added without reworking the core data model

## 2. Scope

### 2.1 Phase 1 UAT scope

The Phase 1 UAT release includes:

- interactive planning by `Store` and by `Department`
- store profile maintenance
- product profile maintenance
- inventory profile maintenance
- pricing policy maintenance
- seasonality and event maintenance
- vendor and supply profile maintenance
- hierarchy maintenance
- workbook import and export
- controlled option-value maintenance
- Microsoft 365 sign-in on the web application
- auditable planning edits, lock/unlock actions, and top-down splash actions
- deterministic undo and redo for up to `30` user actions
- all data structures required for Phase 2 merchandising intelligence

### 2.2 Phase 1 exclusions

Phase 1 excludes:

- workflow approvals
- multi-user real-time collaboration
- production-grade multi-region disaster recovery
- live AI recommendation generation

### 2.3 Phase 2 scope boundary

Phase 2 will add:

- expert merchandising recommendations
- pricing recommendations
- suggested selling prices
- markdown recommendations
- forecasting guidance using actual sales, sold quantities, sell-through, starting inventory, and projected stock on hand

Phase 2 must reuse the Phase 1 data foundations and must not require redesign of the transactional planning core.

## 3. Business Data Sources

### 3.1 Canonical admin import formats

The system must use the following workbook formats as the canonical admin import and export contracts.

- `Branch Profile.xlsx`
  - Store Profile import and export
- `Product Profile.xlsx`
  - `Sheet1` for Product Profile import and export
  - `Sheet2` for Department / Class / Subclass hierarchy and controlled option values
- `Inventory Profile.xlsx`
  - inventory and stock context import and export
- `Pricing Policy.xlsx`
  - pricing, margin, markdown, and ladder policy import and export
- `Seasonality & Events.xlsx`
  - seasonal, promotional, and event context import and export
- `Vendor Supply Profile.xlsx`
  - supplier and replenishment constraint import and export

### 3.2 Import / export compatibility rules

- Export files must preserve the same column ordering, naming, and semantics required by import.
- Existing workbook formats must be enhanced to carry Phase 2-ready optional fields where relevant.
- New Phase 2-ready fields added to existing files must be optional on import if absent.
- Export must include the additional Phase 2-ready fields.
- Import must:
  - update existing rows when the natural business key already exists
  - insert new rows when the natural business key does not exist
- Import must ignore system-added operational columns when present, including:
  - `Remark`
  - `Expected Value`
- Import exceptions must produce a workbook with:
  - the original row data
  - `Remark`
  - `Expected Value`

## 4. Functional Requirements

### 4.1 Planning views

The application must provide:

- `Planning - by Store`
  - hierarchy: `Store -> Department -> Class -> Subclass`
- `Planning - by Department`
  - layout A: `Department -> Store -> Class -> Subclass`
  - layout B: `Department -> Class -> Store -> Subclass`

Both views must be projections over the same canonical planning dataset.

### 4.2 Planning behavior

The planning engine must support:

- bottom-up leaf editing
- top-down aggregate editing through splash allocation
- lock / unlock
- same-year recalculation only
- recalculation of derived measures after edit
- recalculation of ancestors after leaf or splash updates
- audit trail capture for all write actions
- deterministic undo and redo for up to `30` user actions
- calculation accuracy across all supported hierarchical projections

Supported business measure behavior:

- `Sold Qty`
- `ASP`
- `Unit Cost`
- `Sales Revenue`
- `Total Costs`
- `GP`
- `GP%`

Derived measure rules must remain:

- `Sales Revenue = Sold Qty * ASP`
- `Total Costs = Sold Qty * Unit Cost`
- `GP = Sales Revenue - Total Costs`
- `GP% = GP / Sales Revenue`

The planning engine must preserve these invariants:

- all views are projections over one canonical planning fact model
- parent totals always equal the sum of child totals after bottom-up edits
- top-down splash updates must preserve requested totals subject to lock constraints
- rounding residuals must be deterministic and auditable
- recalculation must stay within the same fiscal year unless a future approved cross-year rule is introduced

### 4.3 Store Profile maintenance

Store Profile maintenance must support:

- Add
- Update
- Delete
- Inactivate
- Import
- Export

The Store Profile model must use the current Branch Profile structure and existing planning semantics:

- `CompCode` is the store business key
- `Region` maps to `regionLabel`
- `Branch Type` maps to `clusterLabel`
- no duplicate parallel `region` or `cluster` fields should be introduced

Phase 2 data-foundation fields must be added to the Store Profile format where relevant and treated as optional on import if absent.

### 4.4 Product Profile maintenance

Product Profile maintenance must support:

- Add
- Update
- Delete
- Inactivate
- Import
- Export

The Product Profile model must:

- use the current `Product Profile.xlsx` `Sheet1` format as the canonical import/export contract
- map `Department`, `Class`, and `Subclass` fields directly to the planning hierarchy
- use `Sheet2` as the source of hierarchy structure and controlled option values
- include additional merchandising, pricing, inventory, lifecycle, and policy attributes needed for future AI recommendations
- treat newly added Phase 2 data-foundation fields as optional on import if absent

### 4.5 Additional master-data maintenance

The application must provide full CRUD, import, export, and inactivation support where appropriate for:

- Inventory Profile
- Pricing Policy
- Seasonality & Events
- Vendor Supply Profile

Best-practice behavior:

- server-side paging, filtering, and search
- contextual action menus
- exception workbook generation for failed imports
- soft inactivation preferred over destructive delete for business master data

### 4.6 Hierarchy maintenance

The application must support controlled maintenance of:

- Department
- Class
- Subclass

Hierarchy rules:

- each Class belongs to one Department
- each Subclass belongs to one Class
- the hierarchy used by planning must be consistent with imported Product Profile hierarchy data

### 4.7 Option-value maintenance

The application must support controlled maintenance of enumerated or option-backed fields used by:

- Store Profile
- Product Profile
- Inventory Profile
- Pricing Policy
- Seasonality & Events
- Vendor Supply Profile
- hierarchy structures where applicable

### 4.8 Undo / redo

The application must support:

- undo of up to `30` user actions
- redo of up to `30` user actions where not invalidated by subsequent changes
- compound actions, including:
  - leaf edits
  - splash actions
  - lock and unlock
  - reversible structural actions where supported

Undo and redo must be application-level and server-authoritative.

## 5. UI / UX Requirements

### 5.1 Navigation

The application must use a menu-based navigation pattern to minimize toolbar waste and maximize planning space.

Required design behavior:

- a compact global application menu
- contextual actions grouped by screen or active entity
- no oversized toolbar buttons
- primary planning grid receives maximum available space

### 5.2 Responsive Web Design

The UI must follow RWD principles:

- desktop-first optimization for planning use
- tablet support without horizontal control overflow
- responsive reflow for narrow screens
- menus and dialogs remain usable on smaller viewports
- no overlapping fixed controls

### 5.3 Grid presentation

The planning grid must:

- prioritize visible data density without reducing readability
- use reasonable font sizes for fast scanning
- right-align all numeric cell values
- keep current grid color assignment
- display all aggregate row headers in bold
- display all aggregate cell values in bold
- keep leaf rows visually distinct from aggregates

Aggregate row styling requirements:

- current approved color scheme remains in place
- level 0 and level 1 aggregate rows remain bold
- all aggregate rows use the approved year-based banding scheme

### 5.4 Buttons and controls

All buttons must:

- be just large enough to comfortably read the text
- not consume unnecessary screen space
- remain touch-usable on tablet-class devices

### 5.5 Grid interaction and usability

Required behavior:

- selecting a top-level store changes store scope automatically
- selecting a top-level department changes department scope automatically
- scope changes should feel immediate
- row expansion must be explicit and reliable from the caret
- expanded state should be preserved where practical during data refresh
- visible totals should load quickly
- the grid should minimize disruptive reflows after edits, lock changes, or splash actions

## 6. Performance Requirements

### 6.1 User experience targets

UAT target service levels:

- app shell usable in under `2 seconds` on a warm path
- initial planning view usable in under `3 seconds`
- store or department scope switch in under `1 second`
- branch expansion in under `700 ms`
- leaf edit visible with recalculated values in under `500 ms`
- top-down splash response in under `1.5 seconds` for a normal branch

### 6.2 Loading model

The system must not hydrate the entire planning universe up front.

Startup hydration must be limited to:

- user session context
- menu/navigation metadata
- scope lists
- selected scope root rows
- visible year totals

Additional data must load on demand:

- classes
- subclasses
- month detail
- non-visible branches
- large maintenance datasets

### 6.3 Payload rules

The design must optimize payload size.

Guidelines:

- initial payload under `500 KB` compressed where practical
- first planning slice under `1.5 MB` compressed
- branch expansion payload under `500 KB` compressed
- edit responses should return targeted patches, not full grid reloads
- maintenance lists must page and filter server-side instead of hydrating full tables

### 6.4 Recalculation rules

The system must not perform full-scenario recalculation for a single leaf change.

Instead:

- update the edited leaf row
- recompute only dependent derived measures for that leaf
- recompute only impacted ancestor aggregates in the same year and branch scope
- return only the changed cells and impacted aggregates

### 6.5 AI data readiness in Phase 1

Phase 1 must capture the data foundations needed for later expert merchandising recommendations, including:

- lifecycle stage
- age stage
- brand
- supplier
- price ladder group
- markdown policy
- minimum margin policy
- season and event context
- starting inventory
- projected stock on hand
- sell-through targets
- weeks of cover targets
- replenishment and lead-time constraints

## 7. Security Requirements For UAT

The UAT solution must be designed to harden cleanly into production.

Required UAT security baseline:

- Microsoft 365 sign-in on the web app
- private database access
- TLS for public endpoints
- audit logging for write actions
- no destructive test endpoints in UAT
- restricted CORS
- non-public data persistence

Production-ready design foundations required in UAT:

- backend authorization seam
- security-group based east-west access
- secret-based database credentials
- separation of admin operations from request-time startup
- authorization seams for future planner vs admin separation

## 8. Environment Requirements

### 8.1 UAT

The UAT environment must:

- optimize for low cost where reasonable
- use free-tier or near-free-tier services where possible without materially harming interactive performance
- remain structurally compatible with production migration

### 8.2 Production readiness

The production-ready design must support:

- scaling out the interactive API
- larger product/store datasets
- proper backend authorization
- cache introduction
- async job workers
- stronger observability and security controls

## 9. Data Seeding And Admin Operations

The system must not reseed automatically during runtime startup.

Rules:

- Product Profile and Store Profile imports are managed admin operations
- Inventory Profile, Pricing Policy, Seasonality & Events, and Vendor Supply Profile imports are managed admin operations
- seed and migration state must be tracked explicitly
- runtime startup must be read-only with respect to master data and planning seed state

## 10. Acceptance Criteria

The UAT release is acceptable when:

- current Excel import/export formats work without manual restructuring
- planning views are responsive under seeded UAT data volumes
- a single edit does not trigger a full-grid reload
- store and department scope switching is immediate and intuitive
- aggregate rows remain bold and readable
- the planning grid occupies the majority of the available screen
- all required Phase 2 data-foundation fields can be maintained in Phase 1
- the system is ready for AWS production hardening without redesigning the data or API model

## 11. Supporting Specifications

Detailed support documents:

- [master-data-file-formats.md](/Users/aloysius/Documents/New%20project/docs/master-data-file-formats.md)
- [calculation-and-reconciliation-spec.md](/Users/aloysius/Documents/New%20project/docs/calculation-and-reconciliation-spec.md)
- [non-functional-requirements.md](/Users/aloysius/Documents/New%20project/docs/non-functional-requirements.md)
- [ai-phase-2-merchandising-architecture.md](/Users/aloysius/Documents/New%20project/docs/ai-phase-2-merchandising-architecture.md)
- [phase-roadmap-and-backlog.md](/Users/aloysius/Documents/New%20project/docs/phase-roadmap-and-backlog.md)
