# Sales Budget & Planning User Guide

## 1. Purpose

This guide explains how to use the Phase 1 UAT Sales Budget & Planning application, including the recommended planning sequence, all major user interface areas, master-data maintenance, import and export, and the planning rules that govern bottom-up aggregation and top-down splashing.

## 2. Intended Users

- planners
- merchandising managers
- finance users supporting the budget cycle
- master-data administrators
- UAT reviewers and trainers

## 3. Access And Sign-In

1. Open the UAT web application.
2. Sign in with the approved Microsoft 365 work account.
3. Wait for the initial planning scope to load.
4. Use the toolbar menu to move between planning and maintenance workspaces.

## 4. User Interface Overview

## 4.1 Header and toolbar

The application uses a compact menu-first toolbar to maximize the planning grid area.

Main elements:

- application title and context banner
- workspace selector menu
- contextual controls for the active workspace
- user profile menu
- live status or save message area

## 4.2 Workspace menu

The main workspace menu currently includes:

- `Planning - by Store`
- `Planning - by Department`
- `Hierarchy Maintenance`
- `Store Profile Maintenance`
- `Product Profile Maintenance`
- `Inventory Profile Maintenance`
- `Pricing Policy Maintenance`
- `Seasonality & Events Maintenance`
- `Vendor Supply Maintenance`

## 4.3 Planning grid

The planning grid is the main interactive area.

Key behavior:

- grouped fiscal columns
- hierarchy rows with expand and collapse
- bold aggregate row headers and aggregate values
- retained grid color assignments
- compact but readable buttons and controls
- responsive layout for smaller screens

## 5. Best-Practice Sales Budget And Planning Flow

Use this sequence for a controlled planning cycle.

### Step 1: Prepare hierarchy and option values

- confirm `Department`, `Class`, and `Subclass` structures in `Hierarchy Maintenance`
- confirm option lists needed by the maintenance workspaces

### Step 2: Maintain core master data

Prepare and validate:

- `Store Profile`
- `Product Profile`
- `Inventory Profile`
- `Pricing Policy`
- `Seasonality & Events`
- `Vendor Supply Profile`

### Step 3: Import master data

Import the approved workbook files in this order when possible:

1. `Branch Profile.xlsx`
2. `Product Profile.xlsx`
3. `Inventory Profile.xlsx`
4. `Pricing Policy.xlsx`
5. `Seasonality & Events.xlsx`
6. `Vendor Supply Profile.xlsx`

### Step 4: Confirm the planning year structure

- generate the next year if required
- verify periods are available before editing

### Step 5: Build the plan bottom-up in `Planning - by Store`

Use store view when store teams or planners are working at detailed leaf level.

Recommended use:

- expand only the branches being planned
- enter leaf-level values for:
  - `Sold Qty`
  - `ASP`
  - `Unit Cost`
- review the automatically derived values:
  - `Sales Revenue`
  - `Total Costs`
  - `GP`
  - `GP%`

### Step 6: Review the plan top-down in `Planning - by Department`

Use department view when a cross-store merchandising or finance review is needed.

Recommended use:

- compare the department totals across stores
- use the appropriate department layout
- apply top-down changes where the aggregate target must be redistributed

### Step 7: Apply top-down splash carefully

Use splash when the business target is known at an aggregate level.

Recommended use:

- confirm the correct branch is selected
- confirm locked rows are intentional before splashing
- use the splash result to realign lower levels while preserving the aggregate target

### Step 8: Lock approved values

Lock branches or rows once they are approved to avoid accidental overwrite.

### Step 9: Use undo / redo where needed

- undo and redo are available for up to `30` actions
- use them to reverse recent edits, splash actions, or lock operations when appropriate

### Step 10: Export and review

- export planning workbooks or master-data files
- review totals, exceptions, and audit output
- complete sign-off or the next approval step outside the Phase 1 system if required

## 6. Planning Views

## 6.1 Planning - by Store

Hierarchy:

- `Store -> Department -> Class -> Subclass`

Best use:

- detailed bottom-up planning
- store-level review
- operational branch comparisons

## 6.2 Planning - by Department

Supported layouts:

- `Department -> Store -> Class -> Subclass`
- `Department -> Class -> Store -> Subclass`

Best use:

- top-down merchandising review
- department-level budget alignment
- cross-store comparison within a department

## 7. Expand And Collapse Behavior

- planning hierarchies start collapsed at the higher level
- users expand only the branches they need
- store view follows the store-first hierarchy
- department view follows the currently selected department layout
- department expansion is not limited to a single selected store when `All Stores` is in effect

## 8. Bottom-Up Aggregation Rules

- leaf edits are entered at the most detailed editable row
- parent totals must equal the sum of children after recalculation
- dependent measures are recalculated from the edited inputs
- same-year rules apply
- unaffected branches should not be manually changed unless intentionally edited or splashed

## 9. Top-Down Splashing Rules

- splash starts from the selected aggregate branch
- locked descendants are excluded
- weights are applied deterministically
- residual rounding is deterministic and auditable
- the requested aggregate target is preserved across the unlocked descendants

## 10. Constraints Imposed By The Planning Engine

- same-year recalculation only
- locked cells cannot be overwritten by splash
- all views are projections over the same planning facts
- undo and redo preserve the planning invariants
- rounding must remain deterministic

## 11. Measures

Editable base measures:

- `Sold Qty`
- `ASP`
- `Unit Cost`

Derived measures:

- `Sales Revenue = Sold Qty * ASP`
- `Total Costs = Sold Qty * Unit Cost`
- `GP = Sales Revenue - Total Costs`
- `GP% = GP / Sales Revenue`

## 12. Undo And Redo

- maximum retained depth:
  - `30`
- supported action types include:
  - leaf edits
  - splash actions
  - lock and unlock
  - supported reversible structural actions

## 13. Growth Factors

Growth factor actions apply planning uplift or reduction logic against the selected period scope.

Best practice:

- confirm the correct year or branch is selected
- review seasonality before applying broad growth changes
- use department review after a large growth-factor change

## 14. Audit And Save

- audit records capture the action history
- save actions persist the current planning state
- after major changes, review totals before continuing

## 15. Maintenance Workspaces

## 15.1 Hierarchy Maintenance

Use to maintain:

- Departments
- Classes
- Subclasses

## 15.2 Store Profile Maintenance

Use for:

- store CRUD
- inactivation
- Branch Profile import and export

## 15.3 Product Profile Maintenance

Use for:

- product CRUD
- product hierarchy maintenance
- Product Profile import and export

## 15.4 Inventory Profile Maintenance

Use for:

- inventory context data
- sell-through target context
- stock-on-hand planning inputs

## 15.5 Pricing Policy Maintenance

Use for:

- margin rules
- markdown eligibility
- price boundaries
- price ladder policy

## 15.6 Seasonality & Events Maintenance

Use for:

- seasonal weighting inputs
- event windows
- peak period context

## 15.7 Vendor Supply Maintenance

Use for:

- supplier lead time
- MOQ
- case pack
- replenishment policy

## 16. Import And Export

## 16.1 Supported workbook formats

- `Branch Profile.xlsx`
- `Product Profile.xlsx`
- `Inventory Profile.xlsx`
- `Pricing Policy.xlsx`
- `Seasonality & Events.xlsx`
- `Vendor Supply Profile.xlsx`
- planning workbook export/import

## 16.2 Import behavior

- natural key update-or-insert behavior
- optional Phase 2-ready fields accepted when present
- exception workbook returned for invalid rows

## 16.3 Export behavior

- export includes the extended Phase 1 plus Phase 2-ready schema
- export preserves the canonical import column names and order

## 17. Recommended Daily Working Pattern

1. Check the correct year and scope.
2. Review locked branches before making major changes.
3. Make leaf edits in store view.
4. Review department totals in department view.
5. Use splash only when the business target should flow downward.
6. Save after each logical planning block.
7. Export for external review where required.

## 18. Troubleshooting

If a load looks incomplete:

- confirm the correct workspace is selected
- confirm the correct store or department scope is selected
- expand the next level of the hierarchy
- hard refresh if a newly deployed frontend bundle is expected

If an import fails:

- download the exception workbook
- review `Remark` and `Expected Value`
- correct the source workbook and retry

If a splash result is smaller than expected:

- check for locked descendants
- check whether the selected branch was correct

## 19. Current UAT Limitations To Be Aware Of

- import and export are still synchronous operations
- some large full-hierarchy requests are intentionally constrained
- CloudFront-to-ALB origin traffic is not yet HTTPS-only
- some performance optimizations planned for production are not yet complete

For the live technical limitation register, see:

- [docs/current-limitations-and-recommendations.md](/Users/aloysius/Documents/New%20project/docs/current-limitations-and-recommendations.md)
