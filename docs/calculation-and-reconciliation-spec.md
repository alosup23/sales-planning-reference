# Calculation And Reconciliation Specification

## Core Invariants

- all planning views read from the same canonical planning facts
- store-view and department-view aggregates must reconcile even when branches are loaded lazily
- parent totals equal the sum of child totals after every committed action
- same-year recalculation only
- locked cells are never overwritten by splash or aggregate override paths
- deterministic rounding is applied consistently

## Supported Measures

- `Sold Qty`
- `ASP`
- `Unit Cost`
- `Sales Revenue`
- `Total Costs`
- `GP`
- `GP%`

## Display And Precision Rules

- display values with thousands separators
- display `ASP`, `Unit Cost`, and `GP%` with `2` decimals
- display `Sold Qty`, `Sales Revenue`, `Total Costs`, and `GP` with `0` decimals
- retain higher internal precision during calculation
- round only at persisted and displayed boundaries according to measure type
- `Sold Qty` must be persisted as whole numbers only

## Leaf Calculation Rules

- `Sales Revenue = Sold Qty * ASP`
- `Total Costs = Sold Qty * Unit Cost`
- `GP = Sales Revenue - Total Costs`
- `GP% = GP / Sales Revenue`

## Bottom-Up Edit Rules

- a leaf edit updates the edited input
- dependent derived measures are recalculated for that leaf only
- only impacted ancestor aggregates are updated
- unaffected branches must not be recalculated

Confirmed leaf rules:

- `Sales Revenue` edit preserves `ASP` and solves `Sold Qty`
- `Sold Qty` edit preserves `ASP` and `Unit Cost` and solves `Sales Revenue` and `Total Costs`
- `ASP` edit preserves `Sold Qty` and solves `Sales Revenue`
- `Unit Cost` edit preserves `Sold Qty` and solves `Total Costs`
- `Total Costs` edit preserves `Sold Qty` and solves `Unit Cost`
- `GP` edit preserves `Sold Qty`, `Total Costs`, and therefore `Unit Cost`, solves `ASP`, then derives `Sales Revenue`, `GP`, and `GP%`
- `GP%` edit preserves `Sold Qty`, `Total Costs`, and therefore `Unit Cost`, solves `ASP`, then derives `Sales Revenue`, `GP`, and `GP%`

Leaf year behavior:

- leaf year edits are annual override or allocation instructions
- preserve current monthly shape for unlocked eligible months
- when the year total is zero, allocate equally across unlocked eligible months

## Top-Down Splash Rules

- splash scope is resolved explicitly from the selected aggregate branch
- locked descendants are excluded
- weights must be deterministic
- residual rounding must be allocated deterministically and auditable
- requested totals are preserved across unlocked descendants

Confirmed measure-specific splash rules:

- `Sales Revenue` splash preserves `Total Costs` and `ASP`, allocates `Sales Revenue`, then derives `Sold Qty`, `GP`, and `GP%`
- `Sold Qty` splash preserves `ASP` and `Unit Cost`, allocates `Sold Qty`, then derives `Sales Revenue`, `Total Costs`, `GP`, and `GP%`
- `ASP` splash preserves `Sold Qty`, converts to `Sales Revenue`, then allocates `Sales Revenue`
- `Unit Cost` aggregate splash is not allowed
- `Total Costs` splash preserves `Sold Qty`, allocates `Total Costs`, then derives `Unit Cost`, `GP`, and `GP%`
- `GP` splash holds `Total Costs` constant, solves `ASP`, recalculates `Sales Revenue`, then allocates `Sales Revenue`
- `GP%` splash holds `Total Costs` constant, solves `ASP`, recalculates `Sales Revenue`, then allocates `Sales Revenue`

Impossible-edit rules:

- reject a `GP%` splash when the target scope has no positive `Total Costs`
- reject rate-driven or additive requests when every eligible target is locked
- reject invalid expressions and divide-by-zero before any persistence occurs

Weighting rules:

- use current descendant values as weights
- if all weights are zero, use equal distribution across unlocked eligible targets

Growth factor rules:

- every editable cell has `baseValue`, `growthFactor`, and `effectiveValue`
- `effectiveValue = baseValue × growthFactor`
- default `growthFactor = 1.00`
- direct numeric or expression edit sets `baseValue` and resets `growthFactor` to `1.00`
- growth-factor edits change only `growthFactor`
- save, undo, and redo must preserve both `baseValue` and `growthFactor`
- growth-factor granularity is `0.01`

## Undo / Redo Rules

- undo and redo operate on application-level actions
- compound actions are reversible as a single unit
- maximum retained depth is `30`
- undo and redo must preserve calculation invariants after replay

## Reconciliation Checks

The system must support reconciliation routines for:

- leaf vs aggregate totals
- store-first vs department-first view consistency
- workbook import counts and exception counts
- data-version integrity after cutover or bulk import

Additional reconciliation obligations:

- lock-state consistency between read and mutate paths
- branch reads must include enough descendant data to derive visible aggregate rows correctly
- growth-factor base stability after reread, undo, redo, save, and restore
- year total equality versus month sum after every year-level edit or splash
- branch-scoped splash containment so no month or year outside scope changes
