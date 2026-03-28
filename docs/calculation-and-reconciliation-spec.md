# Calculation And Reconciliation Specification

## Core Invariants

- all planning views read from the same canonical planning facts
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

## Top-Down Splash Rules

- splash scope is resolved explicitly from the selected aggregate branch
- locked descendants are excluded
- weights must be deterministic
- residual rounding must be allocated deterministically and auditable
- requested totals are preserved across unlocked descendants

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
