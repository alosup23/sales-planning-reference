# Sales Budget And Planning Training And Process Overview

## Purpose

This document summarizes the Phase 1 UAT planning process, key business considerations, the recommended sequence of work, and the main application features that users should understand before using the system.

## 1. Training Objectives

Users should be able to:

- understand the purpose of each workspace
- navigate quickly with the compact menu-based toolbar
- maintain the required master data
- execute bottom-up planning
- execute top-down splash adjustments
- use locks, undo, redo, and growth factors correctly
- import and export the supported workbook formats
- understand the planning rules and constraints

## 2. Business Planning Sequence

Recommended sequence:

1. confirm hierarchy and option values
2. load and validate store and product master data
3. load inventory, pricing, seasonality, and vendor context
4. confirm the year and period structure
5. complete bottom-up store-level planning
6. review and align top-down department targets
7. apply locks to approved values
8. export and reconcile the result

## 3. Key Planning Concepts

### 3.1 Bottom-up planning

- detailed leaf planning
- best performed in store view
- builds the plan from the lowest meaningful business level

### 3.2 Top-down planning

- aggregate target setting
- best reviewed in department view
- redistributes the target through splash while respecting constraints

### 3.3 Locks

- used to protect approved values
- excluded from top-down overwrite behavior

### 3.4 Undo and redo

- available up to `30` actions
- useful for controlled experimentation and quick correction

## 4. Application Workspaces

- `Planning - by Store`
- `Planning - by Department`
- `Hierarchy Maintenance`
- `Store Profile Maintenance`
- `Product Profile Maintenance`
- `Inventory Profile Maintenance`
- `Pricing Policy Maintenance`
- `Seasonality & Events Maintenance`
- `Vendor Supply Maintenance`

## 5. Why Both Planning Views Matter

Store view supports:

- ownership at branch level
- operational detail
- bottom-up build quality

Department view supports:

- category review
- cross-store comparison
- top-down alignment
- merchandising management

## 6. Key Planning Rules To Train

- all views use the same underlying planning facts
- parent totals must equal child totals
- splash excludes locked descendants
- same-year recalculation only
- derived measures recalculate automatically
- rounding must stay deterministic

## 7. Master Data To Maintain Before Planning

- Store Profile
- Product Profile
- Inventory Profile
- Pricing Policy
- Seasonality & Events
- Vendor Supply Profile

## 8. Phase 2 Readiness Built Into Phase 1

Phase 1 already captures the data foundations for later:

- price recommendation
- suggested selling price guidance
- markdown recommendation
- sell-through-aware forecasting
- inventory-aware forecasting and replenishment context

## 9. Current UAT Considerations

- use scoped expansion rather than unlimited full hierarchy loading
- use import templates exactly as exported
- review exception workbooks after import failures
- treat the current UAT environment as controlled but not yet the final production hardening level

## 10. Trainer Recommendations

- start with navigation and grid basics
- train store view before department view
- train bottom-up before top-down splash
- train locks before aggregate overwrite workflows
- train imports and exports after users understand the maintenance workspaces
