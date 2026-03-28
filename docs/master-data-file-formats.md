# Master Data File Formats

## Purpose

This document defines the canonical import and export file formats for Phase 1, including the additional data foundations required for Phase 2 AI and expert merchandising capabilities.

Rules:

- Existing file formats remain compatible.
- New Phase 2-ready fields added to existing files are optional on import if absent.
- Export must include the additional fields.
- New master-data files introduced here must support import and export in the same format.

## 1. Branch Profile.xlsx

Natural key:

- `CompCode`

Existing and required business mapping:

- `CompCode` -> `storeCode`
- `BranchName` -> `branchName`
- `Region` -> `regionLabel`
- `Branch Type` -> `clusterLabel`

Additional optional fields to add:

- `Store Cluster Role`
- `Store Capacity SqFt`
- `Store Format Tier`
- `Catchment Type`
- `Demographic Segment`
- `Climate Zone`
- `Fulfilment Enabled`
- `Online Fulfilment Node`
- `Store Opening Season`
- `Store Closure Date`
- `Refurbishment Date`
- `Store Priority`

## 2. Product Profile.xlsx

### Sheet1

Natural key:

- `Product Code`

Hierarchy mapping:

- `Department`
- `Class`
- `Subclass`

Additional optional fields to add:

- `Brand`
- `Supplier`
- `Lifecycle Stage`
- `Age Stage`
- `Gender Target`
- `Material`
- `Pack Size`
- `Size Range`
- `Colour Family`
- `KVI Flag`
- `Markdown Eligible`
- `Markdown Floor Price`
- `Minimum Margin Pct`
- `Price Ladder Group`
- `Good Better Best Tier`
- `Season Code`
- `Event Code`
- `Launch Date`
- `End Of Life Date`
- `Substitute Group`
- `Companion Group`
- `Replenishment Type`
- `Lead Time Days`
- `MOQ`
- `Case Pack`
- `Starting Inventory`
- `Projected Stock On Hand`
- `Sell Through Target Pct`
- `Weeks Of Cover Target`

### Sheet2

Sheet2 remains the source for:

- Department / Class / Subclass hierarchy
- controlled option values where applicable

## 3. Inventory Profile.xlsx

Natural key:

- `CompCode`
- `Product Code`

Columns:

- `CompCode`
- `Product Code`
- `Starting Inventory`
- `Inbound Qty`
- `Reserved Qty`
- `Projected Stock On Hand`
- `Safety Stock`
- `Weeks Of Cover Target`
- `Sell Through Target Pct`
- `Status`

## 4. Pricing Policy.xlsx

Natural key:

- policy row identifier or business composite key

Columns:

- `Department`
- `Class`
- `Subclass`
- `Brand`
- `Price Ladder Group`
- `Min Price`
- `Max Price`
- `Markdown Floor Price`
- `Minimum Margin Pct`
- `KVI Flag`
- `Markdown Eligible`
- `Status`

## 5. Seasonality & Events.xlsx

Natural key:

- `Department`
- `Class`
- `Subclass`
- `Season Code`
- `Event Code`
- `Month`

Columns:

- `Department`
- `Class`
- `Subclass`
- `Season Code`
- `Event Code`
- `Month`
- `Weight`
- `Promo Window`
- `Peak Flag`
- `Status`

## 6. Vendor Supply Profile.xlsx

Natural key:

- `Supplier`
- `Brand`

Columns:

- `Supplier`
- `Brand`
- `Lead Time Days`
- `MOQ`
- `Case Pack`
- `Replenishment Type`
- `Payment Terms`
- `Status`
