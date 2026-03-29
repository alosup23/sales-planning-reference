# API Endpoints And Transaction Flows

## Purpose

This document lists the current Phase 1 UAT API endpoints and describes the main transaction flows used by the application.

## 1. Endpoint Base Paths

- Public health:
  - `/health`
- Public API base through CloudFront:
  - `/api/v1`

## 2. Core Planning Query Endpoints

- `GET /api/v1/planning-store-scopes`
- `GET /api/v1/grid-slices`
- `GET /api/v1/grid-branches`
- `GET /api/v1/audit`
- `GET /api/v1/undo-redo/availability`
- `GET /api/v1/insights`

## 3. Core Planning Command Endpoints

- `POST /api/v1/cell-edits`
- `POST /api/v1/actions/splash`
- `POST /api/v1/locks`
- `POST /api/v1/actions/undo`
- `POST /api/v1/actions/redo`
- `POST /api/v1/growth-factors/apply`
- `POST /api/v1/save`
- `POST /api/v1/rows`
- `POST /api/v1/rows/delete`
- `POST /api/v1/years/generate-next`
- `POST /api/v1/years/delete`

## 4. Master-Data Query Endpoints

- `GET /api/v1/store-profiles`
- `GET /api/v1/store-profile-options`
- `GET /api/v1/product-profiles`
- `GET /api/v1/product-profile-options`
- `GET /api/v1/product-hierarchy`
- `GET /api/v1/hierarchy-mappings`
- `GET /api/v1/inventory-profiles`
- `GET /api/v1/pricing-policies`
- `GET /api/v1/seasonality-event-profiles`
- `GET /api/v1/vendor-supply-profiles`

## 5. Master-Data Command Endpoints

- `POST /api/v1/store-profiles`
- `POST /api/v1/store-profiles/delete`
- `POST /api/v1/store-profiles/inactivate`
- `POST /api/v1/store-profile-options`
- `POST /api/v1/store-profile-options/delete`
- `POST /api/v1/product-profiles`
- `POST /api/v1/product-profiles/delete`
- `POST /api/v1/product-profiles/inactivate`
- `POST /api/v1/product-profile-options`
- `POST /api/v1/product-profile-options/delete`
- `POST /api/v1/product-hierarchy`
- `POST /api/v1/product-hierarchy/delete`
- `POST /api/v1/hierarchy-mappings/departments`
- `POST /api/v1/hierarchy-mappings/classes`
- `POST /api/v1/hierarchy-mappings/subclasses`
- `POST /api/v1/inventory-profiles`
- `POST /api/v1/inventory-profiles/delete`
- `POST /api/v1/inventory-profiles/inactivate`
- `POST /api/v1/pricing-policies`
- `POST /api/v1/pricing-policies/delete`
- `POST /api/v1/pricing-policies/inactivate`
- `POST /api/v1/seasonality-event-profiles`
- `POST /api/v1/seasonality-event-profiles/delete`
- `POST /api/v1/seasonality-event-profiles/inactivate`
- `POST /api/v1/vendor-supply-profiles`
- `POST /api/v1/vendor-supply-profiles/delete`
- `POST /api/v1/vendor-supply-profiles/inactivate`

## 6. Import And Export Endpoints

### 6.1 Master-data imports

- `POST /api/v1/imports/store-profiles`
- `POST /api/v1/imports/product-profiles`
- `POST /api/v1/imports/inventory-profiles`
- `POST /api/v1/imports/pricing-policies`
- `POST /api/v1/imports/seasonality-event-profiles`
- `POST /api/v1/imports/vendor-supply-profiles`

### 6.2 Master-data exports

- `GET /api/v1/exports/store-profiles`
- `GET /api/v1/exports/product-profiles`
- `GET /api/v1/exports/inventory-profiles`
- `GET /api/v1/exports/pricing-policies`
- `GET /api/v1/exports/seasonality-event-profiles`
- `GET /api/v1/exports/vendor-supply-profiles`

### 6.3 Planning workbook import and export

- `POST /api/v1/imports/workbook`
- `GET /api/v1/exports/workbook`

## 7. Main Transaction Flows

### 7.1 Sign-in and startup

1. User signs in through Microsoft Entra.
2. Browser loads the web bundle from CloudFront.
3. App requests:
   - planning store scopes
   - active slice for the current planning view
4. Grid renders only the scoped planning data needed for the starting view.

### 7.2 Store-view branch expansion

1. User opens `Planning - by Store`.
2. App loads the scoped root slice.
3. When the user expands a store, department, or class node, the app requests `grid-branches`.
4. Returned rows are merged into the current grid in place.

### 7.3 Department-view branch expansion

1. User opens `Planning - by Department`.
2. App starts with departments collapsed.
3. Expansion follows the selected department layout:
   - `Department -> Store -> Class -> Subclass`
   - `Department -> Class -> Store -> Subclass`
4. Child rows are loaded on demand and merged into the current grid.

### 7.4 Bottom-up leaf edit

1. User edits a leaf cell.
2. App sends `POST /cell-edits`.
3. Server validates:
   - authorization
   - lock status
   - coordinate validity
4. Server recalculates dependent measures and affected aggregates.
5. Server returns changed-cell patches.
6. Grid applies the patches without reloading the whole slice.

### 7.5 Top-down splash

1. User selects an aggregate node and enters a new target.
2. App sends `POST /actions/splash`.
3. Server determines the descendant scope.
4. Locked descendants are excluded.
5. Weights are applied deterministically.
6. Residual rounding is allocated deterministically.
7. Server returns the changed-cell patches.

### 7.6 Lock / unlock

1. User locks or unlocks a branch or cell.
2. App sends `POST /locks`.
3. Server writes the lock state.
4. Grid refreshes only the planning state needed for the current slice.

### 7.7 Undo / redo

1. User clicks undo or redo.
2. App calls:
   - `POST /actions/undo`
   - or `POST /actions/redo`
3. Server replays the command journal.
4. Server returns the changed patches and updated depth state.
5. Grid applies the returned changes.

### 7.8 Import

1. User opens a maintenance workspace.
2. User selects the relevant workbook.
3. App sends the file to the matching import endpoint.
4. Server validates workbook structure and business rows.
5. If there are errors, the import returns an exception workbook.
6. If valid, the rows are inserted or updated by natural business key.

### 7.9 Export

1. User opens a maintenance workspace or planning workspace.
2. User clicks export.
3. Server builds the workbook in the current canonical format.
4. Browser downloads the generated file.

## 8. Current Behavior Notes

- API authorization is active.
- `undo` and `redo` depth is capped at `30`.
- Import and export are still synchronous request-response operations in Phase 1.
- The current UAT runtime is optimized for scoped planning interactions, not unlimited full-hierarchy expansion.
