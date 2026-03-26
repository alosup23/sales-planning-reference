# Reference Implementation Notes

## Backend

The API uses an action-driven service layer:

- manual edits validate permissions and locks before writing
- splash actions calculate allocations against unlocked targets only
- rollups recompute only impacted ancestors
- audit entries capture action headers and cell deltas

The in-memory repository is intentionally small but keeps the production seams in place:

- repository for current values and metadata
- service for orchestration
- allocator for deterministic splash logic

## Frontend

The React app is organized around three concerns:

- API contracts in `src/lib`
- grid rendering in `src/components`
- app-level orchestration in `src/App.tsx`

AG Grid is configured to mimic a planning sheet:

- tree rows for `Store -> Department -> Class -> Subclass`
- alternate department-first projections for `Department -> Store -> Class -> Subclass` and `Department -> Class -> Store -> Subclass`
- grouped columns for `Year -> Month`
- editable month and year cells based on metadata
- visual states for locked, calculated, overridden, and depth-based aggregate bands

## Suggested Next Steps

1. Replace the SQLite demo repository with PostgreSQL + Redis.
2. Add Azure AD authentication and store-based ABAC.
3. Move audit writes to immutable append tables.
4. Move workbook import/export and forecast jobs into background workers.
