# Sales Budget & Planning Reference Skeleton

This repository contains a greenfield reference implementation skeleton for an enterprise sales planning application with:

- `React + TypeScript + AG Grid Enterprise` on the web front end
- `.NET 8 Web API` for planning actions, locking, import/export, and the splash engine
- SQLite-backed sample persistence locally plus an AWS Lambda demo path with S3-backed persistence
- Microsoft Entra single-tenant sign-in on the frontend plus JWT validation on the API

## Workspace Layout

- `apps/api`: .NET backend skeleton with lock-safe edit and splash services
- `apps/web`: React frontend skeleton with an Excel-like planning grid shell
- `docs`: implementation notes and extension guidance

## What Is Included

- Lock-safe planning cell model
- Bottom-up edits with aggregate rollup hooks
- Top-down splash engine with locked-cell exclusion and deterministic residual distribution
- Cross-axis recalculation so row and column totals stay aligned after bottom-up and top-down edits
- Audit trail contracts
- AG Grid shell with `Planning - by Store` and `Planning - by Department` sheets over the same planning data
- Department-first layouts for `Department -> Store -> Class -> Subclass` and `Department -> Class -> Store -> Subclass`
- Distinct aggregate color bands for second- and third-level subtotal rows
- Copied-store creation, workbook upload, and hierarchy maintenance sheet for department/class/subclass mapping
- Workbook import and export support in the store-sheet format with round-trip validation and exception workbooks
- Store Profile maintenance with CRUD, inactivation, controlled option values, and Branch Profile import/export
- Playwright browser smoke and interaction coverage for planning and hierarchy maintenance flows
- API authorization hardening, narrowed CORS, and test-reset disablement outside local/test mode

## What Is Not Included Yet

- Real-time collaboration notifications
- Approval workflows
- Final production persistence architecture, WAF, and private-origin CDN controls

## Local Setup

### API

```bash
cd apps/api/src/SalesPlanning.Api
dotnet restore
dotnet run
```

### Web

```bash
cd apps/web
npm install
npm run dev
```

The frontend expects the API at `https://localhost:7080` by default and can be updated in [`api.ts`](/Users/aloysius/Documents/New project/apps/web/src/lib/api.ts).

For normal local development, the Vite dev server proxies `/api` requests to the API so edits work without any certificate setup.
For local browser tests, auth is intentionally disabled and the API is started with `PlanningSecurityMode=disabled`.

### Browser Interaction Tests

```bash
cd apps/web
npm ci
npx playwright install --with-deps chromium
npm run test:e2e
```

The Playwright harness starts an isolated local stack automatically:

- API on `http://127.0.0.1:5081`
- Vite dev server on `http://127.0.0.1:5173`

## Continuous Integration

GitHub Actions validation is defined in [ci.yml](/Users/aloysius/Documents/New project/.github/workflows/ci.yml).

Each run performs:

- `.NET` restore, build, and test
- web dependency install and production build
- Playwright browser install
- end-to-end interaction tests for load, edit/rollup, lock/unlock, lock-aware splash, copied-store creation, workbook import, and hierarchy maintenance
