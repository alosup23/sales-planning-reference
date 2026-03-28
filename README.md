# Sales Budget & Planning Reference Skeleton

This repository contains the evolving reference implementation for an enterprise sales planning application, plus the target-state requirements and AWS architecture needed to move from UAT into a production-ready platform.

Current implementation stack:

- `React + TypeScript + AG Grid Enterprise` on the web front end
- `.NET 8 Web API` for planning actions, locking, import/export, and the splash engine
- working UAT runtime currently on `S3-backed SQLite`
- in-progress `PostgreSQL` migration path for the next architecture wave
- Microsoft Entra single-tenant sign-in on the frontend plus API auth seams on the backend

## Workspace Layout

- `apps/api`: .NET backend skeleton with lock-safe edit and splash services
- `apps/web`: React frontend skeleton with an Excel-like planning grid shell
- `docs`: product requirements, architecture, and implementation notes

## Primary Design Documents

- UAT requirements and UX behavior:
  - [docs/uat-product-requirements.md](/Users/aloysius/Documents/New%20project/docs/uat-product-requirements.md)
- AWS target-state architecture, data model, API contracts, grid event model, and phased migration:
  - [docs/aws-target-state-architecture.md](/Users/aloysius/Documents/New%20project/docs/aws-target-state-architecture.md)
- master-data import and export formats:
  - [docs/master-data-file-formats.md](/Users/aloysius/Documents/New%20project/docs/master-data-file-formats.md)
- non-functional, calculation, AI, and backlog support documents:
  - [docs/non-functional-requirements.md](/Users/aloysius/Documents/New%20project/docs/non-functional-requirements.md)
  - [docs/calculation-and-reconciliation-spec.md](/Users/aloysius/Documents/New%20project/docs/calculation-and-reconciliation-spec.md)
  - [docs/ai-phase-2-merchandising-architecture.md](/Users/aloysius/Documents/New%20project/docs/ai-phase-2-merchandising-architecture.md)
  - [docs/phase-roadmap-and-backlog.md](/Users/aloysius/Documents/New%20project/docs/phase-roadmap-and-backlog.md)
- current implementation notes:
  - [docs/reference-implementation.md](/Users/aloysius/Documents/New%20project/docs/reference-implementation.md)

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
- Final production runtime cutover to the target AWS architecture

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
