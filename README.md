# Sales Budget & Planning Reference Skeleton

This repository contains a greenfield reference implementation skeleton for an enterprise sales planning application with:

- `React + TypeScript + AG Grid Community` on the web front end
- `.NET 10 Web API` for planning actions, locking, and the splash engine
- In-memory sample data to keep the skeleton easy to inspect before a real database is introduced

## Workspace Layout

- `apps/api`: .NET backend skeleton with lock-safe edit and splash services
- `apps/web`: React frontend skeleton with an Excel-like planning grid shell
- `docs`: implementation notes and extension guidance

## What Is Included

- Lock-safe planning cell model
- Bottom-up edits with aggregate rollup hooks
- Top-down splash engine with locked-cell exclusion and deterministic residual distribution
- Audit trail contracts
- AG Grid shell with hierarchical rows, grouped year/month columns, and lock/splash actions
- Playwright browser smoke and interaction coverage for core planning flows

## What Is Not Included Yet

- Database persistence
- Authentication wiring
- Excel import/export workers
- Real-time collaboration notifications
- Approval workflows

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

### Browser Interaction Tests

```bash
cd apps/web
npm ci
npx playwright install --with-deps chromium
npm run test:e2e
```

The Playwright harness starts an isolated local stack automatically:

- API on `http://127.0.0.1:5080`
- HTTPS proxy on `https://localhost:7080`
- Vite dev server on `http://localhost:5173`

## Continuous Integration

GitHub Actions validation is defined in [ci.yml](/Users/aloysius/Documents/New project/.github/workflows/ci.yml).

Each run performs:

- `.NET` restore, build, and test
- web dependency install and production build
- Playwright browser install
- end-to-end interaction tests for load, edit/rollup, lock/unlock, and lock-aware splash flows
