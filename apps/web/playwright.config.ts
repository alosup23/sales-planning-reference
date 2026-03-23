import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "@playwright/test";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const workspaceRoot = path.resolve(__dirname, "../..");
const dotnetHome = path.join(workspaceRoot, ".dotnet-cli");
const chromePath = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";

export default defineConfig({
  testDir: "./tests",
  timeout: 30_000,
  expect: {
    timeout: 10_000,
  },
  fullyParallel: false,
  workers: 1,
  reporter: [["list"]],
  use: {
    baseURL: "http://localhost:5173",
    trace: "on-first-retry",
    launchOptions: fs.existsSync(chromePath) ? { executablePath: chromePath } : {},
  },
  webServer: [
    {
      command: `ASPNETCORE_URLS=http://127.0.0.1:5080 DOTNET_CLI_HOME="${dotnetHome}" DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1 dotnet run --no-build`,
      cwd: path.join(workspaceRoot, "apps/api/src/SalesPlanning.Api"),
      url: "http://127.0.0.1:5080/api/v1/grid-slices?scenarioVersionId=1&measureId=1",
      reuseExistingServer: false,
    },
    {
      command: "VITE_API_PROXY_TARGET=http://127.0.0.1:5080 npm run dev -- --host localhost --port 5173",
      cwd: path.join(workspaceRoot, "apps/web"),
      url: "http://localhost:5173",
      reuseExistingServer: false,
    },
  ],
});
