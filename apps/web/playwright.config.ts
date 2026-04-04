import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "@playwright/test";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const workspaceRoot = path.resolve(__dirname, "../..");
const dotnetHome = path.join(workspaceRoot, ".dotnet-cli");
const localDotnetRoot = path.join(workspaceRoot, ".dotnet8");
const dotnetCommand = fs.existsSync(path.join(localDotnetRoot, "dotnet"))
  ? `"${path.join(localDotnetRoot, "dotnet")}"`
  : "dotnet";
const apiProjectPath = path.join(workspaceRoot, "apps/api/src/SalesPlanning.Api/SalesPlanning.Api.csproj");
const apiAssemblyPath = path.join(workspaceRoot, "apps/api/src/SalesPlanning.Api/bin/Debug/net8.0/SalesPlanning.Api.dll");
const manualServers = process.env.PLAYWRIGHT_MANUAL_SERVERS === "true";
const systemChromePath = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const useSystemChrome = process.env.PLAYWRIGHT_USE_SYSTEM_CHROME === "true" && fs.existsSync(systemChromePath);

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
    baseURL: "http://127.0.0.1:5173",
    trace: "on-first-retry",
    launchOptions: useSystemChrome ? { executablePath: systemChromePath } : {},
  },
  webServer: manualServers ? undefined : [
    {
      command: `DOTNET_ROOT="${localDotnetRoot}" DOTNET_CLI_HOME="${dotnetHome}" DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1 ${dotnetCommand} build "${apiProjectPath}" && ASPNETCORE_URLS=http://127.0.0.1:5081 PlanningSecurityMode=disabled EnableTestReset=true DOTNET_ROOT="${localDotnetRoot}" DOTNET_CLI_HOME="${dotnetHome}" DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1 ${dotnetCommand} exec "${apiAssemblyPath}"`,
      cwd: path.join(workspaceRoot, "apps/api/src/SalesPlanning.Api"),
      url: "http://127.0.0.1:5081/api/v1/grid-slices?scenarioVersionId=1&measureId=1",
      reuseExistingServer: true,
    },
    {
      command: "VITE_AUTH_MODE=disabled VITE_API_PROXY_TARGET=http://127.0.0.1:5081 npm run dev -- --host 127.0.0.1 --port 5173",
      cwd: path.join(workspaceRoot, "apps/web"),
      url: "http://127.0.0.1:5173",
      reuseExistingServer: true,
    },
  ],
});
