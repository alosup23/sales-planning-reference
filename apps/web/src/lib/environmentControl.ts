const CONTROL_API_BASE_URL = import.meta.env.VITE_ENVIRONMENT_CONTROL_URL?.trim() ?? "";
const READY_CACHE_WINDOW_MS = 2 * 60 * 1000;
const STARTUP_TIMEOUT_MS = 15 * 60 * 1000;
const POLL_INTERVAL_MS = 10 * 1000;
const READY_CACHE_KEY = "planning-environment-ready-at";

type EnvironmentStatus = {
  ready: boolean;
  dbStatus: string;
  service: {
    desiredCount: number;
    runningCount: number;
    pendingCount: number;
    rolloutState: string;
  };
};

let ensurePromise: Promise<void> | null = null;

function isControlEnabled(): boolean {
  return CONTROL_API_BASE_URL.length > 0;
}

function readReadyAt(): number {
  if (typeof window === "undefined") {
    return 0;
  }

  const rawValue = window.sessionStorage.getItem(READY_CACHE_KEY);
  const readyAt = rawValue ? Number(rawValue) : 0;
  return Number.isFinite(readyAt) ? readyAt : 0;
}

function markReady(): void {
  if (typeof window === "undefined") {
    return;
  }

  window.sessionStorage.setItem(READY_CACHE_KEY, String(Date.now()));
}

async function fetchControl<T>(path: string, accessToken: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${CONTROL_API_BASE_URL}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });

  if (!response.ok) {
    const problem = await response.json().catch(() => null);
    const detail = typeof problem?.detail === "string"
      ? problem.detail
      : `Environment control request failed with ${response.status}.`;
    throw new Error(detail);
  }

  return await response.json() as T;
}

async function waitForEnvironment(accessToken: string): Promise<void> {
  const startDeadline = Date.now() + STARTUP_TIMEOUT_MS;
  let status = await fetchControl<EnvironmentStatus>("/environment/start", accessToken, { method: "POST" });

  while (!status.ready) {
    if (Date.now() >= startDeadline) {
      throw new Error("Planning environment is starting but did not become ready in time.");
    }

    await new Promise((resolve) => window.setTimeout(resolve, POLL_INTERVAL_MS));
    status = await fetchControl<EnvironmentStatus>("/environment/start", accessToken, { method: "POST" });
  }
}

export async function ensurePlanningEnvironmentReady(accessToken: string): Promise<void> {
  if (!isControlEnabled()) {
    return;
  }

  if (Date.now() - readReadyAt() < READY_CACHE_WINDOW_MS) {
    return;
  }

  if (!ensurePromise) {
    ensurePromise = waitForEnvironment(accessToken)
      .then(() => {
        markReady();
      })
      .finally(() => {
        ensurePromise = null;
      });
  }

  await ensurePromise;
}

export function isPlanningEnvironmentControlEnabled(): boolean {
  return isControlEnabled();
}
