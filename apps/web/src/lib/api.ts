import type { EditCellsRequest, GridSliceResponse, LockCellsRequest, SplashRequest } from "./types";
import { sampleGridData } from "./sampleGridData";

const API_BASE_URL = "https://localhost:7080/api/v1";

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
    },
    ...init,
  });

  if (!response.ok) {
    const problem = await response.json().catch(() => null);
    const detail = typeof problem?.detail === "string" ? problem.detail : `${response.status} ${response.statusText}`;
    throw new Error(detail);
  }

  return (await response.json()) as T;
}

export async function getGridSlice(): Promise<GridSliceResponse> {
  try {
    return await fetchJson<GridSliceResponse>(`${API_BASE_URL}/grid-slices?scenarioVersionId=1&measureId=1`);
  } catch {
    return sampleGridData;
  }
}

export async function postEdit(request: EditCellsRequest): Promise<void> {
  await fetchJson(`${API_BASE_URL}/cell-edits`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postSplash(request: SplashRequest): Promise<void> {
  await fetchJson(`${API_BASE_URL}/actions/splash`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postLock(request: LockCellsRequest): Promise<void> {
  await fetchJson(`${API_BASE_URL}/locks`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}
