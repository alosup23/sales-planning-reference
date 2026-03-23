import type {
  AddRowRequest,
  EditCellsRequest,
  GridSliceResponse,
  HierarchyMappingResponse,
  LockCellsRequest,
  SplashRequest,
} from "./types";
import { sampleGridData } from "./sampleGridData";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "/api/v1";
const ENABLE_SAMPLE_FALLBACK = import.meta.env.VITE_ENABLE_SAMPLE_FALLBACK === "true";

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
    if (!ENABLE_SAMPLE_FALLBACK) {
      throw new Error("Planning API unavailable.");
    }

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

export async function postAddRow(request: AddRowRequest): Promise<void> {
  await fetchJson(`${API_BASE_URL}/rows`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function getHierarchyMappings(): Promise<HierarchyMappingResponse> {
  return await fetchJson<HierarchyMappingResponse>(`${API_BASE_URL}/hierarchy-mappings`);
}

export async function postHierarchyCategory(categoryLabel: string): Promise<HierarchyMappingResponse> {
  return await fetchJson<HierarchyMappingResponse>(`${API_BASE_URL}/hierarchy-mappings/categories`, {
    method: "POST",
    body: JSON.stringify({ categoryLabel }),
  });
}

export async function postHierarchySubcategory(categoryLabel: string, subcategoryLabel: string): Promise<HierarchyMappingResponse> {
  return await fetchJson<HierarchyMappingResponse>(`${API_BASE_URL}/hierarchy-mappings/subcategories`, {
    method: "POST",
    body: JSON.stringify({ categoryLabel, subcategoryLabel }),
  });
}

export async function postWorkbookImport(scenarioVersionId: number, measureId: number, file: File): Promise<void> {
  const form = new FormData();
  form.append("scenarioVersionId", String(scenarioVersionId));
  form.append("measureId", String(measureId));
  form.append("file", file);

  const response = await fetch(`${API_BASE_URL}/imports/workbook`, {
    method: "POST",
    body: form,
  });

  if (!response.ok) {
    const problem = await response.json().catch(() => null);
    const detail = typeof problem?.detail === "string" ? problem.detail : `${response.status} ${response.statusText}`;
    throw new Error(detail);
  }
}
