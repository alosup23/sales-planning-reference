import type {
  AddRowRequest,
  DeleteRowRequest,
  DeleteYearRequest,
  EditCellsRequest,
  GridSliceResponse,
  HierarchyMappingResponse,
  ImportWorkbookResponse,
  LockCellsRequest,
  SaveScenarioRequest,
  SaveScenarioResponse,
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
    return await fetchJson<GridSliceResponse>(`${API_BASE_URL}/grid-slices?scenarioVersionId=1`);
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

export async function postDeleteRow(request: DeleteRowRequest): Promise<void> {
  await fetchJson(`${API_BASE_URL}/rows/delete`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postDeleteYear(request: DeleteYearRequest): Promise<void> {
  await fetchJson(`${API_BASE_URL}/years/delete`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function getHierarchyMappings(): Promise<HierarchyMappingResponse> {
  return await fetchJson<HierarchyMappingResponse>(`${API_BASE_URL}/hierarchy-mappings`);
}

export async function postHierarchyDepartment(departmentLabel: string): Promise<HierarchyMappingResponse> {
  return await fetchJson<HierarchyMappingResponse>(`${API_BASE_URL}/hierarchy-mappings/departments`, {
    method: "POST",
    body: JSON.stringify({ departmentLabel }),
  });
}

export async function postHierarchyClass(departmentLabel: string, classLabel: string): Promise<HierarchyMappingResponse> {
  return await fetchJson<HierarchyMappingResponse>(`${API_BASE_URL}/hierarchy-mappings/classes`, {
    method: "POST",
    body: JSON.stringify({ departmentLabel, classLabel }),
  });
}

export async function postWorkbookImport(scenarioVersionId: number, file: File): Promise<ImportWorkbookResponse> {
  const form = new FormData();
  form.append("scenarioVersionId", String(scenarioVersionId));
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

  return (await response.json()) as ImportWorkbookResponse;
}

export async function postSave(request: SaveScenarioRequest): Promise<SaveScenarioResponse> {
  return await fetchJson<SaveScenarioResponse>(`${API_BASE_URL}/save`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function downloadWorkbookExport(): Promise<void> {
  const response = await fetch(`${API_BASE_URL}/exports/workbook?scenarioVersionId=1`);
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }

  const blob = await response.blob();
  triggerDownload(blob, extractFileName(response.headers.get("content-disposition")) ?? "sales-planning-export.xlsx");
}

export function downloadBase64Workbook(base64Content: string, fileName: string): void {
  const byteCharacters = atob(base64Content);
  const bytes = Uint8Array.from(byteCharacters, (character) => character.charCodeAt(0));
  triggerDownload(
    new Blob([bytes], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" }),
    fileName,
  );
}

function triggerDownload(blob: Blob, fileName: string) {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = fileName;
  anchor.click();
  URL.revokeObjectURL(url);
}

function extractFileName(contentDisposition: string | null): string | null {
  if (!contentDisposition) {
    return null;
  }

  const match = /filename=\"?([^\";]+)\"?/i.exec(contentDisposition);
  return match?.[1] ?? null;
}
