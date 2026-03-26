import type {
  AddRowRequest,
  AddRowResponse,
  DeleteStoreProfileRequest,
  DeleteRowRequest,
  DeleteYearRequest,
  EditCellsRequest,
  GenerateNextYearRequest,
  GrowthFactorRequest,
  GridSliceResponse,
  HierarchyMappingResponse,
  ImportWorkbookResponse,
  LockCellsRequest,
  PlanningInsightResponse,
  ProductHierarchyResponse,
  ProductProfile,
  ProductProfileImportResponse,
  ProductProfileOptionsResponse,
  ProductProfileResponse,
  SaveScenarioRequest,
  SaveScenarioResponse,
  StoreProfile,
  StoreProfileImportResponse,
  StoreProfileOptionsResponse,
  StoreProfileResponse,
  SplashRequest,
  UpsertStoreProfileOptionRequest,
  UpsertStoreProfileRequest,
  UpsertProductProfileOptionRequest,
  UpsertProductProfileRequest,
} from "./types";
import { sampleGridData } from "./sampleGridData";
import { authEnabled, getAccessToken } from "./auth";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "/api/v1";
const ENABLE_SAMPLE_FALLBACK = import.meta.env.VITE_ENABLE_SAMPLE_FALLBACK === "true";

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers ?? {});
  const expectsFormData = init?.body instanceof FormData;
  if (!expectsFormData && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  if (authEnabled) {
    const token = await getAccessToken();
    if (token) {
      headers.set("Authorization", `Bearer ${token}`);
    }
  }

  const response = await fetch(url, {
    headers,
    ...init,
  });

  if (!response.ok) {
    const problem = await response.json().catch(() => null);
    const detail = typeof problem?.detail === "string" ? problem.detail : `${response.status} ${response.statusText}`;
    throw new Error(detail);
  }

  return (await response.json()) as T;
}

export async function getGridSlice(selectedStoreId?: number | null): Promise<GridSliceResponse> {
  try {
    const params = new URLSearchParams({ scenarioVersionId: "1" });
    if (selectedStoreId) {
      params.set("selectedStoreId", String(selectedStoreId));
    }

    return await fetchJson<GridSliceResponse>(`${API_BASE_URL}/grid-slices?${params.toString()}`);
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

export async function postGrowthFactor(request: GrowthFactorRequest): Promise<void> {
  await fetchJson(`${API_BASE_URL}/growth-factors/apply`, {
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

export async function postAddRow(request: AddRowRequest): Promise<AddRowResponse> {
  return await fetchJson<AddRowResponse>(`${API_BASE_URL}/rows`, {
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

export async function postGenerateNextYear(request: GenerateNextYearRequest): Promise<void> {
  await fetchJson(`${API_BASE_URL}/years/generate-next`, {
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

export async function postHierarchySubclass(departmentLabel: string, classLabel: string, subclassLabel: string): Promise<HierarchyMappingResponse> {
  return await fetchJson<HierarchyMappingResponse>(`${API_BASE_URL}/hierarchy-mappings/subclasses`, {
    method: "POST",
    body: JSON.stringify({ departmentLabel, classLabel, subclassLabel }),
  });
}

export async function getPlanningInsights(storeId: number, productNodeId: number, yearTimePeriodId: number): Promise<PlanningInsightResponse> {
  return await fetchJson<PlanningInsightResponse>(
    `${API_BASE_URL}/insights?scenarioVersionId=1&storeId=${storeId}&productNodeId=${productNodeId}&yearTimePeriodId=${yearTimePeriodId}`,
  );
}

export async function postWorkbookImport(scenarioVersionId: number, file: File): Promise<ImportWorkbookResponse> {
  const form = new FormData();
  form.append("scenarioVersionId", String(scenarioVersionId));
  form.append("file", file);

  return await fetchJson<ImportWorkbookResponse>(`${API_BASE_URL}/imports/workbook`, {
    method: "POST",
    body: form,
  });
}

export async function postSave(request: SaveScenarioRequest): Promise<SaveScenarioResponse> {
  return await fetchJson<SaveScenarioResponse>(`${API_BASE_URL}/save`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function downloadWorkbookExport(): Promise<void> {
  const headers = new Headers();
  if (authEnabled) {
    const token = await getAccessToken();
    if (token) {
      headers.set("Authorization", `Bearer ${token}`);
    }
  }

  const response = await fetch(`${API_BASE_URL}/exports/workbook?scenarioVersionId=1`, { headers });
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

export async function getStoreProfiles(): Promise<StoreProfileResponse> {
  return await fetchJson<StoreProfileResponse>(`${API_BASE_URL}/store-profiles`);
}

export async function postStoreProfile(request: UpsertStoreProfileRequest): Promise<StoreProfile> {
  return await fetchJson<StoreProfile>(`${API_BASE_URL}/store-profiles`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postDeleteStoreProfile(request: DeleteStoreProfileRequest): Promise<void> {
  await fetchJson(`${API_BASE_URL}/store-profiles/delete`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postInactivateStoreProfile(storeId: number): Promise<StoreProfile> {
  return await fetchJson<StoreProfile>(`${API_BASE_URL}/store-profiles/inactivate`, {
    method: "POST",
    body: JSON.stringify({ storeId }),
  });
}

export async function getStoreProfileOptions(): Promise<StoreProfileOptionsResponse> {
  return await fetchJson<StoreProfileOptionsResponse>(`${API_BASE_URL}/store-profile-options`);
}

export async function postStoreProfileOption(request: UpsertStoreProfileOptionRequest): Promise<StoreProfileOptionsResponse> {
  return await fetchJson<StoreProfileOptionsResponse>(`${API_BASE_URL}/store-profile-options`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postDeleteStoreProfileOption(fieldName: string, value: string): Promise<StoreProfileOptionsResponse> {
  return await fetchJson<StoreProfileOptionsResponse>(`${API_BASE_URL}/store-profile-options/delete`, {
    method: "POST",
    body: JSON.stringify({ fieldName, value }),
  });
}

export async function postStoreProfileImport(file: File): Promise<StoreProfileImportResponse> {
  const form = new FormData();
  form.append("file", file);
  return await fetchJson<StoreProfileImportResponse>(`${API_BASE_URL}/imports/store-profiles`, {
    method: "POST",
    body: form,
  });
}

export async function downloadStoreProfileExport(): Promise<void> {
  const headers = new Headers();
  if (authEnabled) {
    const token = await getAccessToken();
    if (token) {
      headers.set("Authorization", `Bearer ${token}`);
    }
  }

  const response = await fetch(`${API_BASE_URL}/exports/store-profiles`, { headers });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }

  const blob = await response.blob();
  triggerDownload(blob, extractFileName(response.headers.get("content-disposition")) ?? "store-profile-export.xlsx");
}

export async function getProductProfiles(searchTerm?: string, pageNumber = 1, pageSize = 50): Promise<ProductProfileResponse> {
  const params = new URLSearchParams({
    pageNumber: String(pageNumber),
    pageSize: String(pageSize),
  });
  if (searchTerm?.trim()) {
    params.set("searchTerm", searchTerm.trim());
  }

  return await fetchJson<ProductProfileResponse>(`${API_BASE_URL}/product-profiles?${params.toString()}`);
}

export async function postProductProfile(request: UpsertProductProfileRequest): Promise<ProductProfile> {
  return await fetchJson<ProductProfile>(`${API_BASE_URL}/product-profiles`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postDeleteProductProfile(skuVariant: string): Promise<void> {
  await fetchJson(`${API_BASE_URL}/product-profiles/delete`, {
    method: "POST",
    body: JSON.stringify({ skuVariant }),
  });
}

export async function postInactivateProductProfile(skuVariant: string): Promise<ProductProfile> {
  return await fetchJson<ProductProfile>(`${API_BASE_URL}/product-profiles/inactivate`, {
    method: "POST",
    body: JSON.stringify({ skuVariant }),
  });
}

export async function getProductProfileOptions(): Promise<ProductProfileOptionsResponse> {
  return await fetchJson<ProductProfileOptionsResponse>(`${API_BASE_URL}/product-profile-options`);
}

export async function postProductProfileOption(request: UpsertProductProfileOptionRequest): Promise<ProductProfileOptionsResponse> {
  return await fetchJson<ProductProfileOptionsResponse>(`${API_BASE_URL}/product-profile-options`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postDeleteProductProfileOption(fieldName: string, value: string): Promise<ProductProfileOptionsResponse> {
  return await fetchJson<ProductProfileOptionsResponse>(`${API_BASE_URL}/product-profile-options/delete`, {
    method: "POST",
    body: JSON.stringify({ fieldName, value }),
  });
}

export async function getProductHierarchy(): Promise<ProductHierarchyResponse> {
  return await fetchJson<ProductHierarchyResponse>(`${API_BASE_URL}/product-hierarchy`);
}

export async function postProductHierarchy(request: { dptNo: string; clssNo: string; department: string; class: string; prodGroup: string; isActive: boolean }): Promise<ProductHierarchyResponse> {
  return await fetchJson<ProductHierarchyResponse>(`${API_BASE_URL}/product-hierarchy`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postDeleteProductHierarchy(dptNo: string, clssNo: string): Promise<ProductHierarchyResponse> {
  return await fetchJson<ProductHierarchyResponse>(`${API_BASE_URL}/product-hierarchy/delete`, {
    method: "POST",
    body: JSON.stringify({ dptNo, clssNo }),
  });
}

export async function postProductProfileImport(file: File): Promise<ProductProfileImportResponse> {
  const form = new FormData();
  form.append("file", file);
  return await fetchJson<ProductProfileImportResponse>(`${API_BASE_URL}/imports/product-profiles`, {
    method: "POST",
    body: form,
  });
}

export async function downloadProductProfileExport(): Promise<void> {
  const headers = new Headers();
  if (authEnabled) {
    const token = await getAccessToken();
    if (token) {
      headers.set("Authorization", `Bearer ${token}`);
    }
  }

  const response = await fetch(`${API_BASE_URL}/exports/product-profiles`, { headers });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }

  const blob = await response.blob();
  triggerDownload(blob, extractFileName(response.headers.get("content-disposition")) ?? "product-profile-export.xlsx");
}
