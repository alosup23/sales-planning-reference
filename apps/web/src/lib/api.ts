import type {
  AddRowRequest,
  AddRowResponse,
  ApplyGrowthFactorResponse,
  EditCellsResponse,
  InventoryProfile,
  InventoryProfileImportResponse,
  InventoryProfileResponse,
  DeleteStoreProfileRequest,
  DeleteRowRequest,
  DeleteYearRequest,
  EditCellsRequest,
  GenerateNextYearRequest,
  GrowthFactorRequest,
  GridSliceResponse,
  HierarchyMappingResponse,
  ImportWorkbookResponse,
  LockCellsResponse,
  LockCellsRequest,
  PlanningStoreScopeResponse,
  PlanningInsightResponse,
  PricingPolicy,
  PricingPolicyImportResponse,
  PricingPolicyResponse,
  ProductHierarchyResponse,
  ProductProfile,
  ProductProfileImportResponse,
  ProductProfileOptionsResponse,
  ProductProfileResponse,
  SaveScenarioRequest,
  SaveScenarioResponse,
  UndoPlanningActionResponse,
  UndoRedoAvailability,
  SeasonalityEventProfile,
  SeasonalityEventProfileImportResponse,
  SeasonalityEventProfileResponse,
  RedoPlanningActionResponse,
  StoreProfile,
  StoreProfileImportResponse,
  StoreProfileOptionsResponse,
  StoreProfileResponse,
  SplashResponse,
  SplashRequest,
  UpsertInventoryProfileRequest,
  UpsertPricingPolicyRequest,
  UpsertStoreProfileOptionRequest,
  UpsertStoreProfileRequest,
  UpsertProductProfileOptionRequest,
  UpsertProductProfileRequest,
  UpsertSeasonalityEventProfileRequest,
  UpsertVendorSupplyProfileRequest,
  VendorSupplyProfile,
  VendorSupplyProfileImportResponse,
  VendorSupplyProfileResponse,
} from "./types";
import { sampleGridData } from "./sampleGridData";
import { authEnabled, getAccessToken } from "./auth";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "/api/v1";
const ENABLE_SAMPLE_FALLBACK = import.meta.env.VITE_ENABLE_SAMPLE_FALLBACK === "true";
const REQUEST_TIMEOUT_MS = 120000;

export class ApiRequestError extends Error {
  status?: number;
  code?: string;

  constructor(message: string, status?: number, code?: string) {
    super(message);
    this.name = "ApiRequestError";
    this.status = status;
    this.code = code;
  }
}

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers ?? {});
  const expectsFormData = init?.body instanceof FormData;
  if (!expectsFormData && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  if (authEnabled) {
    try {
      const token = await getAccessToken();
      if (token) {
        headers.set("Authorization", `Bearer ${token}`);
      }
    } catch (error) {
      window.clearTimeout(timeoutId);
      const message = error instanceof Error ? error.message : "Session expired. Sign in again.";
      const code = message.includes("Refreshing Microsoft 365 session") || message.includes("Redirecting to Microsoft 365 sign-in")
        ? "auth-redirect"
        : "auth";
      throw new ApiRequestError(message, 401, code);
    }
  }

  let response: Response;
  try {
    response = await fetch(url, {
      headers,
      ...init,
      signal: controller.signal,
    });
  } catch (error) {
    window.clearTimeout(timeoutId);
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new ApiRequestError("Planning request timed out while loading data.", 504, "timeout");
    }

    throw new ApiRequestError("Unable to reach the planning API.", undefined, "network");
  }

  window.clearTimeout(timeoutId);

  if (!response.ok) {
    const problem = await response.json().catch(() => null);
    const detail = typeof problem?.detail === "string" ? problem.detail : `${response.status} ${response.statusText}`;
    const code = response.status === 401 || response.status === 403 ? "auth" : undefined;
    throw new ApiRequestError(detail, response.status, code);
  }

  try {
    return (await response.json()) as T;
  } catch {
    throw new ApiRequestError("The planning API returned an invalid response.", response.status, "invalid-response");
  }
}

export async function getGridSlice(
  options?: {
    selectedStoreId?: number | null;
    selectedDepartmentLabel?: string | null;
    expandedProductNodeIds?: number[];
    expandAllBranches?: boolean;
  },
): Promise<GridSliceResponse> {
  try {
    const params = new URLSearchParams({ scenarioVersionId: "1" });
    if (options?.selectedStoreId) {
      params.set("selectedStoreId", String(options.selectedStoreId));
    }
    if (options?.selectedDepartmentLabel) {
      params.set("selectedDepartmentLabel", options.selectedDepartmentLabel);
    }
    if (options?.expandedProductNodeIds?.length) {
      params.set("expandedProductNodeIds", options.expandedProductNodeIds.join(","));
    }
    if (options?.expandAllBranches) {
      params.set("expandAllBranches", "true");
    }

    return await fetchJson<GridSliceResponse>(`${API_BASE_URL}/grid-slices?${params.toString()}`);
  } catch (error) {
    if (!ENABLE_SAMPLE_FALLBACK) {
      throw error;
    }

    return sampleGridData;
  }
}

export async function getPlanningStoreScopes(): Promise<PlanningStoreScopeResponse> {
  return await fetchJson<PlanningStoreScopeResponse>(`${API_BASE_URL}/planning-store-scopes`);
}

export async function postEdit(request: EditCellsRequest): Promise<EditCellsResponse> {
  return await fetchJson<EditCellsResponse>(`${API_BASE_URL}/cell-edits`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postSplash(request: SplashRequest): Promise<SplashResponse> {
  return await fetchJson<SplashResponse>(`${API_BASE_URL}/actions/splash`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postGrowthFactor(request: GrowthFactorRequest): Promise<ApplyGrowthFactorResponse> {
  return await fetchJson<ApplyGrowthFactorResponse>(`${API_BASE_URL}/growth-factors/apply`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postLock(request: LockCellsRequest): Promise<LockCellsResponse> {
  return await fetchJson<LockCellsResponse>(`${API_BASE_URL}/locks`, {
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

export async function getUndoRedoAvailability(scenarioVersionId = 1): Promise<UndoRedoAvailability> {
  return await fetchJson<UndoRedoAvailability>(`${API_BASE_URL}/undo-redo/availability?scenarioVersionId=${scenarioVersionId}`);
}

export async function postUndo(scenarioVersionId = 1): Promise<UndoPlanningActionResponse> {
  return await fetchJson<UndoPlanningActionResponse>(`${API_BASE_URL}/actions/undo?scenarioVersionId=${scenarioVersionId}`, {
    method: "POST",
  });
}

export async function postRedo(scenarioVersionId = 1): Promise<RedoPlanningActionResponse> {
  return await fetchJson<RedoPlanningActionResponse>(`${API_BASE_URL}/actions/redo?scenarioVersionId=${scenarioVersionId}`, {
    method: "POST",
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

export async function getInventoryProfiles(searchTerm?: string, pageNumber = 1, pageSize = 50): Promise<InventoryProfileResponse> {
  const params = new URLSearchParams({
    pageNumber: String(pageNumber),
    pageSize: String(pageSize),
  });
  if (searchTerm?.trim()) {
    params.set("searchTerm", searchTerm.trim());
  }

  return await fetchJson<InventoryProfileResponse>(`${API_BASE_URL}/inventory-profiles?${params.toString()}`);
}

export async function postInventoryProfile(request: UpsertInventoryProfileRequest): Promise<InventoryProfile> {
  return await fetchJson<InventoryProfile>(`${API_BASE_URL}/inventory-profiles`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postDeleteInventoryProfile(inventoryProfileId: number): Promise<void> {
  await fetchJson(`${API_BASE_URL}/inventory-profiles/delete`, {
    method: "POST",
    body: JSON.stringify({ inventoryProfileId }),
  });
}

export async function postInactivateInventoryProfile(inventoryProfileId: number): Promise<InventoryProfile> {
  return await fetchJson<InventoryProfile>(`${API_BASE_URL}/inventory-profiles/inactivate`, {
    method: "POST",
    body: JSON.stringify({ inventoryProfileId }),
  });
}

export async function postInventoryProfileImport(file: File): Promise<InventoryProfileImportResponse> {
  const form = new FormData();
  form.append("file", file);
  return await fetchJson<InventoryProfileImportResponse>(`${API_BASE_URL}/imports/inventory-profiles`, {
    method: "POST",
    body: form,
  });
}

export async function downloadInventoryProfileExport(): Promise<void> {
  const headers = new Headers();
  if (authEnabled) {
    const token = await getAccessToken();
    if (token) {
      headers.set("Authorization", `Bearer ${token}`);
    }
  }

  const response = await fetch(`${API_BASE_URL}/exports/inventory-profiles`, { headers });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }

  const blob = await response.blob();
  triggerDownload(blob, extractFileName(response.headers.get("content-disposition")) ?? "inventory-profile-export.xlsx");
}

export async function getPricingPolicies(searchTerm?: string, pageNumber = 1, pageSize = 50): Promise<PricingPolicyResponse> {
  const params = new URLSearchParams({
    pageNumber: String(pageNumber),
    pageSize: String(pageSize),
  });
  if (searchTerm?.trim()) {
    params.set("searchTerm", searchTerm.trim());
  }

  return await fetchJson<PricingPolicyResponse>(`${API_BASE_URL}/pricing-policies?${params.toString()}`);
}

export async function postPricingPolicy(request: UpsertPricingPolicyRequest): Promise<PricingPolicy> {
  return await fetchJson<PricingPolicy>(`${API_BASE_URL}/pricing-policies`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postDeletePricingPolicy(pricingPolicyId: number): Promise<void> {
  await fetchJson(`${API_BASE_URL}/pricing-policies/delete`, {
    method: "POST",
    body: JSON.stringify({ pricingPolicyId }),
  });
}

export async function postInactivatePricingPolicy(pricingPolicyId: number): Promise<PricingPolicy> {
  return await fetchJson<PricingPolicy>(`${API_BASE_URL}/pricing-policies/inactivate`, {
    method: "POST",
    body: JSON.stringify({ pricingPolicyId }),
  });
}

export async function postPricingPolicyImport(file: File): Promise<PricingPolicyImportResponse> {
  const form = new FormData();
  form.append("file", file);
  return await fetchJson<PricingPolicyImportResponse>(`${API_BASE_URL}/imports/pricing-policies`, {
    method: "POST",
    body: form,
  });
}

export async function downloadPricingPolicyExport(): Promise<void> {
  const headers = new Headers();
  if (authEnabled) {
    const token = await getAccessToken();
    if (token) {
      headers.set("Authorization", `Bearer ${token}`);
    }
  }

  const response = await fetch(`${API_BASE_URL}/exports/pricing-policies`, { headers });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }

  const blob = await response.blob();
  triggerDownload(blob, extractFileName(response.headers.get("content-disposition")) ?? "pricing-policy-export.xlsx");
}

export async function getSeasonalityEventProfiles(searchTerm?: string, pageNumber = 1, pageSize = 50): Promise<SeasonalityEventProfileResponse> {
  const params = new URLSearchParams({
    pageNumber: String(pageNumber),
    pageSize: String(pageSize),
  });
  if (searchTerm?.trim()) {
    params.set("searchTerm", searchTerm.trim());
  }

  return await fetchJson<SeasonalityEventProfileResponse>(`${API_BASE_URL}/seasonality-event-profiles?${params.toString()}`);
}

export async function postSeasonalityEventProfile(request: UpsertSeasonalityEventProfileRequest): Promise<SeasonalityEventProfile> {
  return await fetchJson<SeasonalityEventProfile>(`${API_BASE_URL}/seasonality-event-profiles`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postDeleteSeasonalityEventProfile(seasonalityEventProfileId: number): Promise<void> {
  await fetchJson(`${API_BASE_URL}/seasonality-event-profiles/delete`, {
    method: "POST",
    body: JSON.stringify({ seasonalityEventProfileId }),
  });
}

export async function postInactivateSeasonalityEventProfile(seasonalityEventProfileId: number): Promise<SeasonalityEventProfile> {
  return await fetchJson<SeasonalityEventProfile>(`${API_BASE_URL}/seasonality-event-profiles/inactivate`, {
    method: "POST",
    body: JSON.stringify({ seasonalityEventProfileId }),
  });
}

export async function postSeasonalityEventProfileImport(file: File): Promise<SeasonalityEventProfileImportResponse> {
  const form = new FormData();
  form.append("file", file);
  return await fetchJson<SeasonalityEventProfileImportResponse>(`${API_BASE_URL}/imports/seasonality-event-profiles`, {
    method: "POST",
    body: form,
  });
}

export async function downloadSeasonalityEventProfileExport(): Promise<void> {
  const headers = new Headers();
  if (authEnabled) {
    const token = await getAccessToken();
    if (token) {
      headers.set("Authorization", `Bearer ${token}`);
    }
  }

  const response = await fetch(`${API_BASE_URL}/exports/seasonality-event-profiles`, { headers });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }

  const blob = await response.blob();
  triggerDownload(blob, extractFileName(response.headers.get("content-disposition")) ?? "seasonality-events-export.xlsx");
}

export async function getVendorSupplyProfiles(searchTerm?: string, pageNumber = 1, pageSize = 50): Promise<VendorSupplyProfileResponse> {
  const params = new URLSearchParams({
    pageNumber: String(pageNumber),
    pageSize: String(pageSize),
  });
  if (searchTerm?.trim()) {
    params.set("searchTerm", searchTerm.trim());
  }

  return await fetchJson<VendorSupplyProfileResponse>(`${API_BASE_URL}/vendor-supply-profiles?${params.toString()}`);
}

export async function postVendorSupplyProfile(request: UpsertVendorSupplyProfileRequest): Promise<VendorSupplyProfile> {
  return await fetchJson<VendorSupplyProfile>(`${API_BASE_URL}/vendor-supply-profiles`, {
    method: "POST",
    body: JSON.stringify(request),
  });
}

export async function postDeleteVendorSupplyProfile(vendorSupplyProfileId: number): Promise<void> {
  await fetchJson(`${API_BASE_URL}/vendor-supply-profiles/delete`, {
    method: "POST",
    body: JSON.stringify({ vendorSupplyProfileId }),
  });
}

export async function postInactivateVendorSupplyProfile(vendorSupplyProfileId: number): Promise<VendorSupplyProfile> {
  return await fetchJson<VendorSupplyProfile>(`${API_BASE_URL}/vendor-supply-profiles/inactivate`, {
    method: "POST",
    body: JSON.stringify({ vendorSupplyProfileId }),
  });
}

export async function postVendorSupplyProfileImport(file: File): Promise<VendorSupplyProfileImportResponse> {
  const form = new FormData();
  form.append("file", file);
  return await fetchJson<VendorSupplyProfileImportResponse>(`${API_BASE_URL}/imports/vendor-supply-profiles`, {
    method: "POST",
    body: form,
  });
}

export async function downloadVendorSupplyProfileExport(): Promise<void> {
  const headers = new Headers();
  if (authEnabled) {
    const token = await getAccessToken();
    if (token) {
      headers.set("Authorization", `Bearer ${token}`);
    }
  }

  const response = await fetch(`${API_BASE_URL}/exports/vendor-supply-profiles`, { headers });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }

  const blob = await response.blob();
  triggerDownload(blob, extractFileName(response.headers.get("content-disposition")) ?? "vendor-supply-profile-export.xlsx");
}
