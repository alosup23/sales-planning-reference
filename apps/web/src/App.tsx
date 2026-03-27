import { Suspense, lazy, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  ApiRequestError,
  downloadProductProfileExport,
  downloadStoreProfileExport,
  downloadBase64Workbook,
  downloadWorkbookExport,
  getGridSlice,
  getHierarchyMappings,
  getPlanningStoreScopes,
  getPlanningInsights,
  getProductHierarchy,
  getProductProfileOptions,
  getProductProfiles,
  getStoreProfileOptions,
  getStoreProfiles,
  postAddRow,
  postDeleteProductHierarchy,
  postDeleteProductProfile,
  postDeleteProductProfileOption,
  postDeleteStoreProfile,
  postDeleteRow,
  postDeleteYear,
  postEdit,
  postGenerateNextYear,
  postGrowthFactor,
  postHierarchyClass,
  postHierarchyDepartment,
  postHierarchySubclass,
  postLock,
  postProductHierarchy,
  postProductProfile,
  postProductProfileImport,
  postProductProfileOption,
  postInactivateProductProfile,
  postSave,
  postSplash,
  postStoreProfile,
  postStoreProfileImport,
  postStoreProfileOption,
  postDeleteStoreProfileOption,
  postInactivateStoreProfile,
  postWorkbookImport,
} from "./lib/api";
import { SignedInUserMenu } from "./components/AuthShell";
import { HierarchyMaintenanceSheet } from "./components/HierarchyMaintenanceSheet";
import { ProductProfileMaintenanceSheet } from "./components/ProductProfileMaintenanceSheet";
import { StoreProfileMaintenanceSheet } from "./components/StoreProfileMaintenanceSheet";
import { authEnabled } from "./lib/auth";
import type {
  AddRowResponse,
  GridCell,
  GridMeasure,
  GridRow,
  GridSliceResponse,
  PlanningStoreScope,
  PlanningInsightResponse,
  ProductHierarchyCatalog,
  ProductProfile,
  StoreProfile,
  UpsertProductProfileRequest,
  UpsertStoreProfileRequest,
} from "./lib/types";

const preloadPlanningGrid = () => import("./components/PlanningGrid");

const PlanningGrid = lazy(async () => {
  const module = await preloadPlanningGrid();
  return { default: module.PlanningGrid };
});

type ActiveView = "planning-store" | "planning-department" | "hierarchy" | "store-profile" | "product-profile";
type DepartmentLayout = "department-store-class" | "department-class-store";

export default function App() {
  const queryClient = useQueryClient();
  const [lastError, setLastError] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<ActiveView>("planning-store");
  const [departmentLayout, setDepartmentLayout] = useState<DepartmentLayout>("department-store-class");
  const [selectedYearId, setSelectedYearId] = useState<number | null>(null);
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
  const [lastSavedAt, setLastSavedAt] = useState<string | null>(null);
  const [insightScope, setInsightScope] = useState<{ storeId: number; productNodeId: number; yearTimePeriodId: number } | null>(null);
  const [pendingRevealRow, setPendingRevealRow] = useState<AddRowResponse | null>(null);
  const [productSearchTerm, setProductSearchTerm] = useState("");
  const [productPageNumber, setProductPageNumber] = useState(1);
  const [selectedPlanningStoreId, setSelectedPlanningStoreId] = useState<number | null>(null);
  const [selectedDepartmentLabel, setSelectedDepartmentLabel] = useState<string | null>(null);
  const [expandedBranchNodeIds, setExpandedBranchNodeIds] = useState<number[]>([]);
  const [expandAllBranches, setExpandAllBranches] = useState(false);

  useEffect(() => {
    void preloadPlanningGrid();
  }, []);

  const gridQuery = useQuery({
    queryKey: ["grid-slice", 1, activeView, selectedPlanningStoreId, selectedDepartmentLabel, expandedBranchNodeIds.join(","), expandAllBranches ? "all" : "branch"],
    queryFn: () => getGridSlice({
      selectedStoreId: activeView === "planning-store" ? selectedPlanningStoreId : null,
      selectedDepartmentLabel: activeView === "planning-department" ? selectedDepartmentLabel : null,
      expandedProductNodeIds: activeView === "planning-store" ? expandedBranchNodeIds : [],
      expandAllBranches,
    }),
    enabled: activeView === "planning-store" ? selectedPlanningStoreId !== null : activeView === "planning-department",
  });
  const planningStoreScopeQuery = useQuery({
    queryKey: ["planning-store-scopes"],
    queryFn: getPlanningStoreScopes,
    enabled: activeView === "planning-store" || activeView === "planning-department",
  });
  const hierarchyQuery = useQuery({
    queryKey: ["hierarchy-mappings"],
    queryFn: getHierarchyMappings,
    enabled: activeView === "hierarchy",
  });
  const storeProfileQuery = useQuery({
    queryKey: ["store-profiles"],
    queryFn: getStoreProfiles,
    enabled: activeView === "store-profile",
  });
  const storeProfileOptionsQuery = useQuery({
    queryKey: ["store-profile-options"],
    queryFn: getStoreProfileOptions,
    enabled: activeView === "store-profile",
  });
  const productProfileQuery = useQuery({
    queryKey: ["product-profiles", productSearchTerm, productPageNumber],
    queryFn: () => getProductProfiles(productSearchTerm, productPageNumber, 50),
    enabled: activeView === "product-profile",
  });
  const productProfileOptionsQuery = useQuery({
    queryKey: ["product-profile-options"],
    queryFn: getProductProfileOptions,
    enabled: activeView === "product-profile",
  });
  const productHierarchyQuery = useQuery({
    queryKey: ["product-hierarchy"],
    queryFn: getProductHierarchy,
    enabled: activeView === "product-profile",
  });
  const insightQuery = useQuery({
    queryKey: ["planning-insights", insightScope?.storeId, insightScope?.productNodeId, insightScope?.yearTimePeriodId],
    queryFn: () => getPlanningInsights(insightScope!.storeId, insightScope!.productNodeId, insightScope!.yearTimePeriodId),
    enabled: Boolean(insightScope),
  });

  useEffect(() => {
    if (selectedPlanningStoreId || !planningStoreScopeQuery.data?.stores.length) {
      return;
    }

    const firstActiveStore = planningStoreScopeQuery.data.stores.find((store) => store.isActive) ?? planningStoreScopeQuery.data.stores[0];
    setSelectedPlanningStoreId(firstActiveStore.storeId);
  }, [selectedPlanningStoreId, planningStoreScopeQuery.data]);

  useEffect(() => {
    setExpandedBranchNodeIds([]);
  }, [selectedPlanningStoreId, selectedDepartmentLabel]);

  useEffect(() => {
    setExpandedBranchNodeIds([]);
    setExpandAllBranches(false);
  }, [activeView, departmentLayout]);

  useEffect(() => {
    if (selectedYearId || !gridQuery.data) {
      return;
    }

    const firstYear = gridQuery.data.periods.find((period) => period.grain === "year");
    setSelectedYearId(firstYear?.timePeriodId ?? null);
  }, [gridQuery.data, selectedYearId]);

  const refresh = async () => {
    await Promise.all([
      queryClient.refetchQueries({ queryKey: ["grid-slice", 1], type: "active" }),
      queryClient.refetchQueries({ queryKey: ["planning-store-scopes"], type: "active" }),
      queryClient.refetchQueries({ queryKey: ["hierarchy-mappings"], type: "active" }),
      queryClient.refetchQueries({ queryKey: ["store-profiles"], type: "active" }),
      queryClient.refetchQueries({ queryKey: ["store-profile-options"], type: "active" }),
      queryClient.refetchQueries({ queryKey: ["product-profiles"], type: "active" }),
      queryClient.refetchQueries({ queryKey: ["product-profile-options"], type: "active" }),
      queryClient.refetchQueries({ queryKey: ["product-hierarchy"], type: "active" }),
    ]);
  };

  const editMutation = useMutation({
    mutationFn: postEdit,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const lockMutation = useMutation({
    mutationFn: postLock,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const splashMutation = useMutation({
    mutationFn: postSplash,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const addRowMutation = useMutation({
    mutationFn: postAddRow,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const deleteRowMutation = useMutation({
    mutationFn: postDeleteRow,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const deleteYearMutation = useMutation({
    mutationFn: postDeleteYear,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const generateNextYearMutation = useMutation({
    mutationFn: postGenerateNextYear,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const importMutation = useMutation({
    mutationFn: ({ file, scenarioVersionId }: { file: File; scenarioVersionId: number }) => postWorkbookImport(scenarioVersionId, file),
    onSuccess: async (result) => {
      setLastError(null);
      setHasUnsavedChanges(true);
      if (result.exceptionWorkbookBase64 && result.exceptionFileName) {
        downloadBase64Workbook(result.exceptionWorkbookBase64, result.exceptionFileName);
      }

      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const exportMutation = useMutation({
    mutationFn: downloadWorkbookExport,
    onSuccess: () => setLastError(null),
    onError: (error: Error) => setLastError(error.message),
  });

  const growthFactorMutation = useMutation({
    mutationFn: postGrowthFactor,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const saveMutation = useMutation({
    mutationFn: ({ mode }: { mode: "manual" | "autosave" }) => postSave({ scenarioVersionId: 1, mode }),
    onSuccess: (result) => {
      setLastError(null);
      setHasUnsavedChanges(false);
      setLastSavedAt(result.savedAt);
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const addHierarchyDepartmentMutation = useMutation({
    mutationFn: postHierarchyDepartment,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const addHierarchyClassMutation = useMutation({
    mutationFn: ({ departmentLabel, classLabel }: { departmentLabel: string; classLabel: string }) =>
      postHierarchyClass(departmentLabel, classLabel),
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const addHierarchySubclassMutation = useMutation({
    mutationFn: ({ departmentLabel, classLabel, subclassLabel }: { departmentLabel: string; classLabel: string; subclassLabel: string }) =>
      postHierarchySubclass(departmentLabel, classLabel, subclassLabel),
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const upsertStoreProfileMutation = useMutation({
    mutationFn: postStoreProfile,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const deleteStoreProfileMutation = useMutation({
    mutationFn: postDeleteStoreProfile,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const inactivateStoreProfileMutation = useMutation({
    mutationFn: postInactivateStoreProfile,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const storeProfileImportMutation = useMutation({
    mutationFn: postStoreProfileImport,
    onSuccess: async (result) => {
      setLastError(null);
      setHasUnsavedChanges(true);
      if (result.exceptionWorkbookBase64 && result.exceptionFileName) {
        downloadBase64Workbook(result.exceptionWorkbookBase64, result.exceptionFileName);
      }

      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const storeProfileExportMutation = useMutation({
    mutationFn: downloadStoreProfileExport,
    onSuccess: () => setLastError(null),
    onError: (error: Error) => setLastError(error.message),
  });

  const storeProfileOptionMutation = useMutation({
    mutationFn: ({ fieldName, value, isActive }: { fieldName: string; value: string; isActive: boolean }) =>
      postStoreProfileOption({ fieldName, value, isActive }),
    onSuccess: async () => {
      setLastError(null);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const deleteStoreProfileOptionMutation = useMutation({
    mutationFn: ({ fieldName, value }: { fieldName: string; value: string }) =>
      postDeleteStoreProfileOption(fieldName, value),
    onSuccess: async () => {
      setLastError(null);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const upsertProductProfileMutation = useMutation({
    mutationFn: postProductProfile,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const deleteProductProfileMutation = useMutation({
    mutationFn: postDeleteProductProfile,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const inactivateProductProfileMutation = useMutation({
    mutationFn: postInactivateProductProfile,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const productProfileImportMutation = useMutation({
    mutationFn: postProductProfileImport,
    onSuccess: async (result) => {
      setLastError(null);
      setHasUnsavedChanges(true);
      if (result.exceptionWorkbookBase64 && result.exceptionFileName) {
        downloadBase64Workbook(result.exceptionWorkbookBase64, result.exceptionFileName);
      }

      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const productProfileExportMutation = useMutation({
    mutationFn: downloadProductProfileExport,
    onSuccess: () => setLastError(null),
    onError: (error: Error) => setLastError(error.message),
  });

  const productProfileOptionMutation = useMutation({
    mutationFn: ({ fieldName, value, isActive }: { fieldName: string; value: string; isActive: boolean }) =>
      postProductProfileOption({ fieldName, value, isActive }),
    onSuccess: async () => {
      setLastError(null);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const deleteProductProfileOptionMutation = useMutation({
    mutationFn: ({ fieldName, value }: { fieldName: string; value: string }) =>
      postDeleteProductProfileOption(fieldName, value),
    onSuccess: async () => {
      setLastError(null);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const productHierarchyMutation = useMutation({
    mutationFn: postProductHierarchy,
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const deleteProductHierarchyMutation = useMutation({
    mutationFn: ({ dptNo, clssNo }: { dptNo: string; clssNo: string }) => postDeleteProductHierarchy(dptNo, clssNo),
    onSuccess: async () => {
      setLastError(null);
      setHasUnsavedChanges(true);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  useEffect(() => {
    const interval = window.setInterval(() => {
      if (!hasUnsavedChanges || saveMutation.isPending) {
        return;
      }

      void saveMutation.mutateAsync({ mode: "autosave" });
    }, 5 * 60 * 1000);

    return () => window.clearInterval(interval);
  }, [hasUnsavedChanges, saveMutation]);

  const isMutating =
    editMutation.isPending ||
    lockMutation.isPending ||
    splashMutation.isPending ||
    addRowMutation.isPending ||
    deleteRowMutation.isPending ||
    deleteYearMutation.isPending ||
    generateNextYearMutation.isPending ||
    importMutation.isPending ||
    exportMutation.isPending ||
    growthFactorMutation.isPending ||
    saveMutation.isPending ||
    addHierarchyDepartmentMutation.isPending ||
    addHierarchyClassMutation.isPending ||
    addHierarchySubclassMutation.isPending ||
    upsertStoreProfileMutation.isPending ||
    deleteStoreProfileMutation.isPending ||
    inactivateStoreProfileMutation.isPending ||
    storeProfileImportMutation.isPending ||
    storeProfileExportMutation.isPending ||
    storeProfileOptionMutation.isPending ||
    deleteStoreProfileOptionMutation.isPending ||
    upsertProductProfileMutation.isPending ||
    deleteProductProfileMutation.isPending ||
    inactivateProductProfileMutation.isPending ||
    productProfileImportMutation.isPending ||
    productProfileExportMutation.isPending ||
    productProfileOptionMutation.isPending ||
    deleteProductProfileOptionMutation.isPending ||
    productHierarchyMutation.isPending ||
    deleteProductHierarchyMutation.isPending;

  const statusText = useMemo(() => {
    const planningViewActive = activeView === "planning-store" || activeView === "planning-department";
    if ((planningViewActive && (planningStoreScopeQuery.isLoading || gridQuery.isLoading || gridQuery.isFetching))
      || (activeView === "store-profile" && storeProfileQuery.isLoading)
      || (activeView === "hierarchy" && hierarchyQuery.isLoading)
      || (activeView === "store-profile" && storeProfileOptionsQuery.isLoading)
      || (activeView === "product-profile" && (productProfileQuery.isLoading || productProfileOptionsQuery.isLoading || productHierarchyQuery.isLoading))) {
      return "Loading planning slice...";
    }

    if (isMutating) {
      return "Applying changes...";
    }

    const activeError =
      (planningViewActive ? planningStoreScopeQuery.error : null) ??
      (planningViewActive ? gridQuery.error : null) ??
      (activeView === "store-profile" ? storeProfileQuery.error : null) ??
      (activeView === "hierarchy" ? hierarchyQuery.error : null) ??
      (activeView === "store-profile" ? storeProfileOptionsQuery.error : null) ??
      (activeView === "product-profile" ? (productProfileQuery.error ?? productProfileOptionsQuery.error ?? productHierarchyQuery.error) : null);

    if (activeError) {
      if (activeError instanceof ApiRequestError) {
        if (activeError.code === "auth-redirect") {
          return activeError.message;
        }

        if (activeError.code === "auth" || activeError.status === 401 || activeError.status === 403) {
          return "Microsoft 365 session expired or not authorized. Sign in again.";
        }

        if (activeError.code === "timeout" || activeError.status === 504) {
          return "Planning data took too long to load. Try expanding fewer branches or refresh.";
        }

        if (activeError.code === "network") {
          return "Unable to reach the planning API.";
        }

        return activeError.message;
      }

      return activeError instanceof Error ? activeError.message : "API unavailable.";
    }

    if (lastError) {
      return lastError;
    }

    if (hasUnsavedChanges) {
      return "Changes pending save. Autosave runs every 5 minutes.";
    }

    if (lastSavedAt) {
      return `All changes saved. Last checkpoint ${new Date(lastSavedAt).toLocaleTimeString()}.`;
    }

    return activeView === "hierarchy"
      ? "Department / Class maintenance sheet ready."
      : activeView === "store-profile"
        ? "Store profile maintenance ready."
        : activeView === "product-profile"
          ? "Product profile maintenance ready."
      : "Multi-year planning grid ready.";
  }, [activeView, gridQuery.error, gridQuery.isFetching, gridQuery.isLoading, hasUnsavedChanges, hierarchyQuery.error, hierarchyQuery.isLoading, isMutating, lastError, lastSavedAt, planningStoreScopeQuery.error, planningStoreScopeQuery.isLoading, productHierarchyQuery.error, productHierarchyQuery.isLoading, productProfileOptionsQuery.error, productProfileOptionsQuery.isLoading, productProfileQuery.error, productProfileQuery.isLoading, storeProfileOptionsQuery.error, storeProfileOptionsQuery.isLoading, storeProfileQuery.error, storeProfileQuery.isLoading]);

  const storeViewData = useMemo(
    () => (gridQuery.data ? buildStoreView(gridQuery.data) : null),
    [gridQuery.data],
  );
  const departmentViewData = useMemo(
    () => (gridQuery.data ? buildDepartmentView(gridQuery.data, departmentLayout) : null),
    [departmentLayout, gridQuery.data],
  );
  const activeGridData = activeView === "planning-department" ? departmentViewData : storeViewData;

  useEffect(() => {
    if (activeView !== "planning-department" || selectedDepartmentLabel || !departmentViewData) {
      return;
    }

    const firstDepartment = departmentViewData.rows.find((row) => row.structureRole === "department" && row.level === 1);
    if (firstDepartment) {
      setSelectedDepartmentLabel(firstDepartment.label);
      setExpandAllBranches(true);
    }
  }, [activeView, departmentViewData, selectedDepartmentLabel]);

  const planningReady = (activeView !== "planning-store" && activeView !== "planning-department")
    || (!!planningStoreScopeQuery.data
      && (activeView !== "planning-store" || selectedPlanningStoreId !== null)
      && !!gridQuery.data
      && !!storeViewData
      && !!departmentViewData);
  const hierarchyReady = activeView !== "hierarchy" || !!hierarchyQuery.data;
  const storeProfileReady = activeView !== "store-profile" || (!!storeProfileQuery.data && !!storeProfileOptionsQuery.data);
  const productMaintenanceReady = activeView !== "product-profile" || (productProfileQuery.data && productProfileOptionsQuery.data && productHierarchyQuery.data);

  if (!planningReady || !hierarchyReady || !storeProfileReady || !productMaintenanceReady) {
    return (
      <main className="app-shell">
        <section className="hero">
          <h1>Sales Budget & Planning</h1>
          <p>{statusText}</p>
        </section>
      </main>
    );
  }

  const planningData = gridQuery.data ?? null;
  const yearPeriods = planningData?.periods.filter((period) => period.grain === "year") ?? [];
  const measureLookup = new Map((planningData?.measures ?? []).map((measure) => [measure.measureId, measure]));

  const handleCellEdit = async (row: GridRow, timePeriodId: number, measureId: number, newValue: number) => {
    if (!planningData) {
      return;
    }

    const measureCell = row.cells[timePeriodId]?.measures[measureId];
    const period = planningData.periods.find((item) => item.timePeriodId === timePeriodId);
    const isLeafMonth = row.isLeaf && period?.grain === "month";
    const scopeRoots = row.splashRoots ?? [];

    if (isLeafMonth && row.bindingProductNodeId) {
      await editMutation.mutateAsync({
        scenarioVersionId: planningData.scenarioVersionId,
        measureId,
        comment: "Grid edit",
        cells: [
          {
            storeId: row.bindingStoreId ?? row.storeId,
            productNodeId: row.bindingProductNodeId,
            timePeriodId,
            newValue,
            editMode: "input",
            rowVersion: measureCell?.rowVersion ?? 0,
          },
        ],
      });
      return;
    }

    if (scopeRoots.length === 0) {
      setLastError("This subtotal is read-only in the current view.");
      return;
    }

    await splashMutation.mutateAsync({
      scenarioVersionId: planningData.scenarioVersionId,
      measureId,
      sourceCell: {
        storeId: scopeRoots[0].storeId,
        productNodeId: scopeRoots[0].productNodeId,
        timePeriodId,
      },
      totalValue: newValue,
      method: period?.grain === "year" ? "seasonality_profile" : "existing_plan",
      roundingScale: measureLookup.get(measureId)?.decimalPlaces ?? 0,
      comment: "Grid top-down edit",
      manualWeights: period?.grain === "year" ? buildSeasonalityWeights(planningData, timePeriodId) : undefined,
      scopeRoots,
    });
  };

  const handleToggleLock = async (row: GridRow, timePeriodId: number, measureId: number, locked: boolean) => {
    if (!planningData) {
      return;
    }

    const scopeRoots = row.splashRoots ?? [];
    if (scopeRoots.length === 0) {
      setLastError("This row cannot be locked in the current view.");
      return;
    }

    await lockMutation.mutateAsync({
      scenarioVersionId: planningData.scenarioVersionId,
      measureId,
      locked,
      reason: locked ? "Manager review hold" : "Released",
      coordinates: scopeRoots.map((scopeRoot) => ({
        storeId: scopeRoot.storeId,
        productNodeId: scopeRoot.productNodeId,
        timePeriodId,
      })),
    });
  };

  const handleSplashYear = async (row: GridRow, yearTimePeriodId: number, measureId: number) => {
    if (!planningData) {
      return;
    }

    const scopeRoots = row.splashRoots ?? [];
    if (scopeRoots.length === 0) {
      setLastError("This row cannot be splashed in the current view.");
      return;
    }

    const totalValue = row.cells[yearTimePeriodId]?.measures[measureId]?.value ?? 0;
    await splashMutation.mutateAsync({
      scenarioVersionId: planningData.scenarioVersionId,
      measureId,
      sourceCell: {
        storeId: scopeRoots[0].storeId,
        productNodeId: scopeRoots[0].productNodeId,
        timePeriodId: yearTimePeriodId,
      },
      totalValue,
      method: "seasonality_profile",
      roundingScale: measureLookup.get(measureId)?.decimalPlaces ?? 0,
      comment: "Spread annual value across months",
      manualWeights: buildSeasonalityWeights(planningData, yearTimePeriodId),
      scopeRoots,
    });
  };

  const handleAddRow = async (level: "store" | "department" | "class" | "subclass", parentRow: GridRow | null) => {
    if (!planningData) {
      return;
    }

    const label = window.prompt(`New ${level} name`);
    if (!label) {
      return;
    }

    let copyFromStoreId: number | null = null;
    let clusterLabel: string | null = null;
    let regionLabel: string | null = null;
    if (level === "store") {
      const stores = (storeViewData ?? activeGridData)?.rows.filter((row) => row.structureRole === "store") ?? [];
      const defaultStore = stores[0]?.label ?? "";
      const copyFromLabel = window.prompt("Copy hierarchy and data from store", defaultStore);
      if (!copyFromLabel) {
        return;
      }

      const sourceStore = stores.find((store) => store.label.toLowerCase() === copyFromLabel.trim().toLowerCase());
      if (!sourceStore) {
        setLastError(`Store '${copyFromLabel}' was not found to copy from.`);
        return;
      }

      copyFromStoreId = sourceStore.storeId;
      clusterLabel = window.prompt("Cluster label", sourceStore.clusterLabel) ?? sourceStore.clusterLabel;
      regionLabel = window.prompt("Region label", sourceStore.regionLabel) ?? sourceStore.regionLabel;
    }

    const createdRow = await addRowMutation.mutateAsync({
      scenarioVersionId: planningData.scenarioVersionId,
      level,
      parentProductNodeId: level === "store" ? null : parentRow?.bindingProductNodeId ?? null,
      label,
      copyFromStoreId,
      clusterLabel,
      regionLabel,
    });

    if (level === "store") {
      setActiveView("planning-store");
      setSelectedPlanningStoreId(createdRow.storeId);
    }

    setPendingRevealRow(createdRow);
  };

  const handleScopeRowClick = (row: GridRow) => {
    if (activeView === "planning-store" && row.structureRole === "store" && row.level === 1) {
      if (row.storeId !== selectedPlanningStoreId || !expandAllBranches) {
        setSelectedPlanningStoreId(row.storeId);
        setExpandAllBranches(true);
      }
      return;
    }

    if (activeView === "planning-department" && row.structureRole === "department" && row.level === 1) {
      if (row.label !== selectedDepartmentLabel || !expandAllBranches) {
        setSelectedDepartmentLabel(row.label);
        setExpandAllBranches(true);
      }
    }
  };

  const ensureBranchLoaded = (row: GridRow) => {
    if (row.isLeaf || row.structureRole === "virtual" || !row.bindingProductNodeId) {
      return;
    }

    if (expandAllBranches) {
      return;
    }

    setExpandedBranchNodeIds((current) => (
      current.includes(row.bindingProductNodeId!)
        ? current
        : [...current, row.bindingProductNodeId!].sort((left, right) => left - right)
    ));
  };

  const handleExpandAllBranches = () => {
    setExpandAllBranches(true);
  };

  const handleCollapseAllBranches = () => {
    setExpandAllBranches(false);
    setExpandedBranchNodeIds([]);
  };

  const handleDeleteRow = async (row: GridRow | null) => {
    if (!planningData) {
      return;
    }

    if (!row?.bindingProductNodeId) {
      setLastError("Select a Store, Department, or Class row to delete.");
      return;
    }

    if (!window.confirm(`Delete '${row.label}' and its related data? Deletes cannot be undone.`)) {
      return;
    }

    await deleteRowMutation.mutateAsync({
      scenarioVersionId: planningData.scenarioVersionId,
      productNodeId: row.bindingProductNodeId,
    });
  };

  const handleDeleteYear = async (yearTimePeriodId: number | null) => {
    if (!planningData) {
      return;
    }

    if (!yearTimePeriodId) {
      setLastError("Select a year to delete.");
      return;
    }

    const label = yearPeriods.find((period) => period.timePeriodId === yearTimePeriodId)?.label ?? String(yearTimePeriodId);
    if (!window.confirm(`Delete '${label}' and all related data? Deletes cannot be undone.`)) {
      return;
    }

    await deleteYearMutation.mutateAsync({
      scenarioVersionId: planningData.scenarioVersionId,
      yearTimePeriodId,
    });
  };

  const handleGenerateNextYear = async (yearTimePeriodId: number | null) => {
    if (!planningData) {
      return;
    }

    if (!yearTimePeriodId) {
      setLastError("Select an active year to generate the following year.");
      return;
    }

    const label = yearPeriods.find((period) => period.timePeriodId === yearTimePeriodId)?.label ?? String(yearTimePeriodId);
    if (!window.confirm(`Generate the following year from '${label}' by copying editable inputs only?`)) {
      return;
    }

    await generateNextYearMutation.mutateAsync({
      scenarioVersionId: planningData.scenarioVersionId,
      sourceYearTimePeriodId: yearTimePeriodId,
    });
  };

  const handleImportWorkbook = async (file: File) => {
    if (!planningData) {
      return;
    }

    await importMutation.mutateAsync({
      file,
      scenarioVersionId: planningData.scenarioVersionId,
    });
  };

  const handleApplyGrowthFactor = async (row: GridRow, timePeriodId: number, measureId: number, growthFactor: number, currentValue: number) => {
    if (!planningData) {
      return;
    }

    const scopeRoots = row.splashRoots ?? [];
    const isLeafMonth = row.isLeaf && planningData.periods.some((period) => period.timePeriodId === timePeriodId && period.grain === "month");
    const sourceStoreId = isLeafMonth ? (row.bindingStoreId ?? row.storeId) : (scopeRoots[0]?.storeId ?? row.storeId);
    const sourceProductNodeId = isLeafMonth ? (row.bindingProductNodeId ?? row.productNodeId) : (scopeRoots[0]?.productNodeId ?? row.productNodeId);

    if (!isLeafMonth && scopeRoots.length === 0) {
      setLastError("Growth factor is not available for this subtotal in the current view.");
      return;
    }

    await growthFactorMutation.mutateAsync({
      scenarioVersionId: planningData.scenarioVersionId,
      measureId,
      sourceCell: {
        storeId: sourceStoreId,
        productNodeId: sourceProductNodeId,
        timePeriodId,
      },
      currentValue,
      growthFactor,
      comment: "Apply growth factor",
      scopeRoots: isLeafMonth ? undefined : scopeRoots,
    });
  };

  const handleAddHierarchyDepartment = async () => {
    const departmentLabel = window.prompt("New department name");
    if (!departmentLabel) {
      return;
    }

    await addHierarchyDepartmentMutation.mutateAsync(departmentLabel);
  };

  const handleAddHierarchyClass = async (departmentLabel: string) => {
    const classLabel = window.prompt(`New class name for ${departmentLabel}`);
    if (!classLabel) {
      return;
    }

    await addHierarchyClassMutation.mutateAsync({ departmentLabel, classLabel });
  };

  const handleAddHierarchySubclass = async (departmentLabel: string, classLabel: string) => {
    const subclassLabel = window.prompt(`New subclass name for ${classLabel}`);
    if (!subclassLabel) {
      return;
    }

    await addHierarchySubclassMutation.mutateAsync({ departmentLabel, classLabel, subclassLabel });
  };

  const handleSaveStoreProfile = async (store: StoreProfile) => {
    const request: UpsertStoreProfileRequest = {
      scenarioVersionId: 1,
      storeId: store.storeId || null,
      storeCode: store.storeCode,
      branchName: store.branchName,
      state: store.state ?? null,
      clusterLabel: store.clusterLabel,
      latitude: store.latitude ?? null,
      longitude: store.longitude ?? null,
      regionLabel: store.regionLabel,
      openingDate: store.openingDate ?? null,
      sssg: store.sssg ?? null,
      salesType: store.salesType ?? null,
      status: store.status ?? null,
      storey: store.storey ?? null,
      buildingStatus: store.buildingStatus ?? null,
      gta: store.gta ?? null,
      nta: store.nta ?? null,
      rsom: store.rsom ?? null,
      dm: store.dm ?? null,
      rental: store.rental ?? null,
      lifecycleState: store.lifecycleState,
      rampProfileCode: store.rampProfileCode ?? null,
      isActive: store.isActive,
    };
    await upsertStoreProfileMutation.mutateAsync(request);
  };

  const handleDeleteStoreProfile = async (store: StoreProfile) => {
    if (!window.confirm(`Delete '${store.branchName}' and all related planning data? Deletes cannot be undone.`)) {
      return;
    }

    await deleteStoreProfileMutation.mutateAsync({ scenarioVersionId: 1, storeId: store.storeId });
  };

  const handleInactivateStoreProfile = async (store: StoreProfile) => {
    if (!window.confirm(`Inactivate '${store.branchName}'?`)) {
      return;
    }

    await inactivateStoreProfileMutation.mutateAsync(store.storeId);
  };

  const handleImportStoreProfiles = async (file: File) => {
    await storeProfileImportMutation.mutateAsync(file);
  };

  const handleExportStoreProfiles = async () => {
    await storeProfileExportMutation.mutateAsync();
  };

  const handleSaveProductProfile = async (profile: ProductProfile) => {
    const request: UpsertProductProfileRequest = { ...profile };
    await upsertProductProfileMutation.mutateAsync(request);
  };

  const handleDeleteProductProfile = async (profile: ProductProfile) => {
    if (!window.confirm(`Delete SKU '${profile.skuVariant}'? Deletes cannot be undone.`)) {
      return;
    }

    await deleteProductProfileMutation.mutateAsync(profile.skuVariant);
  };

  const handleInactivateProductProfile = async (profile: ProductProfile) => {
    if (!window.confirm(`Inactivate SKU '${profile.skuVariant}'?`)) {
      return;
    }

    await inactivateProductProfileMutation.mutateAsync(profile.skuVariant);
  };

  const handleImportProductProfiles = async (file: File) => {
    await productProfileImportMutation.mutateAsync(file);
    setProductPageNumber(1);
  };

  const handleExportProductProfiles = async () => {
    await productProfileExportMutation.mutateAsync();
  };

  const handleSaveProductHierarchy = async (row: ProductHierarchyCatalog) => {
    await productHierarchyMutation.mutateAsync(row);
  };

  const handleDeleteProductHierarchy = async (row: ProductHierarchyCatalog) => {
    if (!window.confirm(`Delete Department/Class '${row.department} / ${row.class}' and related product mappings? Deletes cannot be undone.`)) {
      return;
    }

    await deleteProductHierarchyMutation.mutateAsync({ dptNo: row.dptNo, clssNo: row.clssNo });
  };

  return (
    <main className="app-shell">
      <section className="hero">
        <div>
          <div className="eyebrow">Enterprise planning skeleton</div>
          <h1>Sales Budget & Planning</h1>
          <p>
            Excel-like planning with store-first and department-first sheets backed by the same live planning data.
          </p>
        </div>
        <div className="hero-sidecar">
          {authEnabled ? <SignedInUserMenu /> : null}
          <div className={`status-card${lastError ? " status-card-error" : ""}`} aria-live="polite">
            {statusText}
          </div>
        </div>
      </section>

      <div className="view-menu-bar">
        <label className="year-picker">
          <span>Workspace</span>
          <select value={activeView} onChange={(event) => setActiveView(event.target.value as ActiveView)}>
            <option value="planning-store">Planning - by Store</option>
            <option value="planning-department">Planning - by Department</option>
            <option value="hierarchy">Hierarchy Maintenance</option>
            <option value="store-profile">Store Profile Maintenance</option>
            <option value="product-profile">Product Profile Maintenance</option>
          </select>
        </label>
        {activeView === "planning-store" ? (
          <label className="year-picker">
            <span>Store Scope</span>
            <select value={selectedPlanningStoreId ?? ""} onChange={(event) => setSelectedPlanningStoreId(Number(event.target.value) || null)}>
              {(planningStoreScopeQuery.data?.stores ?? []).map((store: PlanningStoreScope) => (
                <option key={store.storeId} value={store.storeId}>
                  {store.branchName}
                </option>
              ))}
            </select>
          </label>
        ) : null}
        {activeView === "planning-department" ? (
          <label className="year-picker">
            <span>Department Scope</span>
            <select value={selectedDepartmentLabel ?? ""} onChange={(event) => setSelectedDepartmentLabel(event.target.value || null)}>
              {(departmentViewData?.rows ?? [])
                .filter((row) => row.structureRole === "department" && row.level === 1)
                .map((row) => (
                  <option key={row.label} value={row.label}>
                    {row.label}
                  </option>
                ))}
            </select>
          </label>
        ) : null}
        {activeView === "planning-department" ? (
          <label className="year-picker">
            <span>Layout</span>
            <select value={departmentLayout} onChange={(event) => setDepartmentLayout(event.target.value as DepartmentLayout)}>
              <option value="department-store-class">Department - Store - Class - Subclass</option>
              <option value="department-class-store">Department - Class - Store - Subclass</option>
            </select>
          </label>
        ) : null}
      </div>

      {activeView === "planning-store" || activeView === "planning-department" ? (
      <div className="layout-switcher" role="group" aria-label="Sheet actions">
        <label className="year-picker">
          <span>Active Year</span>
          <select value={selectedYearId ?? ""} onChange={(event) => setSelectedYearId(Number(event.target.value) || null)}>
            {yearPeriods.map((period) => (
              <option key={period.timePeriodId} value={period.timePeriodId}>
                {period.label}
              </option>
            ))}
          </select>
        </label>
        <button type="button" className="secondary-button" onClick={() => void exportMutation.mutateAsync()}>
          Export Workbook
        </button>
        <button type="button" className="secondary-button" onClick={() => void handleGenerateNextYear(selectedYearId)}>
          Generate Next Year
        </button>
        <button
          type="button"
          className="secondary-button"
          disabled={!hasUnsavedChanges || saveMutation.isPending}
          onClick={() => void saveMutation.mutateAsync({ mode: "manual" })}
        >
          Save
        </button>
        <button type="button" className="secondary-button danger-button" onClick={() => void handleDeleteYear(selectedYearId)}>
          Delete Year
        </button>
      </div>
      ) : null}

      {activeView === "hierarchy" ? (
        <HierarchyMaintenanceSheet
          departments={hierarchyQuery.data!.departments}
          onAddDepartment={handleAddHierarchyDepartment}
          onAddClass={handleAddHierarchyClass}
          onAddSubclass={handleAddHierarchySubclass}
        />
      ) : activeView === "store-profile" ? (
        <StoreProfileMaintenanceSheet
          stores={storeProfileQuery.data!.stores}
          options={storeProfileOptionsQuery.data!.options}
          onSave={handleSaveStoreProfile}
          onDelete={handleDeleteStoreProfile}
          onInactivate={handleInactivateStoreProfile}
          onImport={handleImportStoreProfiles}
          onExport={handleExportStoreProfiles}
          onUpsertOption={async (fieldName, value, isActive) => {
            await storeProfileOptionMutation.mutateAsync({ fieldName, value, isActive });
          }}
          onDeleteOption={async (fieldName, value) => {
            await deleteStoreProfileOptionMutation.mutateAsync({ fieldName, value });
          }}
        />
      ) : activeView === "product-profile" ? (
        <ProductProfileMaintenanceSheet
          profiles={productProfileQuery.data!.profiles}
          totalCount={productProfileQuery.data!.totalCount}
          pageNumber={productProfileQuery.data!.pageNumber}
          pageSize={productProfileQuery.data!.pageSize}
          searchTerm={productSearchTerm}
          hierarchyRows={productHierarchyQuery.data!.hierarchyRows}
          subclassRows={productHierarchyQuery.data!.subclassRows}
          options={productProfileOptionsQuery.data!.options}
          onSearchChange={(value) => {
            setProductSearchTerm(value);
            setProductPageNumber(1);
          }}
          onPageChange={setProductPageNumber}
          onSave={handleSaveProductProfile}
          onDelete={handleDeleteProductProfile}
          onInactivate={handleInactivateProductProfile}
          onImport={handleImportProductProfiles}
          onExport={handleExportProductProfiles}
          onUpsertOption={async (fieldName, value, isActive) => {
            await productProfileOptionMutation.mutateAsync({ fieldName, value, isActive });
          }}
          onDeleteOption={async (fieldName, value) => {
            await deleteProductProfileOptionMutation.mutateAsync({ fieldName, value });
          }}
          onSaveHierarchy={handleSaveProductHierarchy}
          onDeleteHierarchy={handleDeleteProductHierarchy}
        />
      ) : (
        <div className="planning-workspace">
          <Suspense
            fallback={
              <section className="planning-shell planning-shell-loading" aria-live="polite">
                Preparing planning grid...
              </section>
            }
          >
            <PlanningGrid
              data={activeGridData!}
              selectedYearId={selectedYearId}
              onSelectedYearChange={setSelectedYearId}
              onSelectionContextChange={setInsightScope}
              onCellEdit={handleCellEdit}
              onApplyGrowthFactor={handleApplyGrowthFactor}
              onToggleLock={handleToggleLock}
              onSplashYear={handleSplashYear}
              onAddRow={handleAddRow}
              onDeleteRow={handleDeleteRow}
              onImportWorkbook={handleImportWorkbook}
              onEnsureBranchLoaded={ensureBranchLoaded}
              onExpandAllBranches={handleExpandAllBranches}
              onCollapseAllBranches={handleCollapseAllBranches}
              expandAllBranches={expandAllBranches}
              onScopeRowClick={handleScopeRowClick}
              sheetLabel={activeView === "planning-store" ? "Planning - by Store" : "Planning - by Department"}
              pendingRevealRow={pendingRevealRow}
              onRevealHandled={() => setPendingRevealRow(null)}
            />
          </Suspense>
          <aside className="insight-panel" aria-live="polite">
            <div className="eyebrow">Planning intelligence</div>
            <h2>Forecast & pricing insight</h2>
            {insightScope ? (
              insightQuery.isLoading ? (
                <p>Refreshing demand and GP recommendations...</p>
              ) : insightQuery.data ? (
                <InsightPanelContent insight={insightQuery.data} />
              ) : (
                <p>Select a bound Store, Department, Class, or Subclass branch to load targeted recommendations.</p>
              )
            ) : (
              <p>Select a single planning branch to review forecast model, seasonality, price bands, and GP opportunity.</p>
            )}
          </aside>
        </div>
      )}
    </main>
  );
}

function InsightPanelContent({ insight }: { insight: PlanningInsightResponse }) {
  return (
    <>
      <p className="insight-scope">{insight.scopeLabel}</p>
      <div className="insight-stat-grid">
        <div className="insight-stat">
          <span className="eyebrow">Forecast model</span>
          <strong>{insight.recommendedForecastModel}</strong>
        </div>
        <div className="insight-stat">
          <span className="eyebrow">Seasonality</span>
          <strong>{insight.seasonalityStrength.toFixed(1)}</strong>
        </div>
        <div className="insight-stat">
          <span className="eyebrow">Price band</span>
          <strong>
            {insight.recommendedPriceFloor.toFixed(2)} / {insight.recommendedPriceTarget.toFixed(2)} / {insight.recommendedPriceCeiling.toFixed(2)}
          </strong>
        </div>
        <div className="insight-stat">
          <span className="eyebrow">GP opportunity</span>
          <strong>{Math.round(insight.grossProfitOpportunity).toLocaleString()}</strong>
        </div>
        <div className="insight-stat">
          <span className="eyebrow">Qty opportunity</span>
          <strong>{Math.round(insight.quantityOpportunity).toLocaleString()}</strong>
        </div>
        <div className="insight-stat">
          <span className="eyebrow">Provider</span>
          <strong>{insight.providerStatus}</strong>
        </div>
      </div>
      <ul className="insight-list">
        {insight.insightBullets.map((bullet) => (
          <li key={bullet}>{bullet}</li>
        ))}
      </ul>
    </>
  );
}

function buildStoreView(data: GridSliceResponse): GridSliceResponse {
  const rootLabel = "Store Total";
  const canonicalRows = buildCanonicalRows(data.rows);
  const directRows: GridRow[] = canonicalRows.map((row) => ({
    ...row,
    viewRowId: `store-view:${row.storeId}:${row.productNodeId}`,
    level: row.level + 1,
    path: [rootLabel, ...row.path],
  }));
  const storeRows = directRows.filter((row) => row.structureRole === "store");

  return {
    ...data,
    rows: [
      createSyntheticRow({
        data,
        storeId: 0,
        productNodeId: -10,
        label: rootLabel,
        path: [rootLabel],
        structureRole: "store",
        rows: storeRows,
        splashRoots: storeRows.map(toSplashRoot),
      }),
      ...directRows,
    ],
  };
}

function buildDepartmentView(data: GridSliceResponse, layout: DepartmentLayout): GridSliceResponse {
  const canonicalRows = buildCanonicalRows(data.rows);
  const storeRows = canonicalRows.filter((row) => row.structureRole === "store");
  const departmentRows = canonicalRows.filter((row) => row.structureRole === "department");
  const classRows = canonicalRows.filter((row) => row.structureRole === "class");
  const subclassRows = canonicalRows.filter((row) => row.structureRole === "subclass");
  const storeLabels = new Map(storeRows.map((row) => [row.storeId, row.storeLabel]));
  let syntheticRowSeed = -1;

  const departmentGroups = new Map<string, { departmentRows: GridRow[]; classRows: GridRow[]; subclassRows: GridRow[] }>();
  for (const departmentRow of departmentRows) {
    const departmentLabel = getDepartmentLabel(departmentRow);
    const group = departmentGroups.get(departmentLabel) ?? { departmentRows: [], classRows: [], subclassRows: [] };
    group.departmentRows.push(departmentRow);
    departmentGroups.set(departmentLabel, group);
  }

  for (const classRow of classRows) {
    const departmentLabel = getDepartmentLabel(classRow);
    const group = departmentGroups.get(departmentLabel) ?? { departmentRows: [], classRows: [], subclassRows: [] };
    group.classRows.push(classRow);
    departmentGroups.set(departmentLabel, group);
  }

  for (const subclassRow of subclassRows) {
    const departmentLabel = getDepartmentLabel(subclassRow);
    const group = departmentGroups.get(departmentLabel) ?? { departmentRows: [], classRows: [], subclassRows: [] };
    group.subclassRows.push(subclassRow);
    departmentGroups.set(departmentLabel, group);
  }

  const rows: GridRow[] = [];
  rows.push(createSyntheticRow({
    data,
    storeId: 0,
    productNodeId: syntheticRowSeed--,
    label: "Department Total",
    path: ["Department Total"],
    structureRole: "department",
    rows: storeRows,
    splashRoots: storeRows.map(toSplashRoot),
  }));

  for (const [departmentLabel, group] of [...departmentGroups.entries()].sort(([left], [right]) => left.localeCompare(right))) {
    rows.push(createSyntheticRow({
      data,
      storeId: 0,
      productNodeId: syntheticRowSeed--,
      label: departmentLabel,
      path: ["Department Total", departmentLabel],
      structureRole: "department",
      rows: group.departmentRows,
      splashRoots: group.departmentRows.map(toSplashRoot),
    }));

    if (layout === "department-store-class") {
      for (const departmentRow of [...group.departmentRows].sort((left, right) => {
        const leftLabel = storeLabels.get(left.storeId) ?? "";
        const rightLabel = storeLabels.get(right.storeId) ?? "";
        return leftLabel.localeCompare(rightLabel);
      })) {
        const storeLabel = storeLabels.get(departmentRow.storeId) ?? `Store ${departmentRow.storeId}`;
        rows.push({
          ...departmentRow,
          viewRowId: `department-view:${layout}:${departmentLabel}:${storeLabel}`,
          label: storeLabel,
          level: 2,
          path: ["Department Total", departmentLabel, storeLabel],
          structureRole: "store" as const,
        });

        const matchingClasses = group.classRows
          .filter((classRow) => classRow.storeId === departmentRow.storeId && getDepartmentLabel(classRow) === departmentLabel)
          .sort((left, right) => left.label.localeCompare(right.label));

        for (const classRow of matchingClasses) {
          rows.push({
            ...classRow,
            viewRowId: `department-view:${layout}:${departmentLabel}:${storeLabel}:${classRow.label}`,
            level: 3,
            path: ["Department Total", departmentLabel, storeLabel, classRow.label],
            structureRole: "class" as const,
          });

          const matchingSubclasses = group.subclassRows
            .filter((subclassRow) => subclassRow.storeId === departmentRow.storeId && getDepartmentLabel(subclassRow) === departmentLabel && getClassLabel(subclassRow) === classRow.label)
            .sort((left, right) => left.label.localeCompare(right.label));

          rows.push(...matchingSubclasses.map((subclassRow) => ({
            ...subclassRow,
            viewRowId: `department-view:${layout}:${departmentLabel}:${storeLabel}:${classRow.label}:${subclassRow.label}`,
            level: 4,
            path: ["Department Total", departmentLabel, storeLabel, classRow.label, subclassRow.label],
            structureRole: "subclass" as const,
          })));
        }
      }

      continue;
    }

    const classGroups = new Map<string, GridRow[]>();
    for (const classRow of group.classRows) {
      const classLabel = classRow.label;
      const classGroup = classGroups.get(classLabel) ?? [];
      classGroup.push(classRow);
      classGroups.set(classLabel, classGroup);
    }

    for (const [classLabel, groupedClassRows] of [...classGroups.entries()].sort(([left], [right]) => left.localeCompare(right))) {
      rows.push(createSyntheticRow({
        data,
        storeId: 0,
        productNodeId: syntheticRowSeed--,
        label: classLabel,
        path: ["Department Total", departmentLabel, classLabel],
        structureRole: "class",
        rows: groupedClassRows,
      splashRoots: groupedClassRows.map(toSplashRoot),
        bindingStoreId: groupedClassRows.length === 1 ? groupedClassRows[0].bindingStoreId ?? groupedClassRows[0].storeId : null,
        bindingProductNodeId: groupedClassRows.length === 1 ? groupedClassRows[0].bindingProductNodeId ?? groupedClassRows[0].productNodeId : null,
      }));

      for (const classRow of [...groupedClassRows].sort((left, right) => {
        const leftLabel = storeLabels.get(left.storeId) ?? "";
        const rightLabel = storeLabels.get(right.storeId) ?? "";
        return leftLabel.localeCompare(rightLabel);
      })) {
        const storeLabel = storeLabels.get(classRow.storeId) ?? `Store ${classRow.storeId}`;
        rows.push({
          ...classRow,
          viewRowId: `department-view:${layout}:${departmentLabel}:${classLabel}:${storeLabel}`,
          label: storeLabel,
          level: 3,
          path: ["Department Total", departmentLabel, classLabel, storeLabel],
          structureRole: "store" as const,
        });

        const matchingSubclasses = group.subclassRows
          .filter((subclassRow) => subclassRow.storeId === classRow.storeId && getDepartmentLabel(subclassRow) === departmentLabel && getClassLabel(subclassRow) === classLabel)
          .sort((left, right) => left.label.localeCompare(right.label));

        rows.push(...matchingSubclasses.map((subclassRow) => ({
          ...subclassRow,
          viewRowId: `department-view:${layout}:${departmentLabel}:${classLabel}:${storeLabel}:${subclassRow.label}`,
          level: 4,
          path: ["Department Total", departmentLabel, classLabel, storeLabel, subclassRow.label],
          structureRole: "subclass" as const,
        })));
      }
    }
  }

  return {
    ...data,
    rows,
  };
}

function buildCanonicalRows(rows: GridRow[]): GridRow[] {
  return rows.map((row) => {
    const structureRole: NonNullable<GridRow["structureRole"]> =
      row.nodeKind === "store" || row.nodeKind === "department" || row.nodeKind === "class" || row.nodeKind === "subclass"
        ? row.nodeKind
        : "virtual";

    return {
      ...row,
      structureRole,
      bindingStoreId: row.storeId,
      bindingProductNodeId: row.productNodeId,
      splashRoots: [{ storeId: row.storeId, productNodeId: row.productNodeId }],
    };
  });
}

function getDepartmentLabel(row: GridRow): string {
  if (row.structureRole === "department") {
    return row.label;
  }

  return row.path[1] ?? row.label;
}

function getClassLabel(row: GridRow): string {
  if (row.structureRole === "class") {
    return row.label;
  }

  return row.path[2] ?? row.label;
}

function createSyntheticRow({
  data,
  storeId,
  productNodeId,
  label,
  path,
  structureRole,
  rows,
  splashRoots,
  bindingStoreId = null,
  bindingProductNodeId = null,
}: {
  data: GridSliceResponse;
  storeId: number;
  productNodeId: number;
  label: string;
  path: string[];
  structureRole: NonNullable<GridRow["structureRole"]>;
  rows: GridRow[];
  splashRoots: Array<{ storeId: number; productNodeId: number }>;
  bindingStoreId?: number | null;
  bindingProductNodeId?: number | null;
}): GridRow {
  return {
    storeId,
    productNodeId,
    viewRowId: `synthetic:${path.join(">")}:${productNodeId}`,
    label,
    level: path.length - 1,
    path,
    isLeaf: false,
    nodeKind: "virtual",
    storeLabel: "All Stores",
    clusterLabel: rows[0]?.clusterLabel ?? "Mixed Cluster",
    regionLabel: rows[0]?.regionLabel ?? "Mixed Region",
    lifecycleState: "synthetic",
    rampProfileCode: null,
    effectiveFromTimePeriodId: null,
    effectiveToTimePeriodId: null,
    structureRole,
    bindingStoreId,
    bindingProductNodeId,
    splashRoots,
    cells: sumCells(rows, data),
  };
}

function sumCells(rows: GridRow[], data: GridSliceResponse): Record<number, { measures: Record<number, GridCell> }> {
  return Object.fromEntries(
    data.periods.map((period) => {
      const revenueValue = rows.reduce((total, row) => total + (row.cells[period.timePeriodId]?.measures[1]?.value ?? 0), 0);
      const quantityValue = rows.reduce((total, row) => total + (row.cells[period.timePeriodId]?.measures[2]?.value ?? 0), 0);
      const totalCostsValue = rows.reduce((total, row) => total + (row.cells[period.timePeriodId]?.measures[5]?.value ?? 0), 0);
      const grossProfitValue = rows.reduce((total, row) => total + (row.cells[period.timePeriodId]?.measures[6]?.value ?? 0), 0);
      const aspValue = quantityValue > 0 ? roundToDecimals(revenueValue / quantityValue, 2) : 1;
      const unitCostValue = quantityValue > 0 ? roundToDecimals(totalCostsValue / quantityValue, 2) : 0;
      const grossProfitPercentValue = aspValue > 0 ? roundToDecimals(((aspValue - unitCostValue) / aspValue) * 100, 1) : 0;

      return [
        period.timePeriodId,
        {
          measures: Object.fromEntries(
            data.measures.map((measure) => {
              const value = (() => {
                switch (measure.measureId) {
                  case 1:
                    return revenueValue;
                  case 2:
                    return quantityValue;
                  case 3:
                    return aspValue;
                  case 4:
                    return unitCostValue;
                  case 5:
                    return totalCostsValue;
                  case 6:
                    return grossProfitValue;
                  case 7:
                    return grossProfitPercentValue;
                  default:
                    return 0;
                }
              })();
              const isLocked = rows.length > 0 && rows.every((row) => row.cells[period.timePeriodId]?.measures[measure.measureId]?.isLocked);
              const growthFactors = rows
                .map((row) => row.cells[period.timePeriodId]?.measures[measure.measureId]?.growthFactor ?? 1)
                .filter((value) => Number.isFinite(value));
              const uniformGrowthFactor = growthFactors.length > 0 && growthFactors.every((value) => value === growthFactors[0]) ? growthFactors[0] : 1;

              return [
                measure.measureId,
                {
                  value,
                  growthFactor: uniformGrowthFactor,
                  isLocked,
                  isCalculated: true,
                  isOverride: false,
                  rowVersion: 0,
                  cellKind: "calculated",
                } satisfies GridCell,
              ];
            }),
          ),
        },
      ];
    }),
  );
}

function toSplashRoot(row: GridRow): { storeId: number; productNodeId: number } {
  return {
    storeId: row.bindingStoreId ?? row.storeId,
    productNodeId: row.bindingProductNodeId ?? row.productNodeId,
  };
}

function buildSeasonalityWeights(data: GridSliceResponse, yearTimePeriodId: number): Record<number, number> {
  const monthWeights = [8, 12, 7, 7, 8, 8, 9, 9, 8, 7, 8, 9];
  const monthPeriods = data.periods
    .filter((period) => period.parentTimePeriodId === yearTimePeriodId)
    .sort((left, right) => left.sortOrder - right.sortOrder);

  return Object.fromEntries(monthPeriods.map((period, index) => [period.timePeriodId, monthWeights[index] ?? 1]));
}

function roundToDecimals(value: number, decimals: number): number {
  const scale = 10 ** decimals;
  return Math.round(value * scale) / scale;
}
