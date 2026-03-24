import { Suspense, lazy, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  downloadBase64Workbook,
  downloadWorkbookExport,
  getGridSlice,
  getHierarchyMappings,
  postAddRow,
  postDeleteRow,
  postDeleteYear,
  postEdit,
  postGenerateNextYear,
  postGrowthFactor,
  postHierarchyClass,
  postHierarchyDepartment,
  postLock,
  postSave,
  postSplash,
  postWorkbookImport,
} from "./lib/api";
import { HierarchyMaintenanceSheet } from "./components/HierarchyMaintenanceSheet";
import type { GridCell, GridMeasure, GridRow, GridSliceResponse } from "./lib/types";

const preloadPlanningGrid = () => import("./components/PlanningGrid");

const PlanningGrid = lazy(async () => {
  const module = await preloadPlanningGrid();
  return { default: module.PlanningGrid };
});

type ActiveView = "planning-store" | "planning-department" | "hierarchy";
type DepartmentLayout = "department-store-class" | "department-class-store";

export default function App() {
  const queryClient = useQueryClient();
  const [lastError, setLastError] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<ActiveView>("planning-store");
  const [departmentLayout, setDepartmentLayout] = useState<DepartmentLayout>("department-store-class");
  const [selectedYearId, setSelectedYearId] = useState<number | null>(null);
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
  const [lastSavedAt, setLastSavedAt] = useState<string | null>(null);

  useEffect(() => {
    void preloadPlanningGrid();
  }, []);

  const gridQuery = useQuery({
    queryKey: ["grid-slice", 1],
    queryFn: () => getGridSlice(),
  });
  const hierarchyQuery = useQuery({
    queryKey: ["hierarchy-mappings"],
    queryFn: getHierarchyMappings,
  });

  useEffect(() => {
    if (selectedYearId || !gridQuery.data) {
      return;
    }

    const firstYear = gridQuery.data.periods.find((period) => period.grain === "year");
    setSelectedYearId(firstYear?.timePeriodId ?? null);
  }, [gridQuery.data, selectedYearId]);

  const refresh = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["grid-slice", 1] }),
      queryClient.invalidateQueries({ queryKey: ["hierarchy-mappings"] }),
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
    addHierarchyClassMutation.isPending;

  const statusText = useMemo(() => {
    if (gridQuery.isLoading || hierarchyQuery.isLoading) {
      return "Loading planning slice...";
    }

    if (isMutating) {
      return "Applying changes...";
    }

    if (gridQuery.isError || hierarchyQuery.isError) {
      return "API unavailable.";
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
      : "Multi-year planning grid ready.";
  }, [activeView, gridQuery.isError, gridQuery.isLoading, hasUnsavedChanges, hierarchyQuery.isError, hierarchyQuery.isLoading, isMutating, lastError, lastSavedAt]);

  const storeViewData = useMemo(
    () => (gridQuery.data ? buildStoreView(gridQuery.data) : null),
    [gridQuery.data],
  );
  const departmentViewData = useMemo(
    () => (storeViewData ? buildDepartmentView(storeViewData, departmentLayout) : null),
    [departmentLayout, storeViewData],
  );

  if (!gridQuery.data || !hierarchyQuery.data || !storeViewData || !departmentViewData) {
    return (
      <main className="app-shell">
        <section className="hero">
          <h1>Sales Budget & Planning</h1>
          <p>{statusText}</p>
        </section>
      </main>
    );
  }

  const activeGridData = activeView === "planning-department" ? departmentViewData : storeViewData;
  const yearPeriods = gridQuery.data.periods.filter((period) => period.grain === "year");
  const measureLookup = new Map(gridQuery.data.measures.map((measure) => [measure.measureId, measure]));

  const handleCellEdit = async (row: GridRow, timePeriodId: number, measureId: number, newValue: number) => {
    const measureCell = row.cells[timePeriodId]?.measures[measureId];
    const period = gridQuery.data.periods.find((item) => item.timePeriodId === timePeriodId);
    const isLeafMonth = row.structureRole === "class" && period?.grain === "month";
    const scopeRoots = row.splashRoots ?? [];

    if (isLeafMonth && row.bindingProductNodeId) {
      await editMutation.mutateAsync({
        scenarioVersionId: gridQuery.data.scenarioVersionId,
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
      scenarioVersionId: gridQuery.data.scenarioVersionId,
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
      manualWeights: period?.grain === "year" ? buildSeasonalityWeights(gridQuery.data, timePeriodId) : undefined,
      scopeRoots,
    });
  };

  const handleToggleLock = async (row: GridRow, timePeriodId: number, measureId: number, locked: boolean) => {
    const scopeRoots = row.splashRoots ?? [];
    if (scopeRoots.length === 0) {
      setLastError("This row cannot be locked in the current view.");
      return;
    }

    await lockMutation.mutateAsync({
      scenarioVersionId: gridQuery.data.scenarioVersionId,
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
    const scopeRoots = row.splashRoots ?? [];
    if (scopeRoots.length === 0) {
      setLastError("This row cannot be splashed in the current view.");
      return;
    }

    const totalValue = row.cells[yearTimePeriodId]?.measures[measureId]?.value ?? 0;
    await splashMutation.mutateAsync({
      scenarioVersionId: gridQuery.data.scenarioVersionId,
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
      manualWeights: buildSeasonalityWeights(gridQuery.data, yearTimePeriodId),
      scopeRoots,
    });
  };

  const handleAddRow = async (level: "store" | "department" | "class", parentRow: GridRow | null) => {
    const label = window.prompt(`New ${level} name`);
    if (!label) {
      return;
    }

    let copyFromStoreId: number | null = null;
    if (level === "store") {
      const stores = storeViewData.rows.filter((row) => row.structureRole === "store");
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
    }

    await addRowMutation.mutateAsync({
      scenarioVersionId: gridQuery.data.scenarioVersionId,
      level,
      parentProductNodeId: level === "store" ? null : parentRow?.bindingProductNodeId ?? null,
      label,
      copyFromStoreId,
    });
  };

  const handleDeleteRow = async (row: GridRow | null) => {
    if (!row?.bindingProductNodeId) {
      setLastError("Select a Store, Department, or Class row to delete.");
      return;
    }

    if (!window.confirm(`Delete '${row.label}' and its related data? Deletes cannot be undone.`)) {
      return;
    }

    await deleteRowMutation.mutateAsync({
      scenarioVersionId: gridQuery.data.scenarioVersionId,
      productNodeId: row.bindingProductNodeId,
    });
  };

  const handleDeleteYear = async (yearTimePeriodId: number | null) => {
    if (!yearTimePeriodId) {
      setLastError("Select a year to delete.");
      return;
    }

    const label = yearPeriods.find((period) => period.timePeriodId === yearTimePeriodId)?.label ?? String(yearTimePeriodId);
    if (!window.confirm(`Delete '${label}' and all related data? Deletes cannot be undone.`)) {
      return;
    }

    await deleteYearMutation.mutateAsync({
      scenarioVersionId: gridQuery.data.scenarioVersionId,
      yearTimePeriodId,
    });
  };

  const handleGenerateNextYear = async (yearTimePeriodId: number | null) => {
    if (!yearTimePeriodId) {
      setLastError("Select an active year to generate the following year.");
      return;
    }

    const label = yearPeriods.find((period) => period.timePeriodId === yearTimePeriodId)?.label ?? String(yearTimePeriodId);
    if (!window.confirm(`Generate the following year from '${label}' by copying editable inputs only?`)) {
      return;
    }

    await generateNextYearMutation.mutateAsync({
      scenarioVersionId: gridQuery.data.scenarioVersionId,
      sourceYearTimePeriodId: yearTimePeriodId,
    });
  };

  const handleImportWorkbook = async (file: File) => {
    await importMutation.mutateAsync({
      file,
      scenarioVersionId: gridQuery.data.scenarioVersionId,
    });
  };

  const handleApplyGrowthFactor = async (row: GridRow, timePeriodId: number, measureId: number, growthFactor: number, currentValue: number) => {
    const scopeRoots = row.splashRoots ?? [];
    const isLeafMonth = row.structureRole === "class" && gridQuery.data.periods.some((period) => period.timePeriodId === timePeriodId && period.grain === "month");
    const sourceStoreId = isLeafMonth ? (row.bindingStoreId ?? row.storeId) : (scopeRoots[0]?.storeId ?? row.storeId);
    const sourceProductNodeId = isLeafMonth ? (row.bindingProductNodeId ?? row.productNodeId) : (scopeRoots[0]?.productNodeId ?? row.productNodeId);

    if (!isLeafMonth && scopeRoots.length === 0) {
      setLastError("Growth factor is not available for this subtotal in the current view.");
      return;
    }

    await growthFactorMutation.mutateAsync({
      scenarioVersionId: gridQuery.data.scenarioVersionId,
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
        <div className={`status-card${lastError ? " status-card-error" : ""}`} aria-live="polite">
          {statusText}
        </div>
      </section>

      <div className="view-switcher" role="tablist" aria-label="Sheet navigation">
        <button
          type="button"
          className={`secondary-button${activeView === "planning-store" ? " secondary-button-active" : ""}`}
          onClick={() => setActiveView("planning-store")}
        >
          Planning - by Store
        </button>
        <button
          type="button"
          className={`secondary-button${activeView === "planning-department" ? " secondary-button-active" : ""}`}
          onClick={() => setActiveView("planning-department")}
        >
          Planning - by Department
        </button>
        <button
          type="button"
          className={`secondary-button${activeView === "hierarchy" ? " secondary-button-active" : ""}`}
          onClick={() => setActiveView("hierarchy")}
        >
          Hierarchy Maintenance
        </button>
      </div>

      {activeView === "planning-department" ? (
        <div className="layout-switcher" role="group" aria-label="Department planning layout">
          <button
            type="button"
            className={`secondary-button${departmentLayout === "department-store-class" ? " secondary-button-active" : ""}`}
            onClick={() => setDepartmentLayout("department-store-class")}
          >
            Department - Store - Class
          </button>
          <button
            type="button"
            className={`secondary-button${departmentLayout === "department-class-store" ? " secondary-button-active" : ""}`}
            onClick={() => setDepartmentLayout("department-class-store")}
          >
            Department - Class - Store
          </button>
        </div>
      ) : null}

      <div className="layout-switcher" role="group" aria-label="Year actions">
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

      {activeView === "hierarchy" ? (
        <HierarchyMaintenanceSheet
          departments={hierarchyQuery.data.departments}
          onAddDepartment={handleAddHierarchyDepartment}
          onAddClass={handleAddHierarchyClass}
        />
      ) : (
        <Suspense
          fallback={
            <section className="planning-shell planning-shell-loading" aria-live="polite">
              Preparing planning grid...
            </section>
          }
        >
          <PlanningGrid
            data={activeGridData}
            selectedYearId={selectedYearId}
            onSelectedYearChange={setSelectedYearId}
            onCellEdit={handleCellEdit}
            onApplyGrowthFactor={handleApplyGrowthFactor}
            onToggleLock={handleToggleLock}
            onSplashYear={handleSplashYear}
            onAddRow={handleAddRow}
            onDeleteRow={handleDeleteRow}
            onImportWorkbook={handleImportWorkbook}
            sheetLabel={activeView === "planning-store" ? "Planning - by Store" : "Planning - by Department"}
          />
        </Suspense>
      )}
    </main>
  );
}

function buildStoreView(data: GridSliceResponse): GridSliceResponse {
  const rootLabel = "Store Total";
  const directRows: GridRow[] = data.rows.map((row) => ({
    ...row,
    viewRowId: `store-view:${row.storeId}:${row.productNodeId}`,
    level: row.level + 1,
    path: [rootLabel, ...row.path],
    structureRole: (row.level === 0 ? "store" : row.level === 1 ? "department" : "class") as GridRow["structureRole"],
    bindingStoreId: row.storeId,
    bindingProductNodeId: row.productNodeId,
    splashRoots: [{ storeId: row.storeId, productNodeId: row.productNodeId }],
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
  const storeRows = data.rows.filter((row) => row.structureRole === "store");
  const departmentRows = data.rows.filter((row) => row.structureRole === "department");
  const classRows = data.rows.filter((row) => row.structureRole === "class");
  const storeLabels = new Map(storeRows.map((row) => [row.storeId, row.label]));
  let syntheticRowSeed = -1;

  const departmentGroups = new Map<string, { departmentRows: GridRow[]; classRows: GridRow[] }>();
  for (const departmentRow of departmentRows) {
    const departmentLabel = departmentRow.path[2];
    const group = departmentGroups.get(departmentLabel) ?? { departmentRows: [], classRows: [] };
    group.departmentRows.push(departmentRow);
    departmentGroups.set(departmentLabel, group);
  }

  for (const classRow of classRows) {
    const departmentLabel = classRow.path[2];
    const group = departmentGroups.get(departmentLabel) ?? { departmentRows: [], classRows: [] };
    group.classRows.push(classRow);
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
        });

        const matchingClasses = group.classRows
          .filter((classRow) => classRow.storeId === departmentRow.storeId)
          .sort((left, right) => left.label.localeCompare(right.label));

        rows.push(...matchingClasses.map((classRow) => ({
          ...classRow,
          viewRowId: `department-view:${layout}:${departmentLabel}:${storeLabel}:${classRow.label}`,
          level: 3,
          path: ["Department Total", departmentLabel, storeLabel, classRow.label],
        })));
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
        });
      }
    }
  }

  return {
    ...data,
    rows,
  };
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
}: {
  data: GridSliceResponse;
  storeId: number;
  productNodeId: number;
  label: string;
  path: string[];
  structureRole: NonNullable<GridRow["structureRole"]>;
  rows: GridRow[];
  splashRoots: Array<{ storeId: number; productNodeId: number }>;
}): GridRow {
  return {
    storeId,
    productNodeId,
    viewRowId: `synthetic:${path.join(">")}:${productNodeId}`,
    label,
    level: path.length - 1,
    path,
    isLeaf: false,
    structureRole,
    bindingStoreId: null,
    bindingProductNodeId: null,
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
