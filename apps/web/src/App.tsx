import { Suspense, lazy, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  getGridSlice,
  getHierarchyMappings,
  postAddRow,
  postEdit,
  postHierarchyCategory,
  postHierarchySubcategory,
  postLock,
  postSplash,
  postWorkbookImport,
} from "./lib/api";
import { HierarchyMaintenanceSheet } from "./components/HierarchyMaintenanceSheet";
import type { GridCell, GridRow, GridSliceResponse } from "./lib/types";

const preloadPlanningGrid = () => import("./components/PlanningGrid");

const PlanningGrid = lazy(async () => {
  const module = await preloadPlanningGrid();
  return { default: module.PlanningGrid };
});

const seasonalityWeights: Record<number, number> = {
  202601: 8,
  202602: 12,
  202603: 7,
  202604: 7,
  202605: 8,
  202606: 8,
  202607: 9,
  202608: 9,
  202609: 8,
  202610: 7,
  202611: 8,
  202612: 9,
};

type ActiveView = "planning-store" | "planning-category" | "hierarchy";
type CategoryLayout = "category-store-subcategory" | "category-subcategory-store";

export default function App() {
  const queryClient = useQueryClient();
  const [lastError, setLastError] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<ActiveView>("planning-store");
  const [categoryLayout, setCategoryLayout] = useState<CategoryLayout>("category-store-subcategory");

  useEffect(() => {
    void preloadPlanningGrid();
  }, []);

  const gridQuery = useQuery({
    queryKey: ["grid-slice", 1, 1],
    queryFn: getGridSlice,
  });
  const hierarchyQuery = useQuery({
    queryKey: ["hierarchy-mappings"],
    queryFn: getHierarchyMappings,
  });

  const refresh = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["grid-slice", 1, 1] }),
      queryClient.invalidateQueries({ queryKey: ["hierarchy-mappings"] }),
    ]);
  };

  const editMutation = useMutation({
    mutationFn: postEdit,
    onSuccess: async () => {
      setLastError(null);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const lockMutation = useMutation({
    mutationFn: postLock,
    onSuccess: async () => {
      setLastError(null);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const splashMutation = useMutation({
    mutationFn: postSplash,
    onSuccess: async () => {
      setLastError(null);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const addRowMutation = useMutation({
    mutationFn: postAddRow,
    onSuccess: async () => {
      setLastError(null);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const importMutation = useMutation({
    mutationFn: ({ file, scenarioVersionId, measureId }: { file: File; scenarioVersionId: number; measureId: number }) =>
      postWorkbookImport(scenarioVersionId, measureId, file),
    onSuccess: async () => {
      setLastError(null);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const addHierarchyCategoryMutation = useMutation({
    mutationFn: postHierarchyCategory,
    onSuccess: async () => {
      setLastError(null);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const addHierarchySubcategoryMutation = useMutation({
    mutationFn: ({ categoryLabel, subcategoryLabel }: { categoryLabel: string; subcategoryLabel: string }) =>
      postHierarchySubcategory(categoryLabel, subcategoryLabel),
    onSuccess: async () => {
      setLastError(null);
      await refresh();
    },
    onError: (error: Error) => setLastError(error.message),
  });

  const isMutating =
    editMutation.isPending ||
    lockMutation.isPending ||
    splashMutation.isPending ||
    addRowMutation.isPending ||
    importMutation.isPending ||
    addHierarchyCategoryMutation.isPending ||
    addHierarchySubcategoryMutation.isPending;

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

    return activeView === "hierarchy"
      ? "Hierarchy maintenance sheet ready."
      : "Lock-safe planning grid ready.";
  }, [activeView, gridQuery.isError, gridQuery.isLoading, hierarchyQuery.isError, hierarchyQuery.isLoading, isMutating, lastError]);

  const storeViewData = useMemo(
    () => (gridQuery.data ? buildStoreView(gridQuery.data) : null),
    [gridQuery.data],
  );
  const categoryViewData = useMemo(
    () => (storeViewData ? buildCategoryView(storeViewData, categoryLayout) : null),
    [categoryLayout, storeViewData],
  );

  if (!gridQuery.data || !hierarchyQuery.data || !storeViewData || !categoryViewData) {
    return (
      <main className="app-shell">
        <section className="hero">
          <h1>Sales Budget & Planning</h1>
          <p>{statusText}</p>
        </section>
      </main>
    );
  }
  const activeGridData = activeView === "planning-category" ? categoryViewData : storeViewData;

  const handleCellEdit = async (row: GridRow, timePeriodId: number, newValue: number) => {
    const cell = row.cells[timePeriodId];
    const boundStoreId = row.bindingStoreId ?? row.storeId;
    const boundProductNodeId = row.bindingProductNodeId ?? row.productNodeId;
    if (!row.bindingProductNodeId) {
      setLastError("This subtotal is read-only in the current view.");
      return;
    }

    const period = gridQuery.data.periods.find((item) => item.timePeriodId === timePeriodId);
    const isLeafMonth = row.structureRole === "subcategory" && period?.grain === "month";
    await editMutation.mutateAsync({
      scenarioVersionId: gridQuery.data.scenarioVersionId,
      measureId: gridQuery.data.measureId,
      comment: "Grid edit",
      cells: [
        {
          storeId: boundStoreId,
          productNodeId: boundProductNodeId,
          timePeriodId,
          newValue,
          editMode: isLeafMonth ? "input" : "override",
          rowVersion: cell.rowVersion,
        },
      ],
    });
  };

  const handleToggleLock = async (row: GridRow, timePeriodId: number, locked: boolean) => {
    const boundStoreId = row.bindingStoreId ?? row.storeId;
    const boundProductNodeId = row.bindingProductNodeId ?? row.productNodeId;
    if (!row.bindingProductNodeId) {
      setLastError("Only direct planning rows can be locked in this view.");
      return;
    }

    await lockMutation.mutateAsync({
      scenarioVersionId: gridQuery.data.scenarioVersionId,
      measureId: gridQuery.data.measureId,
      locked,
      reason: locked ? "Manager review hold" : "Released",
      coordinates: [
        {
          storeId: boundStoreId,
          productNodeId: boundProductNodeId,
          timePeriodId,
        },
      ],
    });
  };

  const handleSplashYear = async (row: GridRow, yearValue: number) => {
    const boundStoreId = row.bindingStoreId ?? row.storeId;
    const boundProductNodeId = row.bindingProductNodeId ?? row.productNodeId;
    if (!row.bindingProductNodeId) {
      setLastError("Only direct planning rows can be splashed in this view.");
      return;
    }

    await splashMutation.mutateAsync({
      scenarioVersionId: gridQuery.data.scenarioVersionId,
      measureId: gridQuery.data.measureId,
      sourceCell: {
        storeId: boundStoreId,
        productNodeId: boundProductNodeId,
        timePeriodId: 202600,
      },
      totalValue: yearValue,
      method: "seasonality_profile",
      roundingScale: 0,
      comment: "Spread annual value across months",
      manualWeights: seasonalityWeights,
    });
  };

  const handleAddRow = async (level: "store" | "category" | "subcategory", parentRow: GridRow | null) => {
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
      measureId: gridQuery.data.measureId,
      level,
      parentProductNodeId: level === "store" ? null : parentRow?.bindingProductNodeId ?? null,
      label,
      copyFromStoreId,
    });
  };

  const handleImportWorkbook = async (file: File) => {
    await importMutation.mutateAsync({
      file,
      scenarioVersionId: gridQuery.data.scenarioVersionId,
      measureId: gridQuery.data.measureId,
    });
  };

  const handleAddHierarchyCategory = async () => {
    const categoryLabel = window.prompt("New category name");
    if (!categoryLabel) {
      return;
    }

    await addHierarchyCategoryMutation.mutateAsync(categoryLabel);
  };

  const handleAddHierarchySubcategory = async (categoryLabel: string) => {
    const subcategoryLabel = window.prompt(`New subcategory name for ${categoryLabel}`);
    if (!subcategoryLabel) {
      return;
    }

    await addHierarchySubcategoryMutation.mutateAsync({ categoryLabel, subcategoryLabel });
  };

  return (
    <main className="app-shell">
      <section className="hero">
        <div>
          <div className="eyebrow">Enterprise planning skeleton</div>
          <h1>Sales Budget & Planning</h1>
          <p>
            Excel-like planning with store-first and category-first sheets backed by the same live planning data.
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
          className={`secondary-button${activeView === "planning-category" ? " secondary-button-active" : ""}`}
          onClick={() => setActiveView("planning-category")}
        >
          Planning - by Category
        </button>
        <button
          type="button"
          className={`secondary-button${activeView === "hierarchy" ? " secondary-button-active" : ""}`}
          onClick={() => setActiveView("hierarchy")}
        >
          Hierarchy Maintenance
        </button>
      </div>

      {activeView === "planning-category" ? (
        <div className="layout-switcher" role="group" aria-label="Category planning layout">
          <button
            type="button"
            className={`secondary-button${categoryLayout === "category-store-subcategory" ? " secondary-button-active" : ""}`}
            onClick={() => setCategoryLayout("category-store-subcategory")}
          >
            Category - Store - Subcategory
          </button>
          <button
            type="button"
            className={`secondary-button${categoryLayout === "category-subcategory-store" ? " secondary-button-active" : ""}`}
            onClick={() => setCategoryLayout("category-subcategory-store")}
          >
            Category - Subcategory - Store
          </button>
        </div>
      ) : null}

      {activeView === "hierarchy" ? (
        <HierarchyMaintenanceSheet
          categories={hierarchyQuery.data.categories}
          onAddCategory={handleAddHierarchyCategory}
          onAddSubcategory={handleAddHierarchySubcategory}
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
            onCellEdit={handleCellEdit}
            onToggleLock={handleToggleLock}
            onSplashYear={handleSplashYear}
            onAddRow={handleAddRow}
            onImportWorkbook={handleImportWorkbook}
            sheetLabel={activeView === "planning-store" ? "Planning - by Store" : "Planning - by Category"}
          />
        </Suspense>
      )}
    </main>
  );
}

function buildStoreView(data: GridSliceResponse): GridSliceResponse {
  return {
    ...data,
    rows: data.rows.map((row) => ({
      ...row,
      viewRowId: `store-view:${row.storeId}:${row.productNodeId}`,
      structureRole: row.level === 0 ? "store" : row.level === 1 ? "category" : "subcategory",
      bindingStoreId: row.storeId,
      bindingProductNodeId: row.productNodeId,
    })),
  };
}

function buildCategoryView(data: GridSliceResponse, layout: CategoryLayout): GridSliceResponse {
  const storeRows = data.rows.filter((row) => row.structureRole === "store");
  const categoryRows = data.rows.filter((row) => row.structureRole === "category");
  const leafRows = data.rows.filter((row) => row.structureRole === "subcategory");
  const storeLabels = new Map(storeRows.map((row) => [row.storeId, row.label]));
  let syntheticRowSeed = -1;

  const categoryGroups = new Map<string, { categoryRows: GridRow[]; leafRows: GridRow[] }>();
  for (const categoryRow of categoryRows) {
    const categoryLabel = categoryRow.path[1];
    const group = categoryGroups.get(categoryLabel) ?? { categoryRows: [], leafRows: [] };
    group.categoryRows.push(categoryRow);
    categoryGroups.set(categoryLabel, group);
  }

  for (const leafRow of leafRows) {
    const categoryLabel = leafRow.path[1];
    const group = categoryGroups.get(categoryLabel) ?? { categoryRows: [], leafRows: [] };
    group.leafRows.push(leafRow);
    categoryGroups.set(categoryLabel, group);
  }

  const rows: GridRow[] = [];
  for (const [categoryLabel, group] of [...categoryGroups.entries()].sort(([left], [right]) => left.localeCompare(right))) {
    rows.push(createSyntheticRow({
      storeId: 0,
      productNodeId: syntheticRowSeed--,
      label: categoryLabel,
      path: [categoryLabel],
      cells: sumCells(group.categoryRows, data),
    }));

    if (layout === "category-store-subcategory") {
      for (const categoryRow of [...group.categoryRows].sort((left, right) => {
        const leftLabel = storeLabels.get(left.storeId) ?? "";
        const rightLabel = storeLabels.get(right.storeId) ?? "";
        return leftLabel.localeCompare(rightLabel);
      })) {
        const storeLabel = storeLabels.get(categoryRow.storeId) ?? `Store ${categoryRow.storeId}`;
        rows.push({
          ...categoryRow,
          viewRowId: `category-view:${layout}:${categoryLabel}:${storeLabel}`,
          label: storeLabel,
          level: 1,
          path: [categoryLabel, storeLabel],
        });

        const matchingLeaves = group.leafRows
          .filter((leafRow) => leafRow.storeId === categoryRow.storeId)
          .sort((left, right) => left.label.localeCompare(right.label));

        rows.push(...matchingLeaves.map((leafRow) => ({
          ...leafRow,
          viewRowId: `category-view:${layout}:${categoryLabel}:${storeLabel}:${leafRow.label}`,
          level: 2,
          path: [categoryLabel, storeLabel, leafRow.label],
        })));
      }

      continue;
    }

    const subcategoryGroups = new Map<string, GridRow[]>();
    for (const leafRow of group.leafRows) {
      const subcategoryLabel = leafRow.label;
      const subcategoryGroup = subcategoryGroups.get(subcategoryLabel) ?? [];
      subcategoryGroup.push(leafRow);
      subcategoryGroups.set(subcategoryLabel, subcategoryGroup);
    }

    for (const [subcategoryLabel, subcategoryRows] of [...subcategoryGroups.entries()].sort(([left], [right]) => left.localeCompare(right))) {
      rows.push(createSyntheticRow({
        storeId: 0,
        productNodeId: syntheticRowSeed--,
        label: subcategoryLabel,
        path: [categoryLabel, subcategoryLabel],
        cells: sumCells(subcategoryRows, data),
      }));

      for (const leafRow of [...subcategoryRows].sort((left, right) => {
        const leftLabel = storeLabels.get(left.storeId) ?? "";
        const rightLabel = storeLabels.get(right.storeId) ?? "";
        return leftLabel.localeCompare(rightLabel);
      })) {
        const storeLabel = storeLabels.get(leafRow.storeId) ?? `Store ${leafRow.storeId}`;
        rows.push({
          ...leafRow,
          viewRowId: `category-view:${layout}:${categoryLabel}:${subcategoryLabel}:${storeLabel}`,
          label: storeLabel,
          level: 2,
          path: [categoryLabel, subcategoryLabel, storeLabel],
        });
      }
    }
  }

  return {
    ...data,
    rows,
  };
}

function createSyntheticRow(row: Pick<GridRow, "storeId" | "productNodeId" | "label" | "path" | "cells">): GridRow {
  return {
    ...row,
    viewRowId: `synthetic:${row.path.join(">")}:${row.productNodeId}`,
    level: row.path.length - 1,
    isLeaf: false,
    structureRole: "virtual",
    bindingStoreId: null,
    bindingProductNodeId: null,
  };
}

function sumCells(rows: GridRow[], data: GridSliceResponse): Record<number, GridCell> {
  return Object.fromEntries(
    data.periods.map((period) => {
      const value = rows.reduce((total, row) => total + (row.cells[period.timePeriodId]?.value ?? 0), 0);
      return [
        period.timePeriodId,
        {
          value,
          isLocked: false,
          isCalculated: true,
          isOverride: false,
          rowVersion: 0,
          cellKind: "calculated",
        } satisfies GridCell,
      ];
    }),
  );
}
