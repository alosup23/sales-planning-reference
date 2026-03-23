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
import type { GridRow } from "./lib/types";

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

export default function App() {
  const queryClient = useQueryClient();
  const [lastError, setLastError] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<"planning" | "hierarchy">("planning");

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

  const statusText = useMemo(() => {
    if (gridQuery.isLoading) {
      return "Loading planning slice...";
    }

    if (
      editMutation.isPending ||
      lockMutation.isPending ||
      splashMutation.isPending ||
      addRowMutation.isPending ||
      importMutation.isPending ||
      addHierarchyCategoryMutation.isPending ||
      addHierarchySubcategoryMutation.isPending
    ) {
      return "Applying changes...";
    }

    if (gridQuery.isError || hierarchyQuery.isError) {
      return "API unavailable.";
    }

    if (lastError) {
      return lastError;
    }

    return activeView === "planning"
      ? "Lock-safe planning grid ready."
      : "Hierarchy maintenance sheet ready.";
  }, [
    activeView,
    addHierarchyCategoryMutation.isPending,
    addHierarchySubcategoryMutation.isPending,
    addRowMutation.isPending,
    editMutation.isPending,
    gridQuery.isError,
    gridQuery.isLoading,
    hierarchyQuery.isError,
    importMutation.isPending,
    lastError,
    lockMutation.isPending,
    splashMutation.isPending,
  ]);

  if (!gridQuery.data || !hierarchyQuery.data) {
    return (
      <main className="app-shell">
        <section className="hero">
          <h1>Sales Budget & Planning</h1>
          <p>{statusText}</p>
        </section>
      </main>
    );
  }

  const handleCellEdit = async (row: GridRow, timePeriodId: number, newValue: number) => {
    const cell = row.cells[timePeriodId];
    const period = gridQuery.data.periods.find((item) => item.timePeriodId === timePeriodId);
    const isLeafMonth = row.isLeaf && period?.grain === "month";
    await editMutation.mutateAsync({
      scenarioVersionId: gridQuery.data.scenarioVersionId,
      measureId: gridQuery.data.measureId,
      comment: "Grid edit",
      cells: [
        {
          storeId: row.storeId,
          productNodeId: row.productNodeId,
          timePeriodId,
          newValue,
          editMode: isLeafMonth ? "input" : "override",
          rowVersion: cell.rowVersion,
        },
      ],
    });
  };

  const handleToggleLock = async (row: GridRow, timePeriodId: number, locked: boolean) => {
    await lockMutation.mutateAsync({
      scenarioVersionId: gridQuery.data.scenarioVersionId,
      measureId: gridQuery.data.measureId,
      locked,
      reason: locked ? "Manager review hold" : "Released",
      coordinates: [
        {
          storeId: row.storeId,
          productNodeId: row.productNodeId,
          timePeriodId,
        },
      ],
    });
  };

  const handleSplashYear = async (row: GridRow, yearValue: number) => {
    await splashMutation.mutateAsync({
      scenarioVersionId: gridQuery.data.scenarioVersionId,
      measureId: gridQuery.data.measureId,
      sourceCell: {
        storeId: row.storeId,
        productNodeId: row.productNodeId,
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
      const stores = gridQuery.data.rows.filter((row) => row.level === 0);
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
      parentProductNodeId: level === "store" ? null : parentRow?.productNodeId ?? null,
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
            Excel-like planning with strict bottom-up rollups, explicit top-down splash, and maintainable hierarchy mapping.
          </p>
        </div>
        <div className={`status-card${lastError ? " status-card-error" : ""}`} aria-live="polite">
          {statusText}
        </div>
      </section>

      <div className="view-switcher" role="tablist" aria-label="Sheet navigation">
        <button
          type="button"
          className={`secondary-button${activeView === "planning" ? " secondary-button-active" : ""}`}
          onClick={() => setActiveView("planning")}
        >
          Planning Sheet
        </button>
        <button
          type="button"
          className={`secondary-button${activeView === "hierarchy" ? " secondary-button-active" : ""}`}
          onClick={() => setActiveView("hierarchy")}
        >
          Hierarchy Maintenance
        </button>
      </div>

      {activeView === "planning" ? (
        <Suspense
          fallback={
            <section className="planning-shell planning-shell-loading" aria-live="polite">
              Preparing planning grid...
            </section>
          }
        >
          <PlanningGrid
            data={gridQuery.data}
            onCellEdit={handleCellEdit}
            onToggleLock={handleToggleLock}
            onSplashYear={handleSplashYear}
            onAddRow={handleAddRow}
            onImportWorkbook={handleImportWorkbook}
          />
        </Suspense>
      ) : (
        <HierarchyMaintenanceSheet
          categories={hierarchyQuery.data.categories}
          onAddCategory={handleAddHierarchyCategory}
          onAddSubcategory={handleAddHierarchySubcategory}
        />
      )}
    </main>
  );
}
