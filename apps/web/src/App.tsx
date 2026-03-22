import { Suspense, lazy, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { getGridSlice, postEdit, postLock, postSplash } from "./lib/api";
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

  useEffect(() => {
    void preloadPlanningGrid();
  }, []);

  const gridQuery = useQuery({
    queryKey: ["grid-slice", 1, 1],
    queryFn: getGridSlice,
  });

  const refresh = async () => {
    await queryClient.invalidateQueries({ queryKey: ["grid-slice", 1, 1] });
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

  const statusText = useMemo(() => {
    if (gridQuery.isLoading) {
      return "Loading planning slice...";
    }

    if (editMutation.isPending || lockMutation.isPending || splashMutation.isPending) {
      return "Applying changes...";
    }

    if (gridQuery.isError) {
      return "API unavailable. Showing fallback structure.";
    }

    if (lastError) {
      return lastError;
    }

    return "Lock-safe planning grid ready.";
  }, [editMutation.isPending, gridQuery.isError, gridQuery.isLoading, lastError, lockMutation.isPending, splashMutation.isPending]);

  if (!gridQuery.data) {
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
          editMode: timePeriodId === 202600 ? "override" : "input",
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

  return (
    <main className="app-shell">
      <section className="hero">
        <div>
          <div className="eyebrow">Enterprise planning skeleton</div>
          <h1>Sales Budget & Planning</h1>
          <p>
            Hierarchical grid shell with lock-safe edits, year-to-month splash, and backend action contracts.
          </p>
        </div>
        <div className={`status-card${lastError ? " status-card-error" : ""}`} aria-live="polite">
          {statusText}
        </div>
      </section>

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
        />
      </Suspense>
    </main>
  );
}
