import type { GridCell, GridCellPatch, GridSliceResponse, PlanningGridPatch } from "./types";

export function applyPlanningGridPatch(
  current: GridSliceResponse | undefined,
  patch: PlanningGridPatch | null | undefined,
): GridSliceResponse | undefined {
  if (!current || !patch || current.scenarioVersionId !== patch.scenarioVersionId || patch.cells.length === 0) {
    return current;
  }

  const rowIndexByKey = new Map<string, number>();
  current.rows.forEach((row, index) => {
    rowIndexByKey.set(`${row.storeId}:${row.productNodeId}`, index);
  });

  let nextRows = current.rows;
  const clonedRowIndexes = new Set<number>();
  const clonedPeriodKeys = new Set<string>();

  const ensureRowClone = (rowIndex: number) => {
    if (!clonedRowIndexes.has(rowIndex)) {
      if (nextRows === current.rows) {
        nextRows = [...current.rows];
      }

      nextRows[rowIndex] = {
        ...nextRows[rowIndex],
        cells: { ...nextRows[rowIndex].cells },
      };
      clonedRowIndexes.add(rowIndex);
    }

    return nextRows[rowIndex];
  };

  const applyCell = (rowIndex: number, cellPatch: GridCellPatch) => {
    const row = ensureRowClone(rowIndex);
    const periodKey = `${rowIndex}:${cellPatch.timePeriodId}`;
    const periodCell = row.cells[cellPatch.timePeriodId];

    if (!clonedPeriodKeys.has(periodKey)) {
      row.cells[cellPatch.timePeriodId] = {
        measures: {
          ...(periodCell?.measures ?? {}),
        },
      };
      clonedPeriodKeys.add(periodKey);
    }

    const nextCell: GridCell = {
      ...cellPatch.cell,
    };

    row.cells[cellPatch.timePeriodId].measures[cellPatch.measureId] = nextCell;
  };

  patch.cells.forEach((cellPatch) => {
    const rowIndex = rowIndexByKey.get(`${cellPatch.storeId}:${cellPatch.productNodeId}`);
    if (rowIndex === undefined) {
      return;
    }

    applyCell(rowIndex, cellPatch);
  });

  if (nextRows === current.rows) {
    return current;
  }

  return {
    ...current,
    rows: nextRows,
  };
}
