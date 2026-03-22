import { useEffect, useMemo, useState } from "react";
import {
  ClientSideRowModelModule,
  CommunityFeaturesModule,
  ModuleRegistry,
  type CellContextMenuEvent,
  type CellEditRequestEvent,
  type ColDef,
  type ValueFormatterParams,
} from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import type { GridRow, GridSliceResponse } from "../lib/types";
import "ag-grid-community/styles/ag-grid.css";
import "ag-grid-community/styles/ag-theme-quartz.css";

ModuleRegistry.registerModules([
  ClientSideRowModelModule,
  CommunityFeaturesModule,
]);

type PlanningGridProps = {
  data: GridSliceResponse;
  onCellEdit: (row: GridRow, timePeriodId: number, newValue: number) => Promise<void>;
  onToggleLock: (row: GridRow, timePeriodId: number, locked: boolean) => Promise<void>;
  onSplashYear: (row: GridRow, yearValue: number) => Promise<void>;
};

type GridRowView = GridRow & {
  __path: string[];
};

type RowKey = {
  storeId: number;
  productNodeId: number;
};

type ContextMenuState = {
  x: number;
  y: number;
  rowKey: RowKey;
  timePeriodId: number;
};

function formatValue(params: ValueFormatterParams<GridRowView>): string {
  const value = Number(params.value ?? 0);
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}

export function PlanningGrid({ data, onCellEdit, onToggleLock, onSplashYear }: PlanningGridProps) {
  const [selectedRowKey, setSelectedRowKey] = useState<RowKey | null>(null);
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);

  const monthPeriods = useMemo(
    () => data.periods.filter((period) => period.grain === "month"),
    [data.periods],
  );
  const yearPeriod = useMemo(
    () => data.periods.find((period) => period.grain === "year"),
    [data.periods],
  );

  const rowData = useMemo<GridRowView[]>(
    () => data.rows.map((row) => ({ ...row, __path: row.path })),
    [data.rows],
  );

  const rowLookup = useMemo(() => {
    const lookup = new Map<string, GridRowView>();
    rowData.forEach((row) => {
      lookup.set(getRowKey(row.storeId, row.productNodeId), row);
    });
    return lookup;
  }, [rowData]);

  const selectedRow = selectedRowKey
    ? rowLookup.get(getRowKey(selectedRowKey.storeId, selectedRowKey.productNodeId)) ?? null
    : null;
  const contextRow = contextMenu
    ? rowLookup.get(getRowKey(contextMenu.rowKey.storeId, contextMenu.rowKey.productNodeId)) ?? null
    : null;

  const columnDefs = useMemo<ColDef<GridRowView>[]>(() => {
    const yearCol: ColDef<GridRowView> = {
      headerName: yearPeriod?.label ?? "Year",
      colId: yearPeriod ? String(yearPeriod.timePeriodId) : "year",
      editable: (params) => Boolean(yearPeriod && !params.data?.cells[yearPeriod.timePeriodId]?.isLocked),
      valueGetter: (params) => yearPeriod ? params.data?.cells[yearPeriod.timePeriodId]?.value ?? 0 : 0,
      valueFormatter: formatValue,
      width: 112,
      cellClassRules: {
        "cell-locked": (params) => Boolean(yearPeriod && params.data?.cells[yearPeriod.timePeriodId]?.isLocked),
        "cell-calculated": (params) => Boolean(yearPeriod && params.data?.cells[yearPeriod.timePeriodId]?.isCalculated),
        "cell-override": (params) => Boolean(yearPeriod && params.data?.cells[yearPeriod.timePeriodId]?.isOverride),
      },
    };

    const monthCols: ColDef<GridRowView>[] = monthPeriods.map((period) => ({
      headerName: period.label,
      colId: String(period.timePeriodId),
      editable: (params) => !params.data?.cells[period.timePeriodId]?.isLocked,
      valueGetter: (params) => params.data?.cells[period.timePeriodId]?.value ?? 0,
      valueFormatter: formatValue,
      width: 96,
      cellClassRules: {
        "cell-locked": (params) => Boolean(params.data?.cells[period.timePeriodId]?.isLocked),
        "cell-calculated": (params) => Boolean(params.data?.cells[period.timePeriodId]?.isCalculated),
        "cell-override": (params) => Boolean(params.data?.cells[period.timePeriodId]?.isOverride),
      },
    }));

    return [
      {
        headerName: "Revenue Plan",
        children: [yearCol, ...monthCols],
      },
    ];
  }, [monthPeriods, yearPeriod]);

  const defaultColDef = useMemo<ColDef<GridRowView>>(
    () => ({
      resizable: true,
      sortable: false,
      suppressMovable: true,
    }),
    [],
  );

  useEffect(() => {
    if (!contextMenu) {
      return;
    }

    const handlePointerDown = () => setContextMenu(null);
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setContextMenu(null);
      }
    };

    window.addEventListener("pointerdown", handlePointerDown);
    window.addEventListener("keydown", handleEscape);

    return () => {
      window.removeEventListener("pointerdown", handlePointerDown);
      window.removeEventListener("keydown", handleEscape);
    };
  }, [contextMenu]);

  const handleCellContextMenu = (event: CellContextMenuEvent<GridRowView>) => {
    event.event?.preventDefault();
    const row = event.data;
    const timePeriodId = Number(event.column?.getColId() ?? 0);
    const mouseEvent = event.event instanceof MouseEvent ? event.event : null;

    if (!row || !timePeriodId || !mouseEvent) {
      setContextMenu(null);
      return;
    }

    setContextMenu({
      x: mouseEvent.clientX,
      y: mouseEvent.clientY,
      rowKey: { storeId: row.storeId, productNodeId: row.productNodeId },
      timePeriodId,
    });
  };

  const copyCurrentCell = async () => {
    if (!contextMenu) {
      return;
    }

    const value = contextRow?.cells[contextMenu.timePeriodId]?.value;
    if (value === undefined || value === null || !navigator.clipboard) {
      setContextMenu(null);
      return;
    }

    await navigator.clipboard.writeText(String(value));
    setContextMenu(null);
  };

  const handleCellEditRequest = async (event: CellEditRequestEvent<GridRowView>) => {
    const timePeriodId = Number(event.column.getColId());
    if (!event.data || !Number.isFinite(Number(event.newValue))) {
      return;
    }

    await onCellEdit(event.data, timePeriodId, Number(event.newValue));
  };

  return (
    <div className="planning-shell">
      <div className="planning-toolbar">
        <div>
          <div className="eyebrow">Scenario</div>
          <strong>Budget FY26</strong>
        </div>
        <div>
          <div className="eyebrow">Measure</div>
          <strong>Revenue</strong>
        </div>
        <div className="toolbar-actions">
          <button
            type="button"
            className="secondary-button"
            disabled={!selectedRow}
            onClick={() => {
              if (!selectedRow) {
                return;
              }

              const yearValue = selectedRow.cells[202600]?.value ?? 0;
              void onSplashYear(selectedRow, yearValue);
            }}
          >
            Splash selected row
          </button>
        </div>
      </div>

      <div className="ag-theme-quartz planning-grid">
        <AgGridReact<GridRowView>
          rowData={rowData}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          treeData
          animateRows
          getDataPath={(data) => data.__path}
          groupDefaultExpanded={1}
          getRowId={(params) => `${params.data.storeId}-${params.data.productNodeId}`}
          suppressAggFuncInHeader
          enableCellTextSelection
          readOnlyEdit
          undoRedoCellEditing
          undoRedoCellEditingLimit={20}
          rowSelection="single"
          onRowSelected={(event) => setSelectedRowKey(
            event.node.data
              ? { storeId: event.node.data.storeId, productNodeId: event.node.data.productNodeId }
              : null,
          )}
          onCellContextMenu={handleCellContextMenu}
          onCellEditRequest={handleCellEditRequest}
          autoGroupColumnDef={{
            headerName: "Store / Category / Subcategory",
            pinned: "left",
            minWidth: 280,
            cellRendererParams: {
              suppressCount: true,
            },
          }}
        />
        {contextMenu ? (
          <div
            className="planning-context-menu"
            style={{ left: contextMenu.x, top: contextMenu.y }}
            onPointerDown={(event) => event.stopPropagation()}
          >
            <button type="button" onClick={() => void copyCurrentCell()}>
              Copy value
            </button>
            <button
              type="button"
              onClick={() => {
                if (!contextRow) {
                  setContextMenu(null);
                  return;
                }

                const cell = contextRow.cells[contextMenu.timePeriodId];
                void onToggleLock(contextRow, contextMenu.timePeriodId, !cell?.isLocked);
                setContextMenu(null);
              }}
            >
              {contextRow?.cells[contextMenu.timePeriodId]?.isLocked ? "Unlock cell" : "Lock cell"}
            </button>
            <button
              type="button"
              onClick={() => {
                if (!contextRow) {
                  setContextMenu(null);
                  return;
                }

                const total = contextRow.cells[202600]?.value ?? 0;
                void onSplashYear(contextRow, total);
                setContextMenu(null);
              }}
            >
              Splash from year total
            </button>
          </div>
        ) : null}
      </div>
    </div>
  );
}

function getRowKey(storeId: number, productNodeId: number): string {
  return `${storeId}-${productNodeId}`;
}
