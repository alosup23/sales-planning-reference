import { useEffect, useMemo, useRef, useState } from "react";
import {
  type CellClickedEvent,
  ClientSideRowModelModule,
  CommunityFeaturesModule,
  ModuleRegistry,
  type CellContextMenuEvent,
  type CellEditRequestEvent,
  type CellFocusedEvent,
  type ColDef,
  type RowClickedEvent,
  type SelectionChangedEvent,
  type ValueFormatterParams,
} from "ag-grid-community";
import "ag-grid-enterprise";
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
  onAddRow: (level: "store" | "category" | "subcategory", parentRow: GridRow | null) => Promise<void>;
  onImportWorkbook: (file: File) => Promise<void>;
  sheetLabel: string;
  measureLabel: string;
};

type GridRowView = GridRow & {
  __path: string[];
};

type RowKey = {
  id: string;
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

export function PlanningGrid({ data, onCellEdit, onToggleLock, onSplashYear, onAddRow, onImportWorkbook, sheetLabel, measureLabel }: PlanningGridProps) {
  const [selectedRowKey, setSelectedRowKey] = useState<RowKey | null>(null);
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const importInputRef = useRef<HTMLInputElement | null>(null);
  const gridRef = useRef<AgGridReact<GridRowView> | null>(null);

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
      lookup.set(getRowKey(row), row);
    });
    return lookup;
  }, [rowData]);

  const selectedRow = selectedRowKey
    ? rowLookup.get(selectedRowKey.id) ?? null
    : null;
  const contextRow = contextMenu
    ? rowLookup.get(contextMenu.rowKey.id) ?? null
    : null;
  const canAddCategory = selectedRow?.structureRole === "store";
  const canAddSubcategory = selectedRow?.structureRole === "category";
  const canSplashSelectedRow = Boolean(selectedRow?.splashRoots && selectedRow.splashRoots.length > 0);

  const syncSelectedRow = (row: GridRowView | null | undefined) => {
    setSelectedRowKey(
      row
        ? { id: getRowKey(row) }
        : null,
    );
  };

  const columnDefs = useMemo<ColDef<GridRowView>[]>(() => {
    const isLeafMonthEditable = (row: GridRowView | undefined, timePeriodId: number) =>
      Boolean(
        row?.bindingProductNodeId &&
        row.structureRole === "subcategory" &&
        monthPeriods.some((period) => period.timePeriodId === timePeriodId) &&
        !row.cells[timePeriodId]?.isLocked,
      );
    const isTopDownEditable = (row: GridRowView | undefined, timePeriodId: number) =>
      Boolean(
        row?.splashRoots?.length &&
        !isLeafMonthEditable(row, timePeriodId) &&
        !row?.cells[timePeriodId]?.isLocked,
      );

    const yearCol: ColDef<GridRowView> = {
      headerName: yearPeriod?.label ?? "Year",
      colId: yearPeriod ? String(yearPeriod.timePeriodId) : "year",
      editable: (params) => Boolean(yearPeriod && isTopDownEditable(params.data, yearPeriod.timePeriodId)),
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
      editable: (params) => isLeafMonthEditable(params.data, period.timePeriodId) || isTopDownEditable(params.data, period.timePeriodId),
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

  useEffect(() => {
    const api = gridRef.current?.api;
    if (!api) {
      return;
    }

    api.forEachNode((node) => {
      node.setExpanded((node.level ?? 0) < 3);
    });
  }, [rowData]);

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
      rowKey: { id: getRowKey(row) },
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

  const handleSelectionChanged = (event: SelectionChangedEvent<GridRowView>) => {
    syncSelectedRow(event.api.getSelectedRows()[0]);
  };

  const handleCellClicked = (event: CellClickedEvent<GridRowView>) => {
    syncSelectedRow(event.data);
  };

  const handleRowClicked = (event: RowClickedEvent<GridRowView>) => {
    syncSelectedRow(event.data);
  };

  const handleCellFocused = (event: CellFocusedEvent<GridRowView>) => {
    if (event.rowIndex === null) {
      return;
    }

    const row = event.api.getDisplayedRowAtIndex(event.rowIndex)?.data;
    syncSelectedRow(row);
  };

  return (
    <div className="planning-shell">
      <div className="planning-toolbar">
        <div>
          <div className="eyebrow">Sheet</div>
          <strong>{sheetLabel}</strong>
        </div>
        <div>
          <div className="eyebrow">Scenario</div>
          <strong>{`Budget FY26 / ${measureLabel}`}</strong>
        </div>
        <div className="toolbar-actions">
          <button
            type="button"
            className="secondary-button"
            onClick={() => void onAddRow("store", null)}
          >
            Add Store
          </button>
          <button
            type="button"
            className="secondary-button"
            disabled={!canAddCategory}
            onClick={() => void onAddRow("category", selectedRow)}
          >
            Add Category
          </button>
          <button
            type="button"
            className="secondary-button"
            disabled={!canAddSubcategory}
            onClick={() => void onAddRow("subcategory", selectedRow)}
          >
            Add Subcategory
          </button>
          <button
            type="button"
            className="secondary-button"
            onClick={() => importInputRef.current?.click()}
          >
            Upload Workbook
          </button>
          <button
            type="button"
            className="secondary-button"
            disabled={!canSplashSelectedRow}
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
          <input
            ref={importInputRef}
            type="file"
            accept=".xlsx"
            style={{ display: "none" }}
            onChange={(event) => {
              const file = event.target.files?.[0];
              if (!file) {
                return;
              }

              void onImportWorkbook(file);
              event.currentTarget.value = "";
            }}
          />
        </div>
      </div>

      <div className="ag-theme-quartz planning-grid">
        <AgGridReact<GridRowView>
          ref={gridRef}
          rowData={rowData}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          treeData
          animateRows
          getDataPath={(data) => data.__path}
          groupDefaultExpanded={3}
          getRowId={(params) => getRowKey(params.data)}
          suppressAggFuncInHeader
          enableCellTextSelection
          cellSelection
          readOnlyEdit
          undoRedoCellEditing
          undoRedoCellEditingLimit={20}
          rowSelection="single"
          onSelectionChanged={handleSelectionChanged}
          onCellClicked={handleCellClicked}
          onRowClicked={handleRowClicked}
          onCellFocused={handleCellFocused}
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
                if (contextRow.splashRoots?.length) {
                  void onToggleLock(contextRow, contextMenu.timePeriodId, !cell?.isLocked);
                }
                setContextMenu(null);
              }}
              disabled={!contextRow?.splashRoots?.length}
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

                if (contextRow.splashRoots?.length) {
                  const total = contextRow.cells[202600]?.value ?? 0;
                  void onSplashYear(contextRow, total);
                }
                setContextMenu(null);
              }}
              disabled={!contextRow?.splashRoots?.length}
            >
              Splash from year total
            </button>
          </div>
        ) : null}
      </div>
    </div>
  );
}

function getRowKey(row: GridRowView | GridRow): string {
  return row.viewRowId ?? `${row.storeId}-${row.productNodeId}`;
}
