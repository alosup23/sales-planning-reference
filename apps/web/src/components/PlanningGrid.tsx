import { useEffect, useMemo, useRef, useState } from "react";
import {
  type CellClickedEvent,
  type CellContextMenuEvent,
  type CellEditRequestEvent,
  type CellFocusedEvent,
  ClientSideRowModelModule,
  CommunityFeaturesModule,
  ModuleRegistry,
  type ColDef,
  type ColGroupDef,
  type RowClickedEvent,
  type SelectionChangedEvent,
  type ValueFormatterParams,
} from "ag-grid-community";
import "ag-grid-enterprise";
import { AgGridReact } from "ag-grid-react";
import type { GridMeasure, GridPeriod, GridRow, GridSliceResponse } from "../lib/types";
import "ag-grid-community/styles/ag-grid.css";
import "ag-grid-community/styles/ag-theme-quartz.css";

ModuleRegistry.registerModules([
  ClientSideRowModelModule,
  CommunityFeaturesModule,
]);

type PlanningGridProps = {
  data: GridSliceResponse;
  selectedYearId: number | null;
  onSelectedYearChange: (yearTimePeriodId: number | null) => void;
  onCellEdit: (row: GridRow, timePeriodId: number, measureId: number, newValue: number) => Promise<void>;
  onToggleLock: (row: GridRow, timePeriodId: number, measureId: number, locked: boolean) => Promise<void>;
  onSplashYear: (row: GridRow, yearTimePeriodId: number, measureId: number) => Promise<void>;
  onAddRow: (level: "store" | "department" | "class", parentRow: GridRow | null) => Promise<void>;
  onDeleteRow: (row: GridRow | null) => Promise<void>;
  onImportWorkbook: (file: File) => Promise<void>;
  sheetLabel: string;
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
  measureId: number;
};

function formatValue(params: ValueFormatterParams<GridRowView>, measure: GridMeasure): string {
  const value = Number(params.value ?? 0);
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: measure.decimalPlaces,
    maximumFractionDigits: measure.decimalPlaces,
  }).format(value);
}

export function PlanningGrid({
  data,
  selectedYearId,
  onSelectedYearChange,
  onCellEdit,
  onToggleLock,
  onSplashYear,
  onAddRow,
  onDeleteRow,
  onImportWorkbook,
  sheetLabel,
}: PlanningGridProps) {
  const [selectedRowKey, setSelectedRowKey] = useState<RowKey | null>(null);
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const importInputRef = useRef<HTMLInputElement | null>(null);
  const gridRef = useRef<AgGridReact<GridRowView> | null>(null);

  const yearPeriods = useMemo(
    () => data.periods.filter((period) => period.grain === "year"),
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

  const selectedRow = selectedRowKey ? rowLookup.get(selectedRowKey.id) ?? null : null;
  const contextRow = contextMenu ? rowLookup.get(contextMenu.rowKey.id) ?? null : null;
  const canAddDepartment = selectedRow?.structureRole === "store";
  const canAddClass = selectedRow?.structureRole === "department";
  const canDeleteSelectedRow = Boolean(selectedRow?.bindingProductNodeId);
  const canSplashSelectedRow = Boolean(selectedRow?.splashRoots?.length && selectedYearId);

  const syncSelectedRow = (row: GridRowView | null | undefined) => {
    setSelectedRowKey(row ? { id: getRowKey(row) } : null);
  };

  const columnDefs = useMemo<(ColDef<GridRowView> | ColGroupDef<GridRowView>)[]>(() => {
    const buildMeasureColumn = (period: GridPeriod, measure: GridMeasure): ColDef<GridRowView> => {
      const isLeafMonthEditable = (row: GridRowView | undefined) =>
        Boolean(
          row?.bindingProductNodeId &&
          row.structureRole === "class" &&
          period.grain === "month" &&
          !row.cells[period.timePeriodId]?.measures[measure.measureId]?.isLocked,
        );
      const isTopDownEditable = (row: GridRowView | undefined) =>
        Boolean(
          row?.splashRoots?.length &&
          !isLeafMonthEditable(row) &&
          !row?.cells[period.timePeriodId]?.measures[measure.measureId]?.isLocked,
        );

      return {
        headerName: measure.label,
        colId: `${period.timePeriodId}:${measure.measureId}`,
        editable: (params) => isLeafMonthEditable(params.data) || isTopDownEditable(params.data),
        valueGetter: (params) => params.data?.cells[period.timePeriodId]?.measures[measure.measureId]?.value ?? 0,
        valueFormatter: (params) => formatValue(params, measure),
        width: 112,
        cellClassRules: {
          "cell-locked": (params) => Boolean(params.data?.cells[period.timePeriodId]?.measures[measure.measureId]?.isLocked),
          "cell-calculated": (params) => Boolean(params.data?.cells[period.timePeriodId]?.measures[measure.measureId]?.isCalculated),
          "cell-override": (params) => Boolean(params.data?.cells[period.timePeriodId]?.measures[measure.measureId]?.isOverride),
        },
      };
    };

    return yearPeriods.map((yearPeriod) => {
      const monthPeriods = data.periods
        .filter((period) => period.parentTimePeriodId === yearPeriod.timePeriodId)
        .sort((left, right) => left.sortOrder - right.sortOrder);

      return {
        headerName: yearPeriod.label,
        groupId: `year-${yearPeriod.timePeriodId}`,
        children: [
          {
            headerName: "Total",
            children: data.measures.map((measure) => buildMeasureColumn(yearPeriod, measure)),
          },
          ...monthPeriods.map<ColGroupDef<GridRowView>>((monthPeriod) => ({
            headerName: monthPeriod.label,
            columnGroupShow: "open",
            children: data.measures.map((measure) => buildMeasureColumn(monthPeriod, measure)),
          })),
        ],
      };
    });
  }, [data.measures, data.periods, yearPeriods]);

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
      node.setExpanded((node.level ?? 0) < 2);
    });
  }, [rowData]);

  const handleCellContextMenu = (event: CellContextMenuEvent<GridRowView>) => {
    event.event?.preventDefault();
    const row = event.data;
    const mouseEvent = event.event instanceof MouseEvent ? event.event : null;
    const [timePeriodIdRaw, measureIdRaw] = String(event.column?.getColId() ?? "").split(":");
    const timePeriodId = Number(timePeriodIdRaw);
    const measureId = Number(measureIdRaw);

    if (!row || !timePeriodId || !measureId || !mouseEvent) {
      setContextMenu(null);
      return;
    }

    setContextMenu({
      x: mouseEvent.clientX,
      y: mouseEvent.clientY,
      rowKey: { id: getRowKey(row) },
      timePeriodId,
      measureId,
    });
  };

  const copyCurrentCell = async () => {
    if (!contextMenu || !navigator.clipboard) {
      setContextMenu(null);
      return;
    }

    const value = contextRow?.cells[contextMenu.timePeriodId]?.measures[contextMenu.measureId]?.value;
    if (value === undefined || value === null) {
      setContextMenu(null);
      return;
    }

    await navigator.clipboard.writeText(String(value));
    setContextMenu(null);
  };

  const handleCellEditRequest = async (event: CellEditRequestEvent<GridRowView>) => {
    const [timePeriodIdRaw, measureIdRaw] = event.column.getColId().split(":");
    const timePeriodId = Number(timePeriodIdRaw);
    const measureId = Number(measureIdRaw);
    if (!event.data || !Number.isFinite(Number(event.newValue)) || !timePeriodId || !measureId) {
      return;
    }

    await onCellEdit(event.data, timePeriodId, measureId, Number(event.newValue));
  };

  const handleSelectionChanged = (event: SelectionChangedEvent<GridRowView>) => {
    syncSelectedRow(event.api.getSelectedRows()[0]);
  };

  const handleCellClicked = (event: CellClickedEvent<GridRowView>) => {
    syncSelectedRow(event.data);
    const [timePeriodIdRaw] = String(event.column?.getColId() ?? "").split(":");
    const timePeriodId = Number(timePeriodIdRaw);
    const yearPeriod = data.periods.find((period) => period.timePeriodId === timePeriodId || period.timePeriodId === data.periods.find((candidate) => candidate.timePeriodId === timePeriodId)?.parentTimePeriodId);
    if (yearPeriod) {
      onSelectedYearChange(yearPeriod.grain === "year" ? yearPeriod.timePeriodId : yearPeriod.parentTimePeriodId);
    }
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

  const expandSelectedRow = () => {
    if (!selectedRow) {
      return;
    }

    gridRef.current?.api.getRowNode(getRowKey(selectedRow))?.setExpanded(true);
  };

  const collapseSelectedRow = () => {
    if (!selectedRow) {
      return;
    }

    gridRef.current?.api.getRowNode(getRowKey(selectedRow))?.setExpanded(false);
  };

  const setAllRowExpansion = (expanded: boolean) => {
    gridRef.current?.api.forEachNode((node) => {
      node.setExpanded(expanded);
    });
  };

  const setAllYearGroups = (opened: boolean) => {
    yearPeriods.forEach((year) => {
      gridRef.current?.api.setColumnGroupOpened(`year-${year.timePeriodId}`, opened);
    });
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
          <strong>Budget Multi-Year</strong>
        </div>
        <div className="toolbar-actions toolbar-actions-wrap">
          <button type="button" className="secondary-button" onClick={() => void onAddRow("store", null)}>
            Add Store
          </button>
          <button type="button" className="secondary-button" disabled={!canAddDepartment} onClick={() => void onAddRow("department", selectedRow)}>
            Add Department
          </button>
          <button type="button" className="secondary-button" disabled={!canAddClass} onClick={() => void onAddRow("class", selectedRow)}>
            Add Class
          </button>
          <button type="button" className="secondary-button danger-button" disabled={!canDeleteSelectedRow} onClick={() => void onDeleteRow(selectedRow)}>
            Delete Selected Row
          </button>
          <button type="button" className="secondary-button" onClick={() => importInputRef.current?.click()}>
            Upload Workbook
          </button>
          <button
            type="button"
            className="secondary-button"
            disabled={!canSplashSelectedRow || !selectedYearId}
            onClick={() => {
              if (!selectedRow || !selectedYearId) {
                return;
              }

              void Promise.all(data.measures.map((measure) => onSplashYear(selectedRow, selectedYearId, measure.measureId)));
            }}
          >
            Splash Selected Year
          </button>
          <button type="button" className="secondary-button" disabled={!selectedRow} onClick={expandSelectedRow}>
            Expand
          </button>
          <button type="button" className="secondary-button" disabled={!selectedRow} onClick={collapseSelectedRow}>
            Collapse
          </button>
          <button type="button" className="secondary-button" onClick={() => setAllRowExpansion(true)}>
            Expand All
          </button>
          <button type="button" className="secondary-button" onClick={() => setAllRowExpansion(false)}>
            Collapse All
          </button>
          <button type="button" className="secondary-button" onClick={() => setAllYearGroups(true)}>
            Expand Years
          </button>
          <button type="button" className="secondary-button" onClick={() => setAllYearGroups(false)}>
            Collapse Years
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
          getDataPath={(dataRow) => dataRow.__path}
          groupDefaultExpanded={1}
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
            headerName: "Store / Department / Class",
            pinned: "left",
            minWidth: 320,
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

                const cell = contextRow.cells[contextMenu.timePeriodId]?.measures[contextMenu.measureId];
                if (contextRow.splashRoots?.length) {
                  void onToggleLock(contextRow, contextMenu.timePeriodId, contextMenu.measureId, !cell?.isLocked);
                }
                setContextMenu(null);
              }}
              disabled={!contextRow?.splashRoots?.length}
            >
              {contextRow?.cells[contextMenu.timePeriodId]?.measures[contextMenu.measureId]?.isLocked ? "Unlock cell" : "Lock cell"}
            </button>
            <button
              type="button"
              onClick={() => {
                if (!contextRow) {
                  setContextMenu(null);
                  return;
                }

                const yearTimePeriodId = data.periods.find((period) => period.timePeriodId === contextMenu.timePeriodId)?.parentTimePeriodId ?? contextMenu.timePeriodId;
                void onSplashYear(contextRow, yearTimePeriodId, contextMenu.measureId);
                setContextMenu(null);
              }}
              disabled={!contextRow?.splashRoots?.length}
            >
              Splash current year
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
