import { useEffect, useMemo, useRef, useState, type MouseEvent as ReactMouseEvent } from "react";
import {
  type CellClickedEvent,
  type CellContextMenuEvent,
  type CellEditRequestEvent,
  type CellFocusedEvent,
  ClientSideRowModelModule,
  type CellStyle,
  type ColumnGroupOpenedEvent,
  type ColumnResizedEvent,
  CommunityFeaturesModule,
  type ICellRendererParams,
  type GridApi,
  ModuleRegistry,
  type ColDef,
  type ColGroupDef,
  type RowClickedEvent,
  type RowGroupOpenedEvent,
  type SelectionChangedEvent,
  type ValueFormatterParams,
} from "ag-grid-community";
import "ag-grid-enterprise";
import { AgGridReact } from "ag-grid-react";
import type { AddRowResponse, GridMeasure, GridPeriod, GridRow, GridSliceResponse } from "../lib/types";
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
  onSelectionContextChange?: (context: { storeId: number; productNodeId: number; yearTimePeriodId: number; label: string } | null) => void;
  onCellEdit: (row: GridRow, timePeriodId: number, measureId: number, newValue: number) => Promise<void>;
  onApplyGrowthFactor: (row: GridRow, timePeriodId: number, measureId: number, growthFactor: number, currentValue: number) => Promise<void>;
  onToggleLock: (row: GridRow, timePeriodId: number, measureId: number, locked: boolean) => Promise<void>;
  onSplashYear: (row: GridRow, yearTimePeriodId: number, measureId: number) => Promise<void>;
  onAddRow: (level: "store" | "department" | "class" | "subclass", parentRow: GridRow | null) => Promise<void>;
  onDeleteRow: (row: GridRow | null) => Promise<void>;
  onImportWorkbook: (file: File) => Promise<void>;
  sheetLabel: string;
  pendingRevealRow: AddRowResponse | null;
  onRevealHandled: () => void;
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
  timePeriodId: number | null;
  measureId: number | null;
};

type SelectedCellState = {
  rowKey: string;
  timePeriodId: number;
  measureId: number;
};

function formatValue(params: ValueFormatterParams<GridRowView>, measure: GridMeasure): string {
  const value = Number(params.value ?? 0);
  const formatted = new Intl.NumberFormat("en-US", {
    minimumFractionDigits: measure.decimalPlaces,
    maximumFractionDigits: measure.decimalPlaces,
  }).format(value);
  return measure.displayAsPercent ? `${formatted}%` : formatted;
}

export function PlanningGrid({
  data,
  selectedYearId,
  onSelectedYearChange,
  onSelectionContextChange,
  onCellEdit,
  onApplyGrowthFactor,
  onToggleLock,
  onSplashYear,
  onAddRow,
  onDeleteRow,
  onImportWorkbook,
  sheetLabel,
  pendingRevealRow,
  onRevealHandled,
}: PlanningGridProps) {
  const [selectedRowKey, setSelectedRowKey] = useState<RowKey | null>(null);
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const [selectedCell, setSelectedCell] = useState<SelectedCellState | null>(null);
  const [compactMode, setCompactMode] = useState(false);
  const [showGrowthFactors, setShowGrowthFactors] = useState(false);
  const importInputRef = useRef<HTMLInputElement | null>(null);
  const gridRef = useRef<AgGridReact<GridRowView> | null>(null);
  const expandedRowStateRef = useRef<Map<string, boolean>>(new Map());
  const yearGroupStateRef = useRef<Map<string, boolean>>(new Map());
  const columnWidthStateRef = useRef<Map<string, number>>(new Map());
  const hasAppliedInitialExpansionRef = useRef(false);

  const yearPeriods = useMemo(
    () => data.periods.filter((period) => period.grain === "year"),
    [data.periods],
  );
  const yearIndexByTimePeriodId = useMemo(() => {
    const indexByPeriod = new Map<number, number>();
    const sortedYears = [...yearPeriods].sort((left, right) => left.sortOrder - right.sortOrder);

    sortedYears.forEach((yearPeriod, index) => {
      indexByPeriod.set(yearPeriod.timePeriodId, index);
      data.periods
        .filter((period) => period.parentTimePeriodId === yearPeriod.timePeriodId)
        .forEach((period) => indexByPeriod.set(period.timePeriodId, index));
    });

    return indexByPeriod;
  }, [data.periods, yearPeriods]);

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
  const selectedCellRow = selectedCell ? rowLookup.get(selectedCell.rowKey) ?? null : null;
  const canAddDepartment = selectedRow?.structureRole === "store" && Boolean(selectedRow?.bindingProductNodeId);
  const canAddClass = selectedRow?.structureRole === "department" && Boolean(selectedRow?.bindingProductNodeId);
  const canAddSubclass = selectedRow?.structureRole === "class" && Boolean(selectedRow?.bindingProductNodeId);
  const canDeleteSelectedRow = Boolean(selectedRow?.bindingProductNodeId);

  const syncSelectedRow = (row: GridRowView | null | undefined) => {
    setSelectedRowKey(row ? { id: getRowKey(row) } : null);
  };

  const columnDefs = useMemo<(ColDef<GridRowView> | ColGroupDef<GridRowView>)[]>(() => {
      const buildMeasureColumn = (period: GridPeriod, measure: GridMeasure): ColDef<GridRowView> => {
        const isYearTotal = period.grain === "year";
        const measureHeaderWidth = Math.max(measure.displayAsPercent ? 108 : 112, measure.label.length * 8 + 32);
        const isLeafMonthEditable = (row: GridRowView | undefined) =>
          Boolean(
            measure.editableAtLeaf &&
            row?.bindingProductNodeId &&
            row.isLeaf &&
            period.grain === "month" &&
            !row.cells[period.timePeriodId]?.measures[measure.measureId]?.isLocked,
          );
        const isTopDownEditable = (row: GridRowView | undefined) =>
          Boolean(
            measure.editableAtAggregate &&
            row?.splashRoots?.length &&
            !isLeafMonthEditable(row) &&
            !row?.cells[period.timePeriodId]?.measures[measure.measureId]?.isLocked,
          );

      return {
        headerName: measure.label,
        colId: `${period.timePeriodId}:${measure.measureId}`,
        wrapHeaderText: true,
        autoHeaderHeight: true,
        editable: (params) => !showGrowthFactors && (isLeafMonthEditable(params.data) || isTopDownEditable(params.data)),
        valueGetter: (params) => params.data?.cells[period.timePeriodId]?.measures[measure.measureId]?.value ?? 0,
        valueFormatter: (params) => formatValue(params, measure),
        initialWidth: columnWidthStateRef.current.get(`${period.timePeriodId}:${measure.measureId}`) ?? (isYearTotal ? measureHeaderWidth : (measure.displayAsPercent ? 92 : 100)),
        minWidth: isYearTotal ? measureHeaderWidth : (measure.displayAsPercent ? 92 : 100),
        cellStyle: (params): CellStyle => ({
          backgroundColor: getMeasureBandColor(params.data, period.timePeriodId, yearIndexByTimePeriodId),
          textAlign: "right",
        }),
        cellRenderer: GrowthCellRenderer,
        cellRendererParams: {
          measure,
          period,
          showGrowthFactors,
          onApplyGrowthFactor,
        },
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
  }, [data.measures, data.periods, onApplyGrowthFactor, showGrowthFactors, yearIndexByTimePeriodId, yearPeriods]);

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
      if (!node.group || !node.data) {
        return;
      }

      const rowKey = getRowKey(node.data);
      const rememberedState = expandedRowStateRef.current.get(rowKey);
      const nextExpanded = rememberedState ?? (!hasAppliedInitialExpansionRef.current && (node.level ?? 0) < 1);

      if (nextExpanded !== undefined) {
        node.setExpanded(nextExpanded);
        expandedRowStateRef.current.set(rowKey, nextExpanded);
      }
    });

    hasAppliedInitialExpansionRef.current = true;
  }, [rowData]);

  useEffect(() => {
    const api = gridRef.current?.api;
    if (!api) {
      return;
    }

    const widthState = [...columnWidthStateRef.current.entries()].map(([colId, width]) => ({ colId, width }));
    if (widthState.length > 0) {
      api.applyColumnState({ state: widthState, applyOrder: false });
    }

    yearPeriods.forEach((year) => {
      const groupId = `year-${year.timePeriodId}`;
      const rememberedState = yearGroupStateRef.current.get(groupId);
      const nextOpened = rememberedState ?? false;
      yearGroupStateRef.current.set(groupId, nextOpened);
      api.setColumnGroupOpened(groupId, nextOpened);
    });
  }, [columnDefs, yearPeriods]);

  useEffect(() => {
    if (!pendingRevealRow) {
      return;
    }

    const api = gridRef.current?.api;
    if (!api) {
      return;
    }

    const targetRow = rowData.find((row) => row.bindingProductNodeId === pendingRevealRow.productNodeId && row.bindingStoreId === pendingRevealRow.storeId)
      ?? rowData.find((row) => row.productNodeId === pendingRevealRow.productNodeId && row.storeId === pendingRevealRow.storeId);

    if (!targetRow) {
      return;
    }

    for (let depth = 1; depth < targetRow.__path.length; depth += 1) {
      const ancestor = rowData.find((row) =>
        row.__path.length === depth &&
        row.__path.every((segment, index) => segment === targetRow.__path[index]));

      if (!ancestor) {
        continue;
      }

      const ancestorKey = getRowKey(ancestor);
      expandedRowStateRef.current.set(ancestorKey, true);
      api.getRowNode(ancestorKey)?.setExpanded(true);
    }

    const rowKey = getRowKey(targetRow);
    syncSelectedRow(targetRow);
    const rowNode = api.getRowNode(rowKey);
    rowNode?.setSelected(true, true);
    if (rowNode) {
      api.ensureNodeVisible(rowNode, "middle");
    }
    onRevealHandled();
  }, [onRevealHandled, pendingRevealRow, rowData]);

  const handleCellContextMenu = (event: CellContextMenuEvent<GridRowView>) => {
    event.event?.preventDefault();
    const row = event.data;
    const mouseEvent = event.event instanceof MouseEvent ? event.event : null;
    const [timePeriodIdRaw, measureIdRaw] = String(event.column?.getColId() ?? "").split(":");
    const timePeriodId = Number(timePeriodIdRaw);
    const measureId = Number(measureIdRaw);

    if (!row || !mouseEvent) {
      setContextMenu(null);
      return;
    }

    setContextMenu({
      x: mouseEvent.clientX,
      y: mouseEvent.clientY,
      rowKey: { id: getRowKey(row) },
      timePeriodId: Number.isFinite(timePeriodId) ? timePeriodId : null,
      measureId: Number.isFinite(measureId) ? measureId : null,
    });
  };

  const copyCurrentCell = async () => {
    if (!contextMenu || !navigator.clipboard) {
      setContextMenu(null);
      return;
    }

    if (!contextMenu.timePeriodId || !contextMenu.measureId) {
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

  const handleRowGroupOpened = (event: RowGroupOpenedEvent<GridRowView>) => {
    if (!event.node.data || !event.node.group) {
      return;
    }

    expandedRowStateRef.current.set(getRowKey(event.node.data), event.node.expanded ?? false);
  };

  const handleColumnGroupOpened = (event: ColumnGroupOpenedEvent<GridRowView>) => {
    const providedGroup = event.columnGroups?.[0];
    const groupId = providedGroup?.getGroupId?.();
    if (!groupId) {
      return;
    }

    yearGroupStateRef.current.set(groupId, providedGroup?.isExpanded?.() ?? false);
  };

  const handleColumnResized = (event: ColumnResizedEvent<GridRowView>) => {
    if (!event.finished || (event.source !== "uiColumnDragged" && event.source !== "uiColumnResized")) {
      return;
    }

    event.columns?.forEach((column) => {
      columnWidthStateRef.current.set(column.getColId(), column.getActualWidth());
    });
  };

  const handleCellClicked = (event: CellClickedEvent<GridRowView>) => {
    const clickedElement = event.event?.target instanceof HTMLElement ? event.event.target : null;
    const clickedToggle = clickedElement?.closest(".ag-group-contracted, .ag-group-expanded, .ag-group-contracted-icon, .ag-group-expanded-icon");
    if (clickedToggle && event.node?.group && event.data) {
      const nextExpanded = !(event.node.expanded ?? false);
      queueMicrotask(() => {
        event.node.setExpanded(nextExpanded);
        expandedRowStateRef.current.set(getRowKey(event.data!), nextExpanded);
      });
    }

    syncSelectedRow(event.data);
    const [timePeriodIdRaw, measureIdRaw] = String(event.column?.getColId() ?? "").split(":");
    const timePeriodId = Number(timePeriodIdRaw);
    const measureId = Number(measureIdRaw);
    if (event.data && timePeriodId && measureId) {
      setSelectedCell({ rowKey: getRowKey(event.data), timePeriodId, measureId });
    }
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
    if (row && event.column && typeof event.column !== "string") {
      const [timePeriodIdRaw, measureIdRaw] = String(event.column.getColId()).split(":");
      const timePeriodId = Number(timePeriodIdRaw);
      const measureId = Number(measureIdRaw);
      if (timePeriodId && measureId) {
        setSelectedCell({ rowKey: getRowKey(row), timePeriodId, measureId });
      }
    }
  };

  const expandRow = (row: GridRowView | null) => {
    if (!row) {
      return;
    }

    expandedRowStateRef.current.set(getRowKey(row), true);
    gridRef.current?.api.getRowNode(getRowKey(row))?.setExpanded(true);
  };

  const collapseRow = (row: GridRowView | null) => {
    if (!row) {
      return;
    }

    expandedRowStateRef.current.set(getRowKey(row), false);
    gridRef.current?.api.getRowNode(getRowKey(row))?.setExpanded(false);
  };

  const setAllRowExpansion = (expanded: boolean) => {
    gridRef.current?.api.forEachNode((node) => {
      if (!node.group || !node.data) {
        return;
      }

      expandedRowStateRef.current.set(getRowKey(node.data), expanded);
      node.setExpanded(expanded);
    });
  };

  const setAllYearGroups = (opened: boolean) => {
    yearPeriods.forEach((year) => {
      const groupId = `year-${year.timePeriodId}`;
      yearGroupStateRef.current.set(groupId, opened);
      gridRef.current?.api.setColumnGroupOpened(groupId, opened);
    });
  };

  const formulaBarText = useMemo(() => {
    if (!selectedCell || !selectedCellRow) {
      return "Select a cell to review the editable drivers, formula dependencies, and same-year rollup or splash scope.";
    }

    const measure = data.measures.find((item) => item.measureId === selectedCell.measureId);
    const period = data.periods.find((item) => item.timePeriodId === selectedCell.timePeriodId);
    const isLeaf = selectedCellRow.isLeaf && period?.grain === "month";
    const dependencyText = measure?.measureId === 1
      ? "Sales Revenue edit derives Sold Qty from ASP, then recalculates Total Costs, GP, and GP%."
      : measure?.measureId === 2
        ? "Sold Qty edit recalculates Sales Revenue, Total Costs, GP, and GP%."
        : measure?.measureId === 3
          ? "ASP edit recalculates Sales Revenue, GP, and GP%."
          : measure?.measureId === 4
            ? "Unit Cost edit recalculates Total Costs, GP, and GP%."
            : measure?.measureId === 7
              ? "GP% edit derives ASP, then recalculates Sales Revenue, Total Costs, and GP."
              : "Calculated measures refresh from the editable drivers.";
    const scopeText = isLeaf
      ? "Leaf actions roll up only through the matching branch and year."
      : "Aggregate actions splash only to descendants inside this branch and year, then roll up ancestors.";

    return `${selectedCellRow.path.join(" > ")} | ${period?.label ?? ""} | ${measure?.label ?? ""}. ${dependencyText} ${scopeText}`;
  }, [data.measures, data.periods, selectedCell, selectedCellRow]);

  useEffect(() => {
    if (!onSelectionContextChange) {
      return;
    }

    if (!selectedCell || !selectedCellRow) {
      onSelectionContextChange(null);
      return;
    }

    const period = data.periods.find((item) => item.timePeriodId === selectedCell.timePeriodId);
    const yearTimePeriodId = period?.grain === "year" ? period.timePeriodId : period?.parentTimePeriodId;
    const boundStoreId = selectedCellRow.bindingStoreId ?? selectedCellRow.storeId;
    const boundProductNodeId = selectedCellRow.bindingProductNodeId;

    if (!yearTimePeriodId) {
      onSelectionContextChange(null);
      return;
    }

    if (boundProductNodeId) {
      onSelectionContextChange({
        storeId: boundStoreId,
        productNodeId: boundProductNodeId,
        yearTimePeriodId,
        label: selectedCellRow.path.join(" > "),
      });
      return;
    }

    if (selectedCellRow.splashRoots?.length === 1) {
      onSelectionContextChange({
        storeId: selectedCellRow.splashRoots[0].storeId,
        productNodeId: selectedCellRow.splashRoots[0].productNodeId,
        yearTimePeriodId,
        label: selectedCellRow.path.join(" > "),
      });
      return;
    }

    onSelectionContextChange(null);
  }, [data.periods, onSelectionContextChange, selectedCell, selectedCellRow]);

  return (
    <div className={`planning-shell${compactMode ? " planning-shell-compact" : ""}`}>
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
          <button type="button" className="secondary-button" disabled={!canAddSubclass} onClick={() => void onAddRow("subclass", selectedRow)}>
            Add Subclass
          </button>
          <button type="button" className="secondary-button danger-button" disabled={!canDeleteSelectedRow} onClick={() => void onDeleteRow(selectedRow)}>
            Delete Selected Row
          </button>
          <button type="button" className="secondary-button" onClick={() => importInputRef.current?.click()}>
            Upload Workbook
          </button>
          <button
            type="button"
            className={`secondary-button${compactMode ? " secondary-button-active" : ""}`}
            onClick={() => setCompactMode((current) => !current)}
          >
            Compact Mode
          </button>
          <button
            type="button"
            className={`secondary-button${showGrowthFactors ? " secondary-button-active" : ""}`}
            onClick={() => setShowGrowthFactors((current) => !current)}
          >
            Growth Factors
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

      <div className="formula-bar" aria-live="polite">
        {formulaBarText} {selectedCellRow ? `Cluster ${selectedCellRow.clusterLabel} | Region ${selectedCellRow.regionLabel} | Ramp ${selectedCellRow.rampProfileCode ?? "none"} | Lifecycle ${selectedCellRow.lifecycleState}.` : ""}
      </div>

      <div className="ag-theme-quartz planning-grid">
        <AgGridReact<GridRowView>
          ref={gridRef}
          rowData={rowData}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          getRowClass={(params) => getRowClasses(params.data)}
          treeData
          animateRows
          getDataPath={(dataRow) => dataRow.__path}
          groupDefaultExpanded={1}
          getRowId={(params) => getRowKey(params.data)}
          suppressAggFuncInHeader
          enableCellTextSelection
          cellSelection
          readOnlyEdit
          suppressClickEdit={showGrowthFactors}
          undoRedoCellEditing
          undoRedoCellEditingLimit={20}
          rowHeight={compactMode ? 24 : 28}
          headerHeight={compactMode ? 28 : 32}
          groupHeaderHeight={compactMode ? 30 : 34}
          rowSelection="single"
          onSelectionChanged={handleSelectionChanged}
          onCellClicked={handleCellClicked}
          onRowClicked={handleRowClicked}
          onCellFocused={handleCellFocused}
          onCellContextMenu={handleCellContextMenu}
          onCellEditRequest={handleCellEditRequest}
          onRowGroupOpened={handleRowGroupOpened}
          onColumnGroupOpened={handleColumnGroupOpened}
          onColumnResized={handleColumnResized}
          autoGroupColumnDef={{
            headerName: "Store / Department / Class / Subclass",
            pinned: "left",
            minWidth: 260,
            valueGetter: (params) => params.data?.label ?? "",
            cellStyle: (params) => ({
              backgroundColor: getBaseBandColor(params.data),
            }),
            cellRenderer: HierarchyCellRenderer,
            cellRendererParams: {
              onToggleExpandedState: (row: GridRowView, expanded: boolean) => {
                expandedRowStateRef.current.set(getRowKey(row), expanded);
              },
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
                expandRow(contextRow);
                setContextMenu(null);
              }}
              disabled={!contextRow}
            >
              Expand
            </button>
            <button
              type="button"
              onClick={() => {
                collapseRow(contextRow);
                setContextMenu(null);
              }}
              disabled={!contextRow}
            >
              Collapse
            </button>
            <button
              type="button"
              onClick={() => {
                setAllRowExpansion(true);
                setContextMenu(null);
              }}
            >
              Expand All
            </button>
            <button
              type="button"
              onClick={() => {
                setAllRowExpansion(false);
                setContextMenu(null);
              }}
            >
              Collapse All
            </button>
            <button
              type="button"
              onClick={() => {
                if (!contextRow || !contextMenu.timePeriodId || !contextMenu.measureId) {
                  setContextMenu(null);
                  return;
                }

                const cell = contextRow.cells[contextMenu.timePeriodId]?.measures[contextMenu.measureId];
                if (contextRow.splashRoots?.length) {
                  void onToggleLock(contextRow, contextMenu.timePeriodId, contextMenu.measureId, !cell?.isLocked);
                }
                setContextMenu(null);
              }}
              disabled={!contextRow?.splashRoots?.length || !contextMenu.timePeriodId || !contextMenu.measureId}
            >
              {contextMenu.timePeriodId && contextMenu.measureId && contextRow?.cells[contextMenu.timePeriodId]?.measures[contextMenu.measureId]?.isLocked ? "Unlock cell" : "Lock cell"}
            </button>
            <button
              type="button"
              onClick={() => {
                if (!contextRow || !contextMenu.timePeriodId || !contextMenu.measureId) {
                  setContextMenu(null);
                  return;
                }

                const yearTimePeriodId = data.periods.find((period) => period.timePeriodId === contextMenu.timePeriodId)?.parentTimePeriodId ?? contextMenu.timePeriodId;
                void onSplashYear(contextRow, yearTimePeriodId, contextMenu.measureId);
                setContextMenu(null);
              }}
              disabled={!contextRow?.splashRoots?.length || !contextMenu.timePeriodId || !contextMenu.measureId}
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

function getRowClasses(row: GridRowView | undefined): string[] {
  if (!row) {
    return [];
  }

  const classes = [getRowBandClass(row)];
  if (!row.isLeaf && row.level === 2) {
    classes.push("aggregate-row-level-2");
  }

  if (!row.isLeaf && row.level === 3) {
    classes.push("aggregate-row-level-3");
  }

  return classes;
}

function getRowBandClass(row: GridRowView): string {
  if (row.isLeaf) {
    return "row-band-leaf";
  }

  return `row-band-level-${Math.min(row.level, 3)}`;
}

function getMeasureBandColor(row: GridRowView | undefined, timePeriodId: number, yearIndexByTimePeriodId: Map<number, number>): string {
  const baseColor = getBaseBandColor(row);
  if (baseColor === "#ffffff") {
    return baseColor;
  }

  return enrichHexColor(baseColor, yearIndexByTimePeriodId.get(timePeriodId) ?? 0);
}

function getBaseBandColor(row: GridRowView | undefined): string {
  if (!row || row.isLeaf) {
    return "#ffffff";
  }

  switch (Math.min(row.level, 3)) {
    case 0:
      return "#d9d9d9";
    case 1:
      return "#adebeb";
    case 2:
      return "#c2f0f0";
    default:
      return "#d6f5f5";
  }
}

function enrichHexColor(hex: string, yearIndex: number): string {
  if (yearIndex <= 0) {
    return hex;
  }

  const richnessFactor = Math.max(0, 1 - (yearIndex * 0.05));
  const [red, green, blue] = [1, 3, 5].map((offset) => Number.parseInt(hex.slice(offset, offset + 2), 16));
  const toHex = (value: number) => Math.round(value * richnessFactor).toString(16).padStart(2, "0");
  return `#${toHex(red)}${toHex(green)}${toHex(blue)}`;
}

type GrowthCellRendererProps = {
  api: GridApi<GridRowView>;
  value?: number;
  data?: GridRowView;
  measure: GridMeasure;
  period: GridPeriod;
  showGrowthFactors: boolean;
  onApplyGrowthFactor: (row: GridRow, timePeriodId: number, measureId: number, growthFactor: number, currentValue: number) => Promise<void>;
};

type HierarchyCellRendererProps = ICellRendererParams<GridRowView> & {
  onToggleExpandedState: (row: GridRowView, expanded: boolean) => void;
};

function HierarchyCellRenderer(props: HierarchyCellRendererProps) {
  const { data, node, onToggleExpandedState, value } = props;
  const canExpand = Boolean(node.group && data);
  const depth = Math.max(node.level ?? 0, 0);

  const handleToggle = (event: ReactMouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    event.stopPropagation();

    if (!canExpand || !data) {
      return;
    }

    const nextExpanded = !(node.expanded ?? false);
    node.setExpanded(nextExpanded);
    onToggleExpandedState(data, nextExpanded);
    node.setSelected(true, true);
  };

  return (
    <div className="hierarchy-cell" style={{ paddingLeft: `${depth * 14}px` }}>
      {canExpand ? (
        <button
          type="button"
          className="hierarchy-toggle"
          aria-label={`${node.expanded ? "Collapse" : "Expand"} ${value ?? data?.label ?? "row"}`}
          onMouseDown={(event) => event.stopPropagation()}
          onClick={handleToggle}
        >
          {node.expanded ? "▾" : "▸"}
        </button>
      ) : (
        <span className="hierarchy-toggle-spacer" aria-hidden="true" />
      )}
      <span className="hierarchy-label">{String(value ?? data?.label ?? "")}</span>
    </div>
  );
}

function GrowthCellRenderer(props: GrowthCellRendererProps) {
  const { data, measure, period, showGrowthFactors, onApplyGrowthFactor } = props;
  const currentValue = Number(props.value ?? 0);
  const cell = data?.cells[period.timePeriodId]?.measures[measure.measureId];
  const [draftGrowthFactor, setDraftGrowthFactor] = useState(String(cell?.growthFactor ?? 1));
  const isApplyingGrowthFactorRef = useRef(false);

  useEffect(() => {
    setDraftGrowthFactor(String(cell?.growthFactor ?? 1));
  }, [cell?.growthFactor]);

  const isLeafMonthEditable = Boolean(
    measure.editableAtLeaf &&
    data?.bindingProductNodeId &&
    data.isLeaf &&
    period.grain === "month" &&
    !cell?.isLocked,
  );

  const isAggregateEditable = Boolean(
    measure.editableAtAggregate &&
    data?.splashRoots?.length &&
    !isLeafMonthEditable &&
    !cell?.isLocked,
  );

  const canEditGrowthFactor = showGrowthFactors && (isLeafMonthEditable || isAggregateEditable);

  const commitGrowthFactor = async () => {
    const parsed = Number(draftGrowthFactor);
    if (!data || !Number.isFinite(parsed) || parsed < 0 || parsed === Number(cell?.growthFactor ?? 1)) {
      setDraftGrowthFactor(String(cell?.growthFactor ?? 1));
      return;
    }

    isApplyingGrowthFactorRef.current = true;
    try {
      await onApplyGrowthFactor(data, period.timePeriodId, measure.measureId, parsed, currentValue);
      setDraftGrowthFactor("1.0");
    } finally {
      isApplyingGrowthFactorRef.current = false;
    }
  };

  return (
    <div className={`measure-cell${showGrowthFactors ? " measure-cell-with-growth" : ""}`}>
      <span className="measure-value">{formatValue({ value: currentValue } as ValueFormatterParams<GridRowView>, measure)}</span>
      {showGrowthFactors ? (
        <input
          className="growth-factor-input"
          aria-label={`${measure.label} growth factor`}
          type="number"
          step="0.1"
          value={draftGrowthFactor}
          disabled={!canEditGrowthFactor}
          onFocus={() => props.api.stopEditing()}
          onMouseDown={(event) => event.stopPropagation()}
          onClick={(event) => event.stopPropagation()}
          onDoubleClick={(event) => event.stopPropagation()}
          onChange={(event) => setDraftGrowthFactor(event.target.value)}
          onBlur={() => {
            if (isApplyingGrowthFactorRef.current) {
              return;
            }

            setDraftGrowthFactor(String(cell?.growthFactor ?? 1));
          }}
          onKeyDown={async (event) => {
            event.stopPropagation();
            if (event.key === "Enter") {
              event.preventDefault();
              props.api.stopEditing();
              await commitGrowthFactor();
            }

            if (event.key === "Escape") {
              setDraftGrowthFactor(String(cell?.growthFactor ?? 1));
              event.preventDefault();
              props.api.stopEditing();
              event.currentTarget.blur();
            }
          }}
        />
      ) : null}
    </div>
  );
}
