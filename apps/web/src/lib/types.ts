export type GridCell = {
  value: number;
  isLocked: boolean;
  isCalculated: boolean;
  isOverride: boolean;
  rowVersion: number;
  cellKind: string;
};

export type GridMeasure = {
  measureId: number;
  label: string;
  decimalPlaces: number;
  derivedAtAggregateLevels: boolean;
};

export type GridPeriod = {
  timePeriodId: number;
  label: string;
  grain: "year" | "month";
  parentTimePeriodId: number | null;
  sortOrder: number;
};

export type GridPeriodCell = {
  measures: Record<number, GridCell>;
};

export type GridRow = {
  storeId: number;
  productNodeId: number;
  viewRowId?: string;
  label: string;
  level: number;
  path: string[];
  isLeaf: boolean;
  cells: Record<number, GridPeriodCell>;
  structureRole?: "store" | "department" | "class" | "virtual";
  bindingStoreId?: number | null;
  bindingProductNodeId?: number | null;
  splashRoots?: Array<{
    storeId: number;
    productNodeId: number;
  }>;
};

export type GridSliceResponse = {
  scenarioVersionId: number;
  measures: GridMeasure[];
  periods: GridPeriod[];
  rows: GridRow[];
};

export type SplashRequest = {
  scenarioVersionId: number;
  measureId: number;
  sourceCell: {
    storeId: number;
    productNodeId: number;
    timePeriodId: number;
  };
  totalValue: number;
  method: string;
  roundingScale: number;
  comment?: string;
  manualWeights?: Record<number, number>;
  scopeRoots?: Array<{
    storeId: number;
    productNodeId: number;
  }>;
};

export type EditCellsRequest = {
  scenarioVersionId: number;
  measureId: number;
  comment?: string;
  cells: Array<{
    storeId: number;
    productNodeId: number;
    timePeriodId: number;
    newValue: number;
    editMode: "input" | "override";
    rowVersion: number;
  }>;
};

export type LockCellsRequest = {
  scenarioVersionId: number;
  measureId: number;
  locked: boolean;
  reason?: string;
  coordinates: Array<{
    storeId: number;
    productNodeId: number;
    timePeriodId: number;
  }>;
};

export type AddRowRequest = {
  scenarioVersionId: number;
  level: "store" | "department" | "class";
  parentProductNodeId: number | null;
  label: string;
  copyFromStoreId: number | null;
};

export type DeleteRowRequest = {
  scenarioVersionId: number;
  productNodeId: number;
};

export type DeleteYearRequest = {
  scenarioVersionId: number;
  yearTimePeriodId: number;
};

export type ImportWorkbookResponse = {
  rowsProcessed: number;
  cellsUpdated: number;
  rowsCreated: number;
  status: string;
  exceptionFileName?: string | null;
  exceptionWorkbookBase64?: string | null;
};

export type HierarchyDepartment = {
  departmentLabel: string;
  classLabels: string[];
};

export type HierarchyMappingResponse = {
  departments: HierarchyDepartment[];
};
