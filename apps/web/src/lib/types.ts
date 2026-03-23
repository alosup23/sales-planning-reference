export type GridCell = {
  value: number;
  isLocked: boolean;
  isCalculated: boolean;
  isOverride: boolean;
  rowVersion: number;
  cellKind: string;
};

export type GridPeriod = {
  timePeriodId: number;
  label: string;
  grain: "year" | "month";
  parentTimePeriodId: number | null;
  sortOrder: number;
};

export type GridRow = {
  storeId: number;
  productNodeId: number;
  viewRowId?: string;
  label: string;
  level: number;
  path: string[];
  isLeaf: boolean;
  cells: Record<number, GridCell>;
  structureRole?: "store" | "category" | "subcategory" | "virtual";
  bindingStoreId?: number | null;
  bindingProductNodeId?: number | null;
  splashRoots?: Array<{
    storeId: number;
    productNodeId: number;
  }>;
};

export type GridSliceResponse = {
  scenarioVersionId: number;
  measureId: number;
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
  measureId: number;
  level: "store" | "category" | "subcategory";
  parentProductNodeId: number | null;
  label: string;
  copyFromStoreId: number | null;
};

export type HierarchyCategory = {
  categoryLabel: string;
  subcategoryLabels: string[];
};

export type HierarchyMappingResponse = {
  categories: HierarchyCategory[];
};
