export type GridCell = {
  value: number;
  growthFactor: number;
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
  displayAsPercent: boolean;
  editableAtLeaf: boolean;
  editableAtAggregate: boolean;
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
  nodeKind: "store" | "department" | "class" | "subclass" | "virtual";
  storeLabel: string;
  clusterLabel: string;
  regionLabel: string;
  lifecycleState: string;
  rampProfileCode?: string | null;
  effectiveFromTimePeriodId?: number | null;
  effectiveToTimePeriodId?: number | null;
  cells: Record<number, GridPeriodCell>;
  structureRole?: "store" | "department" | "class" | "subclass" | "virtual";
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

export type GrowthFactorRequest = {
  scenarioVersionId: number;
  measureId: number;
  sourceCell: {
    storeId: number;
    productNodeId: number;
    timePeriodId: number;
  };
  currentValue: number;
  growthFactor: number;
  comment?: string;
  scopeRoots?: Array<{
    storeId: number;
    productNodeId: number;
  }>;
};

export type GenerateNextYearRequest = {
  scenarioVersionId: number;
  sourceYearTimePeriodId: number;
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
  level: "store" | "department" | "class" | "subclass";
  parentProductNodeId: number | null;
  label: string;
  copyFromStoreId: number | null;
  clusterLabel?: string | null;
  regionLabel?: string | null;
};

export type AddRowResponse = {
  storeId: number;
  productNodeId: number;
  label: string;
  level: number;
  path: string[];
  isLeaf: boolean;
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

export type SaveScenarioRequest = {
  scenarioVersionId: number;
  mode: "manual" | "autosave";
};

export type SaveScenarioResponse = {
  status: string;
  mode: string;
  savedAt: string;
};

export type HierarchyDepartment = {
  departmentLabel: string;
  lifecycleState: string;
  rampProfileCode?: string | null;
  effectiveFromTimePeriodId?: number | null;
  effectiveToTimePeriodId?: number | null;
  classes: HierarchyClass[];
};

export type HierarchyClass = {
  classLabel: string;
  lifecycleState: string;
  rampProfileCode?: string | null;
  effectiveFromTimePeriodId?: number | null;
  effectiveToTimePeriodId?: number | null;
  subclasses: HierarchySubclass[];
};

export type HierarchySubclass = {
  subclassLabel: string;
  lifecycleState: string;
  rampProfileCode?: string | null;
  effectiveFromTimePeriodId?: number | null;
  effectiveToTimePeriodId?: number | null;
};

export type HierarchyMappingResponse = {
  departments: HierarchyDepartment[];
};

export type PlanningInsightResponse = {
  providerStatus: string;
  scopeLabel: string;
  recommendedForecastModel: string;
  seasonalityStrength: number;
  recommendedPriceFloor: number;
  recommendedPriceTarget: number;
  recommendedPriceCeiling: number;
  grossProfitOpportunity: number;
  quantityOpportunity: number;
  insightBullets: string[];
};
