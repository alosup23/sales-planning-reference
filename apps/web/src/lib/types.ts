export type GridCell = {
  baseValue: number;
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

export type GridBranchResponse = {
  scenarioVersionId: number;
  parentProductNodeId: number;
  rows: GridRow[];
};

export type GridViewBlockResponse = {
  scenarioVersionId: number;
  parentViewRowId: string;
  rows: GridRow[];
};

export type PlanningDepartmentScopeResponse = {
  departments: string[];
};

export type GridCellPatch = {
  storeId: number;
  productNodeId: number;
  timePeriodId: number;
  measureId: number;
  cell: GridCell;
};

export type PlanningGridPatch = {
  scenarioVersionId: number;
  cells: GridCellPatch[];
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
  baseValue: number;
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

export type AsyncJobSummary = {
  rowsProcessed?: number | null;
  cellsUpdated?: number | null;
  rowsCreated?: number | null;
  recordsAdded?: number | null;
  recordsUpdated?: number | null;
  mismatchCount?: number | null;
  checkedCellCount?: number | null;
  resultMessage?: string | null;
};

export type AsyncJobStatus = {
  jobId: string;
  category: string;
  operation: string;
  status: string;
  progressPercent: number;
  progressMessage: string;
  createdAt: string;
  startedAt?: string | null;
  completedAt?: string | null;
  errorMessage?: string | null;
  summary?: AsyncJobSummary | null;
  hasDownload: boolean;
  downloadFileName?: string | null;
  downloadContentType?: string | null;
};

export type StartAsyncJobResponse = {
  jobId: string;
};

export type ReconciliationMismatch = {
  dimension: string;
  storeId: number;
  productNodeId: number;
  timePeriodId: number;
  measureId: number;
  expectedValue: number;
  actualValue: number;
  difference: number;
  message: string;
};

export type ReconciliationReportResponse = {
  scenarioVersionId: number;
  checkedCellCount: number;
  mismatchCount: number;
  mismatches: ReconciliationMismatch[];
  status: string;
};

export type EditCellsResponse = {
  actionId: number;
  updatedCellCount: number;
  status: string;
  patch?: PlanningGridPatch | null;
  availability: UndoRedoAvailability;
};

export type SplashResponse = {
  actionId: number;
  status: string;
  cellsUpdated: number;
  lockedCellsSkipped: number;
  patch?: PlanningGridPatch | null;
  availability: UndoRedoAvailability;
};

export type LockCellsResponse = {
  updatedCellCount: number;
  locked: boolean;
  availability: UndoRedoAvailability;
};

export type ApplyGrowthFactorResponse = {
  actionId: number;
  status: string;
  growthFactor: number;
  updatedCellCount: number;
  patch?: PlanningGridPatch | null;
  availability: UndoRedoAvailability;
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

export type StoreProfile = {
  storeId: number;
  storeCode: string;
  branchName: string;
  state?: string | null;
  clusterLabel: string;
  latitude?: number | null;
  longitude?: number | null;
  regionLabel: string;
  openingDate?: string | null;
  sssg?: string | null;
  salesType?: string | null;
  status?: string | null;
  storey?: string | null;
  buildingStatus?: string | null;
  gta?: number | null;
  nta?: number | null;
  rsom?: string | null;
  dm?: string | null;
  rental?: number | null;
  lifecycleState: string;
  rampProfileCode?: string | null;
  isActive: boolean;
  storeClusterRole?: string | null;
  storeCapacitySqFt?: number | null;
  storeFormatTier?: string | null;
  catchmentType?: string | null;
  demographicSegment?: string | null;
  climateZone?: string | null;
  fulfilmentEnabled?: boolean;
  onlineFulfilmentNode?: boolean;
  storeOpeningSeason?: string | null;
  storeClosureDate?: string | null;
  refurbishmentDate?: string | null;
  storePriority?: string | null;
};

export type StoreProfileResponse = {
  stores: StoreProfile[];
};

export type PlanningStoreScope = {
  storeId: number;
  rootProductNodeId: number;
  branchName: string;
  storeCode?: string | null;
  clusterLabel: string;
  regionLabel: string;
  isActive: boolean;
};

export type PlanningStoreScopeResponse = {
  stores: PlanningStoreScope[];
};

export type UpsertStoreProfileRequest = {
  scenarioVersionId: number;
  storeId?: number | null;
  storeCode: string;
  branchName: string;
  state?: string | null;
  clusterLabel: string;
  latitude?: number | null;
  longitude?: number | null;
  regionLabel: string;
  openingDate?: string | null;
  sssg?: string | null;
  salesType?: string | null;
  status?: string | null;
  storey?: string | null;
  buildingStatus?: string | null;
  gta?: number | null;
  nta?: number | null;
  rsom?: string | null;
  dm?: string | null;
  rental?: number | null;
  lifecycleState: string;
  rampProfileCode?: string | null;
  isActive: boolean;
  storeClusterRole?: string | null;
  storeCapacitySqFt?: number | null;
  storeFormatTier?: string | null;
  catchmentType?: string | null;
  demographicSegment?: string | null;
  climateZone?: string | null;
  fulfilmentEnabled?: boolean;
  onlineFulfilmentNode?: boolean;
  storeOpeningSeason?: string | null;
  storeClosureDate?: string | null;
  refurbishmentDate?: string | null;
  storePriority?: string | null;
};

export type DeleteStoreProfileRequest = {
  scenarioVersionId: number;
  storeId: number;
};

export type StoreProfileImportResponse = {
  rowsProcessed: number;
  storesAdded: number;
  storesUpdated: number;
  status: string;
  exceptionFileName?: string | null;
  exceptionWorkbookBase64?: string | null;
};

export type StoreProfileOption = {
  fieldName: string;
  value: string;
  isActive: boolean;
};

export type StoreProfileOptionsResponse = {
  options: StoreProfileOption[];
};

export type UpsertStoreProfileOptionRequest = {
  fieldName: string;
  value: string;
  isActive: boolean;
};

export type ProductProfile = {
  skuVariant: string;
  description: string;
  description2?: string | null;
  price: number;
  cost: number;
  dptNo: string;
  clssNo: string;
  brandNo?: string | null;
  department: string;
  class: string;
  brand?: string | null;
  revDepartment?: string | null;
  revClass?: string | null;
  subclass: string;
  prodGroup?: string | null;
  prodType?: string | null;
  activeFlag?: string | null;
  orderFlag?: string | null;
  brandType?: string | null;
  launchMonth?: string | null;
  gender?: string | null;
  size?: string | null;
  collection?: string | null;
  promo?: string | null;
  ramadhanPromo?: string | null;
  isActive: boolean;
  supplier?: string | null;
  lifecycleStage?: string | null;
  ageStage?: string | null;
  genderTarget?: string | null;
  material?: string | null;
  packSize?: string | null;
  sizeRange?: string | null;
  colourFamily?: string | null;
  kviFlag?: boolean;
  markdownEligible?: boolean;
  markdownFloorPrice?: number | null;
  minimumMarginPct?: number | null;
  priceLadderGroup?: string | null;
  goodBetterBestTier?: string | null;
  seasonCode?: string | null;
  eventCode?: string | null;
  launchDate?: string | null;
  endOfLifeDate?: string | null;
  substituteGroup?: string | null;
  companionGroup?: string | null;
  replenishmentType?: string | null;
  leadTimeDays?: number | null;
  moq?: number | null;
  casePack?: number | null;
  startingInventory?: number | null;
  projectedStockOnHand?: number | null;
  sellThroughTargetPct?: number | null;
  weeksOfCoverTarget?: number | null;
};

export type ProductProfileResponse = {
  profiles: ProductProfile[];
  totalCount: number;
  pageNumber: number;
  pageSize: number;
  searchTerm?: string | null;
};

export type UpsertProductProfileRequest = ProductProfile;

export type ProductProfileImportResponse = {
  rowsProcessed: number;
  productsAdded: number;
  productsUpdated: number;
  hierarchyRowsProcessed: number;
  status: string;
  exceptionFileName?: string | null;
  exceptionWorkbookBase64?: string | null;
};

export type ProductProfileOption = {
  fieldName: string;
  value: string;
  isActive: boolean;
};

export type ProductProfileOptionsResponse = {
  options: ProductProfileOption[];
};

export type UpsertProductProfileOptionRequest = {
  fieldName: string;
  value: string;
  isActive: boolean;
};

export type ProductHierarchyCatalog = {
  dptNo: string;
  clssNo: string;
  department: string;
  class: string;
  prodGroup: string;
  isActive: boolean;
};

export type ProductHierarchySubclass = {
  department: string;
  class: string;
  subclass: string;
  isActive: boolean;
};

export type ProductHierarchyResponse = {
  hierarchyRows: ProductHierarchyCatalog[];
  subclassRows: ProductHierarchySubclass[];
};

export type InventoryProfile = {
  inventoryProfileId?: number | null;
  storeCode: string;
  productCode: string;
  startingInventory: number;
  inboundQty?: number | null;
  reservedQty?: number | null;
  projectedStockOnHand?: number | null;
  safetyStock?: number | null;
  weeksOfCoverTarget?: number | null;
  sellThroughTargetPct?: number | null;
  isActive: boolean;
};

export type InventoryProfileResponse = {
  profiles: InventoryProfile[];
  totalCount: number;
  pageNumber: number;
  pageSize: number;
  searchTerm?: string | null;
};

export type UpsertInventoryProfileRequest = InventoryProfile;

export type DeleteInventoryProfileRequest = {
  inventoryProfileId: number;
};

export type InventoryProfileImportResponse = {
  rowsProcessed: number;
  recordsAdded: number;
  recordsUpdated: number;
  status: string;
  exceptionFileName?: string | null;
  exceptionWorkbookBase64?: string | null;
};

export type PricingPolicy = {
  pricingPolicyId?: number | null;
  department?: string | null;
  class?: string | null;
  subclass?: string | null;
  brand?: string | null;
  priceLadderGroup?: string | null;
  minPrice?: number | null;
  maxPrice?: number | null;
  markdownFloorPrice?: number | null;
  minimumMarginPct?: number | null;
  kviFlag: boolean;
  markdownEligible: boolean;
  isActive: boolean;
};

export type PricingPolicyResponse = {
  policies: PricingPolicy[];
  totalCount: number;
  pageNumber: number;
  pageSize: number;
  searchTerm?: string | null;
};

export type UpsertPricingPolicyRequest = PricingPolicy;

export type DeletePricingPolicyRequest = {
  pricingPolicyId: number;
};

export type PricingPolicyImportResponse = {
  rowsProcessed: number;
  recordsAdded: number;
  recordsUpdated: number;
  status: string;
  exceptionFileName?: string | null;
  exceptionWorkbookBase64?: string | null;
};

export type SeasonalityEventProfile = {
  seasonalityEventProfileId?: number | null;
  department?: string | null;
  class?: string | null;
  subclass?: string | null;
  seasonCode?: string | null;
  eventCode?: string | null;
  month: number;
  weight: number;
  promoWindow?: string | null;
  peakFlag: boolean;
  isActive: boolean;
};

export type SeasonalityEventProfileResponse = {
  profiles: SeasonalityEventProfile[];
  totalCount: number;
  pageNumber: number;
  pageSize: number;
  searchTerm?: string | null;
};

export type UpsertSeasonalityEventProfileRequest = SeasonalityEventProfile;

export type DeleteSeasonalityEventProfileRequest = {
  seasonalityEventProfileId: number;
};

export type SeasonalityEventProfileImportResponse = {
  rowsProcessed: number;
  recordsAdded: number;
  recordsUpdated: number;
  status: string;
  exceptionFileName?: string | null;
  exceptionWorkbookBase64?: string | null;
};

export type VendorSupplyProfile = {
  vendorSupplyProfileId?: number | null;
  supplier: string;
  brand?: string | null;
  leadTimeDays?: number | null;
  moq?: number | null;
  casePack?: number | null;
  replenishmentType?: string | null;
  paymentTerms?: string | null;
  isActive: boolean;
};

export type VendorSupplyProfileResponse = {
  profiles: VendorSupplyProfile[];
  totalCount: number;
  pageNumber: number;
  pageSize: number;
  searchTerm?: string | null;
};

export type UpsertVendorSupplyProfileRequest = VendorSupplyProfile;

export type DeleteVendorSupplyProfileRequest = {
  vendorSupplyProfileId: number;
};

export type VendorSupplyProfileImportResponse = {
  rowsProcessed: number;
  recordsAdded: number;
  recordsUpdated: number;
  status: string;
  exceptionFileName?: string | null;
  exceptionWorkbookBase64?: string | null;
};

export type UndoRedoAvailability = {
  canUndo: boolean;
  canRedo: boolean;
  undoDepth: number;
  redoDepth: number;
  limit: number;
};

export type UndoPlanningActionResponse = {
  status: string;
  availability: UndoRedoAvailability;
};

export type RedoPlanningActionResponse = {
  status: string;
  availability: UndoRedoAvailability;
};
