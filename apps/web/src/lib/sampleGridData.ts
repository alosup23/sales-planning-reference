import type { GridSliceResponse } from "./types";

const zeroMeasureCell = (value = 0, isLocked = false) => ({
  value,
  isLocked,
  isCalculated: true,
  isOverride: false,
  rowVersion: 1,
  cellKind: "calculated",
});

export const sampleGridData: GridSliceResponse = {
  scenarioVersionId: 1,
  measures: [
    { measureId: 1, label: "Sales Revenue", decimalPlaces: 0, derivedAtAggregateLevels: false },
    { measureId: 2, label: "Sold Qty", decimalPlaces: 0, derivedAtAggregateLevels: false },
    { measureId: 3, label: "ASP", decimalPlaces: 2, derivedAtAggregateLevels: true },
  ],
  periods: [
    { timePeriodId: 202600, label: "FY26", grain: "year", parentTimePeriodId: null, sortOrder: 2600 },
    { timePeriodId: 202601, label: "Jan", grain: "month", parentTimePeriodId: 202600, sortOrder: 2601 },
    { timePeriodId: 202602, label: "Feb", grain: "month", parentTimePeriodId: 202600, sortOrder: 2602 },
    { timePeriodId: 202700, label: "FY27", grain: "year", parentTimePeriodId: null, sortOrder: 2700 },
    { timePeriodId: 202701, label: "Jan", grain: "month", parentTimePeriodId: 202700, sortOrder: 2701 },
    { timePeriodId: 202702, label: "Feb", grain: "month", parentTimePeriodId: 202700, sortOrder: 2702 },
  ],
  rows: [
    {
      storeId: 101,
      productNodeId: 2000,
      label: "Store A",
      level: 0,
      path: ["Store A"],
      isLeaf: false,
      cells: {
        202600: { measures: { 1: zeroMeasureCell(1500), 2: zeroMeasureCell(300), 3: zeroMeasureCell(5), } },
        202601: { measures: { 1: zeroMeasureCell(700), 2: zeroMeasureCell(140), 3: zeroMeasureCell(5), } },
        202602: { measures: { 1: zeroMeasureCell(800), 2: zeroMeasureCell(160), 3: zeroMeasureCell(5), } },
        202700: { measures: { 1: zeroMeasureCell(0), 2: zeroMeasureCell(0), 3: zeroMeasureCell(1), } },
        202701: { measures: { 1: zeroMeasureCell(0), 2: zeroMeasureCell(0), 3: zeroMeasureCell(1), } },
        202702: { measures: { 1: zeroMeasureCell(0), 2: zeroMeasureCell(0), 3: zeroMeasureCell(1), } },
      },
    },
  ],
};
