import type { GridSliceResponse } from "./types";

const zeroMeasureCell = (value = 0, isLocked = false) => ({
  baseValue: value,
  value,
  growthFactor: 1,
  isLocked,
  isCalculated: true,
  isOverride: false,
  rowVersion: 1,
  cellKind: "calculated",
});

export const sampleGridData: GridSliceResponse = {
  scenarioVersionId: 1,
  measures: [
    { measureId: 1, label: "Sales Revenue", decimalPlaces: 0, derivedAtAggregateLevels: false, displayAsPercent: false, editableAtLeaf: true, editableAtAggregate: true },
    { measureId: 2, label: "Sold Qty", decimalPlaces: 0, derivedAtAggregateLevels: false, displayAsPercent: false, editableAtLeaf: true, editableAtAggregate: true },
    { measureId: 3, label: "ASP", decimalPlaces: 2, derivedAtAggregateLevels: true, displayAsPercent: false, editableAtLeaf: true, editableAtAggregate: true },
    { measureId: 4, label: "Unit Cost", decimalPlaces: 2, derivedAtAggregateLevels: true, displayAsPercent: false, editableAtLeaf: true, editableAtAggregate: true },
    { measureId: 5, label: "Total Costs", decimalPlaces: 0, derivedAtAggregateLevels: false, displayAsPercent: false, editableAtLeaf: false, editableAtAggregate: false },
    { measureId: 6, label: "GP", decimalPlaces: 0, derivedAtAggregateLevels: false, displayAsPercent: false, editableAtLeaf: false, editableAtAggregate: false },
    { measureId: 7, label: "GP%", decimalPlaces: 1, derivedAtAggregateLevels: true, displayAsPercent: true, editableAtLeaf: true, editableAtAggregate: true },
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
      nodeKind: "store",
      storeLabel: "Store A",
      clusterLabel: "Baby Mart",
      regionLabel: "Central",
      lifecycleState: "active",
      rampProfileCode: "new-store-ramp",
      effectiveFromTimePeriodId: null,
      effectiveToTimePeriodId: null,
      cells: {
        202600: { measures: { 1: zeroMeasureCell(1500), 2: zeroMeasureCell(300), 3: zeroMeasureCell(5), 4: zeroMeasureCell(3), 5: zeroMeasureCell(900), 6: zeroMeasureCell(600), 7: zeroMeasureCell(40) } },
        202601: { measures: { 1: zeroMeasureCell(700), 2: zeroMeasureCell(140), 3: zeroMeasureCell(5), 4: zeroMeasureCell(3), 5: zeroMeasureCell(420), 6: zeroMeasureCell(280), 7: zeroMeasureCell(40) } },
        202602: { measures: { 1: zeroMeasureCell(800), 2: zeroMeasureCell(160), 3: zeroMeasureCell(5), 4: zeroMeasureCell(3), 5: zeroMeasureCell(480), 6: zeroMeasureCell(320), 7: zeroMeasureCell(40) } },
        202700: { measures: { 1: zeroMeasureCell(0), 2: zeroMeasureCell(0), 3: zeroMeasureCell(1), 4: zeroMeasureCell(0.6), 5: zeroMeasureCell(0), 6: zeroMeasureCell(0), 7: zeroMeasureCell(40) } },
        202701: { measures: { 1: zeroMeasureCell(0), 2: zeroMeasureCell(0), 3: zeroMeasureCell(1), 4: zeroMeasureCell(0.6), 5: zeroMeasureCell(0), 6: zeroMeasureCell(0), 7: zeroMeasureCell(40) } },
        202702: { measures: { 1: zeroMeasureCell(0), 2: zeroMeasureCell(0), 3: zeroMeasureCell(1), 4: zeroMeasureCell(0.6), 5: zeroMeasureCell(0), 6: zeroMeasureCell(0), 7: zeroMeasureCell(40) } },
      },
    },
  ],
};
