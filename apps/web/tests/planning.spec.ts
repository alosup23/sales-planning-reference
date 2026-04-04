import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { expect, test, type APIRequestContext } from "@playwright/test";

const storeLabelsById: Record<number, string> = {
  101: "Store A",
  102: "Store B",
  103: "Store Z E2E",
};

const storeRowId = (storeId: number, productNodeId: number) =>
  `view:store:node:${storeId}:${productNodeId}:${encodeURIComponent(`Store Total>${storeLabelsById[storeId] ?? `Store ${storeId}`}`)}`;
const storeRootRowId = "view:store:root";
const departmentRootRowId = "view:department:root";

const editableMeasureScenarios = [
  { measureId: 1, label: "Sales Revenue", decimals: 0, displayAsPercent: false, yearFirst: "3900", yearSecond: "4200", monthFirst: "1350", monthSecond: "1425" },
  { measureId: 2, label: "Sold Qty", decimals: 0, displayAsPercent: false, yearFirst: "390", yearSecond: "420", monthFirst: "125", monthSecond: "145" },
  { measureId: 3, label: "ASP", decimals: 2, displayAsPercent: false, yearFirst: "12.25", yearSecond: "13.75", monthFirst: "11.75", monthSecond: "12.50" },
  { measureId: 4, label: "Unit Cost", decimals: 2, displayAsPercent: false, yearFirst: "7.25", yearSecond: "7.75", monthFirst: "7.10", monthSecond: "7.60" },
  { measureId: 7, label: "GP%", decimals: 1, displayAsPercent: true, yearFirst: "28.5", yearSecond: "31.0", monthFirst: "26.5", monthSecond: "29.0" },
] as const;

async function openGrid(page: import("@playwright/test").Page) {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Sales Budget & Planning" })).toBeVisible();
  await expectReady(page);
}

async function expectReady(page: import("@playwright/test").Page) {
  const statusCard = page.locator(".status-card");
  await expect(statusCard).toBeVisible();
  await expect(statusCard).not.toContainText("Loading planning slice...");
  await expect(statusCard).not.toContainText("Applying changes...");
}

async function expandYears(page: import("@playwright/test").Page) {
  await page.getByRole("button", { name: "Expand Years" }).click();
}

async function selectWorkspace(page: import("@playwright/test").Page, value: string) {
  await expect.poll(async () => {
    const workspaceSelect = page.locator(".view-menu-bar select").first();
    await workspaceSelect.selectOption(value);
    return await workspaceSelect.inputValue();
  }, {
    timeout: 15_000,
  }).toBe(value);

  if (value === "planning-department") {
    await expect(page.locator(".view-menu-bar .year-picker").filter({ hasText: "Layout" }).locator("select")).toBeVisible();
  }
}

async function selectStoreScope(page: import("@playwright/test").Page, storeId: string) {
  await page.locator(".view-menu-bar .year-picker").filter({ hasText: "Store Scope" }).locator("select").selectOption(storeId);
  await expectReady(page);
}

async function selectDepartmentLayout(page: import("@playwright/test").Page, value: "department-store-class" | "department-class-store") {
  await expect.poll(async () => {
    const layoutSelect = page.locator(".view-menu-bar .year-picker").filter({ hasText: "Layout" }).locator("select");
    await layoutSelect.selectOption(value);
    return await layoutSelect.inputValue();
  }, {
    timeout: 15_000,
  }).toBe(value);
}

async function toggleRowCaret(page: import("@playwright/test").Page, rowId: string) {
  const row = page.locator(`.ag-pinned-left-cols-container [row-id="${rowId}"]`);
  const toggle = row.locator(".hierarchy-toggle").first();
  await expect(toggle).toBeVisible();
  await toggle.click({ force: true });
}

async function expandRowById(page: import("@playwright/test").Page, rowId: string) {
  const row = page.locator(`.ag-pinned-left-cols-container [row-id="${rowId}"]`);
  if (await row.count() === 0) {
    return;
  }

  const toggle = row.locator(".hierarchy-toggle").first();
  await expect(toggle).toBeVisible();
  const ariaLabel = await toggle.getAttribute("aria-label");
  if (ariaLabel?.startsWith("Expand")) {
    await toggle.click({ force: true });
  }
}

async function toggleRowCaretByLabel(page: import("@playwright/test").Page, label: string) {
  let row = page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: label }).first();
  if (await row.count() === 0) {
    for (const rootRowId of [storeRootRowId, departmentRootRowId]) {
      await expandRowById(page, rootRowId);
      row = page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: label }).first();
      if (await row.count()) {
        break;
      }
    }
  }

  await expect(row).toBeVisible();
  const toggle = row.locator(".hierarchy-toggle").first();
  await expect(toggle).toBeVisible();
  await toggle.click({ force: true });
}

async function expandRowByLabel(page: import("@playwright/test").Page, label: string) {
  let row = page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: label }).first();
  if (await row.count() === 0) {
    for (const rootRowId of [storeRootRowId, departmentRootRowId]) {
      await expandRowById(page, rootRowId);
      row = page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: label }).first();
      if (await row.count()) {
        break;
      }
    }
  }

  await expect(row).toBeVisible();
  const toggle = row.locator(".hierarchy-toggle").first();
  await expect(toggle).toBeVisible();
  const ariaLabel = await toggle.getAttribute("aria-label");
  if (ariaLabel?.startsWith("Expand")) {
    await toggle.click({ force: true });
  }
}

async function expectToggleLabel(page: import("@playwright/test").Page, rowId: string, label: string) {
  await expect(page.locator(`.ag-pinned-left-cols-container [row-id="${rowId}"] .hierarchy-toggle`).first()).toHaveAttribute("aria-label", label);
}

async function ensurePinnedRowVisible(page: import("@playwright/test").Page, rowId: string, ancestorRowIds: string[] = []) {
  const row = page.locator(`.ag-pinned-left-cols-container [row-id="${rowId}"]`).first();
  if (await row.count()) {
    return;
  }

  for (const ancestorRowId of ancestorRowIds) {
    const ancestorRow = page.locator(`.ag-pinned-left-cols-container [row-id="${ancestorRowId}"]`).first();
    if (await ancestorRow.count()) {
      const hierarchyToggle = ancestorRow.locator(".hierarchy-toggle").first();
      if (await hierarchyToggle.count()) {
        const ariaLabel = await hierarchyToggle.getAttribute("aria-label");
        if (ariaLabel?.startsWith("Expand")) {
          await hierarchyToggle.click({ force: true });
        }
      }
    }

    if (await row.count()) {
      return;
    }
  }

  if (ancestorRowIds.length > 0 && await row.count() === 0) {
    for (const ancestorRowId of ancestorRowIds) {
      const ancestorRow = page.locator(`.ag-pinned-left-cols-container [row-id="${ancestorRowId}"]`).first();
      if (await ancestorRow.count() === 0) {
        continue;
      }

      const hierarchyToggle = ancestorRow.locator(".hierarchy-toggle").first();
      if (await hierarchyToggle.count() === 0) {
        continue;
      }

      const ariaLabel = await hierarchyToggle.getAttribute("aria-label");
      if (ariaLabel?.startsWith("Collapse")) {
        await hierarchyToggle.click({ force: true });
      }

      await hierarchyToggle.click({ force: true });
      if (await row.count()) {
        return;
      }
    }
  }

  await expect(row).toBeVisible();
}

async function gridCell(page: import("@playwright/test").Page, rowId: string, colId: string) {
  await ensurePinnedRowVisible(page, rowId, [storeRootRowId, departmentRootRowId]);
  await ensureGridColumnVisible(page, colId);
  const pinnedRow = page.locator(`.ag-pinned-left-cols-container [row-id="${rowId}"]`).first();
  await expect(pinnedRow).toBeVisible();
  return page.locator(`.ag-center-cols-container [row-id="${rowId}"] [col-id="${colId}"]`).first();
}

async function gridCellText(page: import("@playwright/test").Page, rowId: string, colId: string) {
  return (await gridCell(page, rowId, colId)).textContent();
}

async function gridCellByPinnedText(page: import("@playwright/test").Page, rowText: string, colId: string) {
  let pinnedRow = page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: rowText }).first();
  if (await pinnedRow.count() === 0) {
    for (const ancestorRowId of [storeRootRowId, departmentRootRowId]) {
      const ancestorRow = page.locator(`.ag-pinned-left-cols-container [row-id="${ancestorRowId}"]`).first();
      if (await ancestorRow.count()) {
        const hierarchyToggle = ancestorRow.locator(".hierarchy-toggle").first();
        if (await hierarchyToggle.count()) {
          const ariaLabel = await hierarchyToggle.getAttribute("aria-label");
          if (ariaLabel?.startsWith("Expand")) {
            await hierarchyToggle.click();
          }
        }
      }

      pinnedRow = page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: rowText }).first();
      if (await pinnedRow.count()) {
        break;
      }
    }
  }

  await expect(pinnedRow).toBeVisible();
  const rowId = await pinnedRow.getAttribute("row-id");
  expect(rowId).not.toBeNull();
  await ensureGridColumnVisible(page, colId);
  return page.locator(`.ag-center-cols-container [row-id="${rowId}"] [col-id="${colId}"]`).first();
}

async function ensureGridColumnVisible(page: import("@playwright/test").Page, colId: string) {
  await page.evaluate((targetColId) => {
    window.__planningGridTestApi?.ensureColumnVisible(targetColId);
  }, colId);
}

async function editCell(page: import("@playwright/test").Page, rowId: string, colId: string, nextValue: string) {
  const cell = await gridCell(page, rowId, colId);
  await beginCellEdit(page, cell, nextValue);
}

async function editCellByPinnedText(page: import("@playwright/test").Page, rowText: string, colId: string, nextValue: string) {
  const cell = await gridCellByPinnedText(page, rowText, colId);
  await beginCellEdit(page, cell, nextValue);
}

async function beginCellEdit(page: import("@playwright/test").Page, cell: import("@playwright/test").Locator, nextValue: string) {
  await cell.dblclick();
  const editorInput = page.locator(".ag-cell-inline-editing input").last();
  await expect(editorInput).toBeVisible();
  await editorInput.fill(nextValue);
  await page.keyboard.press("Enter");
}

async function savePlanningChanges(page: import("@playwright/test").Page) {
  const saveButton = page.getByRole("button", { name: "Save" });
  await expect(saveButton).toBeEnabled();
  await saveButton.click();
  await expect(page.locator(".status-card")).toContainText("All changes saved.");
  await expect(saveButton).toBeDisabled();
  await expectReady(page);
}

function formatGridValue(value: string, decimals: number, displayAsPercent: boolean) {
  const formatted = new Intl.NumberFormat("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(Number(value));
  return displayAsPercent ? `${formatted}%` : formatted;
}

async function openStoreLeafPath(
  page: import("@playwright/test").Page,
  departmentLabel: string,
  classLabel: string,
  subclassLabel: string,
) {
  await selectWorkspace(page, "planning-store");
  await expectReady(page);
  await expandRowById(page, storeRootRowId);
  await expectReady(page);
  await expandRowByLabel(page, "Store A");
  await expectReady(page);
  await expandRowByLabel(page, departmentLabel);
  await expectReady(page);
  await expandRowByLabel(page, classLabel);
  await expectReady(page);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: subclassLabel }).first()).toBeVisible();
}

async function openDepartmentLeafPath(
  page: import("@playwright/test").Page,
  departmentLabel: string,
  classLabel: string,
  subclassLabel: string,
) {
  await selectWorkspace(page, "planning-department");
  await expectReady(page);
  await selectDepartmentLayout(page, "department-store-class");
  await expectReady(page);
  await expandRowByLabel(page, departmentLabel);
  await expectReady(page);
  await expandRowByLabel(page, "Store A");
  await expectReady(page);
  await expandRowByLabel(page, classLabel);
  await expectReady(page);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: subclassLabel }).first()).toBeVisible();
}

async function normalizedCellText(cell: import("@playwright/test").Locator) {
  return ((await cell.textContent()) ?? "").replace(/\s+/g, "");
}

async function fetchJson<T>(request: APIRequestContext, url: string): Promise<T> {
  const response = await request.get(url);
  expect(response.ok()).toBeTruthy();
  return await response.json() as T;
}

async function fetchDepartmentLeafMeasureValue(
  request: APIRequestContext,
  departmentLabel: string,
  classLabel: string,
  subclassLabel: string,
  timePeriodId: number,
  measureId: number,
): Promise<number> {
  const root = await fetchJson<{ rows: Array<{ viewRowId: string }> }>(
    request,
    "/api/v1/grid-view-root?scenarioVersionId=1&view=department&departmentLayout=department-store-class",
  );
  const rootRowId = root.rows[0]?.viewRowId;
  expect(rootRowId).toBeTruthy();

  const departmentRows = await fetchJson<{ rows: Array<{ label: string; viewRowId: string }> }>(
    request,
    `/api/v1/grid-view-children?scenarioVersionId=1&view=department&departmentLayout=department-store-class&parentViewRowId=${encodeURIComponent(rootRowId!)}`,
  );
  const departmentRow = departmentRows.rows.find((row) => row.label === departmentLabel);
  expect(departmentRow).toBeTruthy();

  const storeRows = await fetchJson<{ rows: Array<{ label: string; viewRowId: string }> }>(
    request,
    `/api/v1/grid-view-children?scenarioVersionId=1&view=department&departmentLayout=department-store-class&parentViewRowId=${encodeURIComponent(departmentRow!.viewRowId)}`,
  );
  const storeRow = storeRows.rows.find((row) => row.label === "Store A");
  expect(storeRow).toBeTruthy();

  const classRows = await fetchJson<{ rows: Array<{ label: string; viewRowId: string }> }>(
    request,
    `/api/v1/grid-view-children?scenarioVersionId=1&view=department&departmentLayout=department-store-class&parentViewRowId=${encodeURIComponent(storeRow!.viewRowId)}`,
  );
  const classRow = classRows.rows.find((row) => row.label === classLabel);
  expect(classRow).toBeTruthy();

  const leafRows = await fetchJson<{ rows: Array<{ label: string; cells: Record<string, { measures: Record<string, { value: number }> }> }> }>(
    request,
    `/api/v1/grid-view-children?scenarioVersionId=1&view=department&departmentLayout=department-store-class&parentViewRowId=${encodeURIComponent(classRow!.viewRowId)}`,
  );
  const leafRow = leafRows.rows.find((row) => row.label === subclassLabel);
  expect(leafRow).toBeTruthy();

  const value = leafRow?.cells[String(timePeriodId)]?.measures[String(measureId)]?.value;
  expect(value).not.toBeUndefined();
  return Number(value);
}

async function fetchStoreLeafMeasureValue(
  request: APIRequestContext,
  departmentLabel: string,
  classLabel: string,
  subclassLabel: string,
  timePeriodId: number,
  measureId: number,
): Promise<number> {
  const root = await fetchJson<{ rows: Array<{ label: string; viewRowId: string }> }>(
    request,
    "/api/v1/grid-view-root?scenarioVersionId=1&view=store",
  );
  const rootRowId = root.rows[0]?.viewRowId;
  expect(rootRowId).toBeTruthy();

  const storeRows = await fetchJson<{ rows: Array<{ label: string; viewRowId: string }> }>(
    request,
    `/api/v1/grid-view-children?scenarioVersionId=1&view=store&parentViewRowId=${encodeURIComponent(rootRowId!)}`,
  );
  const storeRow = storeRows.rows.find((row) => row.label === "Store A");
  expect(storeRow).toBeTruthy();

  const departmentRows = await fetchJson<{ rows: Array<{ label: string; viewRowId: string }> }>(
    request,
    `/api/v1/grid-view-children?scenarioVersionId=1&view=store&parentViewRowId=${encodeURIComponent(storeRow!.viewRowId)}`,
  );
  const departmentRow = departmentRows.rows.find((row) => row.label === departmentLabel);
  expect(departmentRow).toBeTruthy();

  const classRows = await fetchJson<{ rows: Array<{ label: string; viewRowId: string }> }>(
    request,
    `/api/v1/grid-view-children?scenarioVersionId=1&view=store&parentViewRowId=${encodeURIComponent(departmentRow!.viewRowId)}`,
  );
  const classRow = classRows.rows.find((row) => row.label === classLabel);
  expect(classRow).toBeTruthy();

  const leafRows = await fetchJson<{ rows: Array<{ label: string; cells: Record<string, { measures: Record<string, { value: number }> }> }> }>(
    request,
    `/api/v1/grid-view-children?scenarioVersionId=1&view=store&parentViewRowId=${encodeURIComponent(classRow!.viewRowId)}`,
  );
  const leafRow = leafRows.rows.find((row) => row.label === subclassLabel);
  expect(leafRow).toBeTruthy();

  const value = leafRow?.cells[String(timePeriodId)]?.measures[String(measureId)]?.value;
  expect(value).not.toBeUndefined();
  return Number(value);
}

async function acceptPrompts(page: import("@playwright/test").Page, responses: string[]) {
  let responseIndex = 0;
  const handler = async (dialog: import("@playwright/test").Dialog) => {
    const response = responses[responseIndex] ?? "";
    responseIndex += 1;
    await dialog.accept(response);
  };

  page.on("dialog", handler);
  return () => page.off("dialog", handler);
}

function createWorkbookBuffer(rows: Array<Record<string, string | number>>, sheetName = "Store B"): Buffer {
  const tempRoot = mkdtempSync(path.join(tmpdir(), "sales-plan-xlsx-"));
  const workbookRoot = path.join(tempRoot, "xlsx");
  const cleanup = () => rmSync(tempRoot, { recursive: true, force: true });

  const xmlEscape = (value: string) => value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&apos;");

  const headers = ["Store", "Department", "Class", "Subclass", "Year", "Month", "Sales Revenue", "Sold Qty", "ASP", "Unit Cost", "Total Costs", "GP", "GP%"];
  const allRows = [headers, ...rows.map((row) => headers.map((header) => row[header] ?? ""))];

  const cellRef = (columnIndex: number, rowIndex: number) => `${String.fromCharCode(65 + columnIndex)}${rowIndex}`;
  const cellXml = (value: string | number, columnIndex: number, rowIndex: number) => {
    if (typeof value === "number") {
      return `<c r="${cellRef(columnIndex, rowIndex)}"><v>${value}</v></c>`;
    }

    return `<c r="${cellRef(columnIndex, rowIndex)}" t="inlineStr"><is><t>${xmlEscape(value)}</t></is></c>`;
  };

  const sheetRows = allRows
    .map((row, rowIndex) => `<row r="${rowIndex + 1}">${row.map((value, columnIndex) => cellXml(value, columnIndex, rowIndex + 1)).join("")}</row>`)
    .join("");

  mkdirSync(path.join(workbookRoot, "_rels"), { recursive: true });
  mkdirSync(path.join(workbookRoot, "xl", "_rels"), { recursive: true });
  mkdirSync(path.join(workbookRoot, "xl", "worksheets"), { recursive: true });

  writeFileSync(
    path.join(workbookRoot, "[Content_Types].xml"),
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>`,
  );

  writeFileSync(
    path.join(workbookRoot, "_rels", ".rels"),
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>`,
  );

  writeFileSync(
    path.join(workbookRoot, "xl", "workbook.xml"),
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="${xmlEscape(sheetName)}" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>`,
  );

  writeFileSync(
    path.join(workbookRoot, "xl", "_rels", "workbook.xml.rels"),
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>`,
  );

  writeFileSync(
    path.join(workbookRoot, "xl", "styles.xml"),
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
  <fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
  <borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
  <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
  <cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>
  <cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>`,
  );

  writeFileSync(
    path.join(workbookRoot, "xl", "worksheets", "sheet1.xml"),
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>${sheetRows}</sheetData>
</worksheet>`,
  );

  const workbookPath = path.join(tempRoot, "import.xlsx");
  execFileSync("zip", ["-X", "-q", "-r", workbookPath, "."], { cwd: workbookRoot });
  const buffer = readFileSync(workbookPath);
  cleanup();
  return buffer;
}

test.describe.configure({ mode: "serial" });

test.beforeEach(async ({ page, request }) => {
  await request.post("http://127.0.0.1:5081/api/v1/test/reset");
  await openGrid(page);
});

test("loads with collapsed years, readable measure labels, and the first store level expanded by default", async ({ page }) => {
  await expect(page.locator(".ag-header-group-text", { hasText: "FY26" }).first()).toBeVisible();
  await expect(page.locator(".ag-header-cell-text", { hasText: "Sales Revenue" }).first()).toBeVisible();
  await expect(page.locator(".ag-header-cell-text", { hasText: "Sold Qty" }).first()).toBeVisible();
  await expect(page.locator(".ag-header-group-text", { hasText: "Jan" })).toHaveCount(0);
  await expect(page.locator(`.ag-pinned-left-cols-container [row-id="${storeRootRowId}"]`)).toBeVisible();
  await expect(page.locator(`.ag-pinned-left-cols-container [row-id="${storeRowId(101, 2000)}"]`)).toBeVisible();
  await expect(page.locator(`.ag-pinned-left-cols-container [row-id="${storeRowId(101, 2100)}"]`)).toHaveCount(0);
  await expect(await gridCell(page, storeRowId(101, 2000), "202600:1")).toContainText(/[0-9,]+/);
  await expect((await gridCell(page, storeRowId(101, 2000), "202600:1")).locator(".measure-value")).toHaveCSS("text-align", "right");
  await expect(await gridCell(page, storeRowId(101, 2000), "202600:1")).toHaveCSS("background-color", "rgb(255, 255, 255)");
  await expect(await gridCell(page, storeRowId(101, 2000), "202700:1")).toHaveCSS("background-color", "rgb(242, 242, 242)");
});

test("supports expand and collapse controls for rows and years", async ({ page }) => {
  const storeARow = page.locator(`.ag-pinned-left-cols-container [row-id="${storeRowId(101, 2000)}"]`);

  await expect(storeARow).toBeVisible();
  await expectToggleLabel(page, storeRootRowId, "Collapse Store Total");
  await expectToggleLabel(page, storeRowId(101, 2000), "Expand Store A");

  await toggleRowCaret(page, storeRootRowId);
  await expectReady(page);
  await expect(storeARow).toHaveCount(0);
  await expectToggleLabel(page, storeRootRowId, "Expand Store Total");

  await page.getByRole("button", { name: "Expand Years" }).click();
  await expect(page.locator(".ag-header-group-text", { hasText: "FY26" }).first()).toBeVisible();
});

test("keeps year groups usable after scoped refreshes", async ({ page }) => {
  await page.getByRole("button", { name: "Expand Years" }).click();
  await expectReady(page);
  await ensureGridColumnVisible(page, "202601:1");
  await expect(page.locator('.ag-center-cols-container [col-id="202601:1"]').first()).toBeVisible();
  await selectStoreScope(page, "102");
  await expectReady(page);

  await page.getByRole("button", { name: "Expand Years" }).click();
  await expectReady(page);
  await ensureGridColumnVisible(page, "202601:1");
  await expect(page.locator('.ag-center-cols-container [col-id="202601:1"]').first()).toBeVisible();
});

test("does not pin year total measure columns and keeps caret expansion working in every view", async ({ page }) => {
  await expect(page.locator(`.ag-pinned-left-cols-container [col-id="202600:1"]`)).toHaveCount(0);

  await expectToggleLabel(page, storeRootRowId, "Collapse Store Total");
  await toggleRowCaret(page, storeRootRowId);
  await expectReady(page);
  await expectToggleLabel(page, storeRootRowId, "Expand Store Total");
  await toggleRowCaret(page, storeRootRowId);
  await expectReady(page);
  await expectToggleLabel(page, storeRootRowId, "Collapse Store Total");

  await selectWorkspace(page, "planning-department");
  await expectReady(page);
  await expectToggleLabel(page, departmentRootRowId, "Expand Department Total");
  await toggleRowCaret(page, departmentRootRowId);
  await expectReady(page);
  await expectToggleLabel(page, departmentRootRowId, "Collapse Department Total");
  await toggleRowCaret(page, departmentRootRowId);
  await expectReady(page);
  await expectToggleLabel(page, departmentRootRowId, "Expand Department Total");
});

test("renders the correct department hierarchy order in both layouts and applies aggregate color bands", async ({ page }) => {
  await selectWorkspace(page, "planning-department");
  await expectReady(page);

  await expect(page.locator(".view-menu-bar .year-picker").filter({ hasText: "Department Scope" }).locator("select")).toHaveValue("");
  await expect(page.locator(`.ag-pinned-left-cols-container [row-id="${departmentRootRowId}"] .ag-cell`).first()).toHaveCSS("background-color", "rgb(221, 221, 221)");
  await expect(page.locator(`.ag-pinned-left-cols-container [row-id="${departmentRootRowId}"]`)).toHaveClass(/row-band-level-0/);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Beverages" })).toHaveCount(0);
  await toggleRowCaret(page, departmentRootRowId);
  await expectReady(page);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Beverages" }).first()).toBeVisible();
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store A" })).toHaveCount(0);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store B" })).toHaveCount(0);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Soft Drinks" })).toHaveCount(0);

  await toggleRowCaretByLabel(page, "Beverages");
  await expectReady(page);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store A" }).first()).toBeVisible();
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store B" }).first()).toBeVisible();
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Soft Drinks" })).toHaveCount(0);

  await selectDepartmentLayout(page, "department-class-store");
  await expectReady(page);
  await expect(page.locator(`.ag-pinned-left-cols-container [row-id="${departmentRootRowId}"]`)).toBeVisible();
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store A" })).toHaveCount(0);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store B" })).toHaveCount(0);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Soft Drinks" })).toHaveCount(0);

  await toggleRowCaretByLabel(page, "Beverages");
  await expectReady(page);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Soft Drinks" }).first()).toBeVisible();
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: /^Store A$/ })).toHaveCount(0);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: /^Store B$/ })).toHaveCount(0);

  await toggleRowCaretByLabel(page, "Soft Drinks");
  await expectReady(page);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store A" }).first()).toBeVisible();
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store B" }).first()).toBeVisible();
});

test("supports All Stores scope and keeps department expansion independent from store scope", async ({ page }) => {
  await openGrid(page);

  await selectStoreScope(page, "all");
  await toggleRowCaret(page, storeRootRowId);
  await expectReady(page);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store A" }).first()).toBeVisible();
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store B" }).first()).toBeVisible();

  await selectStoreScope(page, "101");
  await selectWorkspace(page, "planning-department");
  await expectReady(page);
  await toggleRowCaretByLabel(page, "Beverages");
  await expectReady(page);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store A" }).first()).toBeVisible();
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store B" }).first()).toBeVisible();
});

test("renders every Phase 1 maintenance workspace with core CRUD and import export affordances", async ({ page }) => {
  await openGrid(page);

  await selectWorkspace(page, "hierarchy");
  await expectReady(page);
  await expect(page.getByText("Department / Class / Subclass Mapping")).toBeVisible();
  await expect(page.getByRole("button", { name: "Add Department" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Add Class" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Add Subclass" })).toBeVisible();

  await selectWorkspace(page, "store-profile");
  await expectReady(page);
  await expect(page.getByRole("heading", { name: "Store Profile" })).toBeVisible();
  await expect(page.getByRole("button", { name: "New Store" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Export Store Profiles" })).toBeVisible();
  await expect(page.getByText("Option maintenance")).toBeVisible();

  await selectWorkspace(page, "product-profile");
  await expectReady(page);
  await expect(page.getByRole("heading", { name: "Product Profile" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Export Product Profiles" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Save Hierarchy Row" })).toBeVisible();
  await expect(page.getByText("Option maintenance")).toBeVisible();

  await selectWorkspace(page, "inventory-profile");
  await expectReady(page);
  await expect(page.getByRole("heading", { name: "Inventory Profile" })).toBeVisible();
  await expect(page.getByRole("button", { name: "New Inventory Row" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Export Inventory Profile" })).toBeVisible();

  await selectWorkspace(page, "pricing-policy");
  await expectReady(page);
  await expect(page.getByRole("heading", { name: "Pricing Policy" })).toBeVisible();
  await expect(page.getByRole("button", { name: "New Policy" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Export Pricing Policy" })).toBeVisible();

  await selectWorkspace(page, "seasonality-event");
  await expectReady(page);
  await expect(page.getByRole("heading", { name: "Seasonality & Events" })).toBeVisible();
  await expect(page.getByRole("button", { name: "New Seasonality Row" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Export Seasonality & Events" })).toBeVisible();

  await selectWorkspace(page, "vendor-supply");
  await expectReady(page);
  await expect(page.getByRole("heading", { name: "Vendor Supply Profile" })).toBeVisible();
  await expect(page.getByRole("button", { name: "New Vendor Row" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Export Vendor Supply Profile" })).toBeVisible();
});

test("editing a visible Sales Revenue month persists the updated value", async ({ page }) => {
  await expandYears(page);
  const monthRevenueCol = "202601:1";
  const before = await (await gridCellByPinnedText(page, "Store A", monthRevenueCol)).textContent();

  await editCellByPinnedText(page, "Store A", monthRevenueCol, "12000");
  await expectReady(page);
  await expandYears(page);
  await expect(await gridCellByPinnedText(page, "Store A", monthRevenueCol)).toContainText("12,000");
  expect(before).not.toContain("12,000");
});

test("department view remains available after a visible store edit", async ({ page }) => {
  await editCellByPinnedText(page, "Store A", "202600:1", "12000");
  await expectReady(page);

  await selectWorkspace(page, "planning-department");
  await expectReady(page);
  await expect(page.locator(`.ag-center-cols-container [row-id="${departmentRootRowId}"] [col-id="202600:1"]`).first()).toContainText(/[0-9,]+/);
});

test("store leaf edits propagate to the department view aggregates", async ({ page }) => {
  await expandYears(page);
  await selectWorkspace(page, "planning-department");
  await expectReady(page);
  await expandYears(page);
  await expandRowByLabel(page, "Beverages");
  await expectReady(page);
  const beforeDepartmentCell = await gridCellByPinnedText(page, "Store A", "202601:1");
  const beforeDepartmentValue = (await beforeDepartmentCell.textContent())?.replace(/\s+/g, "") ?? "";

  await selectWorkspace(page, "planning-store");
  await expectReady(page);
  await expandRowById(page, storeRootRowId);
  await expandRowByLabel(page, "Store A");
  await expectReady(page);
  await expandRowByLabel(page, "Beverages");
  await expectReady(page);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Soft Drinks" }).first()).toBeVisible();
  await expandRowByLabel(page, "Soft Drinks");
  await expectReady(page);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Cola" }).first()).toBeVisible();
  await editCellByPinnedText(page, "Cola", "202601:1", "12345");
  await expectReady(page);

  await selectWorkspace(page, "planning-department");
  await expectReady(page);
  await expandYears(page);
  await expandRowByLabel(page, "Beverages");
  await expectReady(page);
  const afterDepartmentCell = await gridCellByPinnedText(page, "Store A", "202601:1");
  const afterDepartmentValue = (await afterDepartmentCell.textContent())?.replace(/\s+/g, "") ?? "";

  expect(afterDepartmentValue).not.toBe(beforeDepartmentValue);
});

test("store leaf year edits propagate to the department view aggregates", async ({ page }) => {
  await selectWorkspace(page, "planning-department");
  await expectReady(page);
  await expandRowByLabel(page, "Beverages");
  await expectReady(page);
  const beforeDepartmentCell = await gridCellByPinnedText(page, "Store A", "202600:1");
  const beforeDepartmentValue = (await beforeDepartmentCell.textContent())?.replace(/\s+/g, "") ?? "";

  await selectWorkspace(page, "planning-store");
  await expectReady(page);
  await expandRowById(page, storeRootRowId);
  await ensurePinnedRowVisible(page, storeRowId(101, 2000), [storeRootRowId]);
  await expandRowById(page, storeRowId(101, 2000));
  await expectReady(page);
  await expandRowByLabel(page, "Beverages");
  await expectReady(page);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Soft Drinks" }).first()).toBeVisible();
  await expandRowByLabel(page, "Soft Drinks");
  await expectReady(page);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Cola" }).first()).toBeVisible();
  await editCellByPinnedText(page, "Cola", "202600:1", "54321");
  await expectReady(page);

  await selectWorkspace(page, "planning-department");
  await expectReady(page);
  await expandRowByLabel(page, "Beverages");
  await expectReady(page);
  const afterDepartmentCell = await gridCellByPinnedText(page, "Store A", "202600:1");
  const afterDepartmentValue = (await afterDepartmentCell.textContent())?.replace(/\s+/g, "") ?? "";

  expect(afterDepartmentValue).not.toBe(beforeDepartmentValue);
});

test("year Sold Qty edits survive save, remain visible across the department view, and keep department ancestors rolling up", async ({ page }) => {
  await selectWorkspace(page, "planning-store");
  await expectReady(page);
  await expandRowById(page, storeRootRowId);
  await expectReady(page);
  await expandRowByLabel(page, "Store A");
  await expectReady(page);
  await expandRowByLabel(page, "Beverages");
  await expectReady(page);
  await expandRowByLabel(page, "Soft Drinks");
  await expectReady(page);

  await editCellByPinnedText(page, "Cola", "202600:2", "390");
  await expectReady(page);
  await savePlanningChanges(page);

  await selectWorkspace(page, "planning-department");
  await expectReady(page);
  await selectDepartmentLayout(page, "department-store-class");
  await expectReady(page);
  await expandRowByLabel(page, "Beverages");
  await expectReady(page);
  await expandRowByLabel(page, "Store A");
  await expectReady(page);
  await expandRowByLabel(page, "Soft Drinks");
  await expectReady(page);

  const departmentLeafCell = await gridCellByPinnedText(page, "Cola", "202600:2");
  await expect(departmentLeafCell).toContainText("390");

  const departmentStoreBefore = await normalizedCellText(await gridCellByPinnedText(page, "Store A", "202600:2"));
  const departmentTotalBefore = await normalizedCellText(await gridCellByPinnedText(page, "Beverages", "202600:2"));

  await editCellByPinnedText(page, "Cola", "202600:2", "420");
  await expectReady(page);

  await expect(await gridCellByPinnedText(page, "Cola", "202600:2")).toContainText("420");
  const departmentStoreAfter = await normalizedCellText(await gridCellByPinnedText(page, "Store A", "202600:2"));
  const departmentTotalAfter = await normalizedCellText(await gridCellByPinnedText(page, "Beverages", "202600:2"));

  expect(departmentStoreAfter).not.toBe(departmentStoreBefore);
  expect(departmentTotalAfter).not.toBe(departmentTotalBefore);
});

test("direct API year GP% splash returns the exact requested patch", async ({ request }) => {
  const response = await request.post("http://127.0.0.1:5081/api/v1/actions/splash", {
    data: {
      scenarioVersionId: 1,
      measureId: 7,
      sourceCell: {
        storeId: 101,
        productNodeId: 2111,
        timePeriodId: 202600,
      },
      totalValue: 28.5,
      method: "seasonality_profile",
      roundingScale: 1,
      comment: "Direct API GP% splash",
      scopeRoots: [{ storeId: 101, productNodeId: 2111 }],
    },
  });
  expect(response.ok()).toBeTruthy();

  const body = await response.json() as {
    patch?: {
      cells?: Array<{
        storeId: number;
        productNodeId: number;
        timePeriodId: number;
        measureId: number;
        cell: { value: number };
      }>;
    };
  };
  const patchCell = body.patch?.cells?.find((cell) =>
    cell.storeId === 101 &&
    cell.productNodeId === 2111 &&
    cell.timePeriodId === 202600 &&
    cell.measureId === 7);
  expect(patchCell?.cell.value).toBe(28.5);
});

for (const scenario of editableMeasureScenarios) {
  test(`year ${scenario.label} draft edits remain visible across cross-view repeated editing without save`, async ({ page, request }) => {
    await openStoreLeafPath(page, "Beverages", "Soft Drinks", "Cola");
    await editCellByPinnedText(page, "Cola", `202600:${scenario.measureId}`, scenario.yearFirst);
    await expectReady(page);

    const draftYearStoreApiValue = await fetchStoreLeafMeasureValue(request, "Beverages", "Soft Drinks", "Cola", 202600, scenario.measureId);
    const draftYearDepartmentApiValue = await fetchDepartmentLeafMeasureValue(request, "Beverages", "Soft Drinks", "Cola", 202600, scenario.measureId);
    expect(draftYearStoreApiValue).toBe(Number(scenario.yearFirst));
    expect(draftYearDepartmentApiValue).toBe(Number(scenario.yearFirst));

    await openDepartmentLeafPath(page, "Beverages", "Soft Drinks", "Cola");
    await expect(await gridCellByPinnedText(page, "Cola", `202600:${scenario.measureId}`))
      .toContainText(formatGridValue(scenario.yearFirst, scenario.decimals, scenario.displayAsPercent));

    const departmentStoreBefore = await normalizedCellText(await gridCellByPinnedText(page, "Store A", `202600:${scenario.measureId}`));
    const departmentTotalBefore = await normalizedCellText(await gridCellByPinnedText(page, "Beverages", `202600:${scenario.measureId}`));

    await editCellByPinnedText(page, "Cola", `202600:${scenario.measureId}`, scenario.yearSecond);
    await expectReady(page);

    const updatedDraftYearStoreApiValue = await fetchStoreLeafMeasureValue(request, "Beverages", "Soft Drinks", "Cola", 202600, scenario.measureId);
    const updatedDraftYearDepartmentApiValue = await fetchDepartmentLeafMeasureValue(request, "Beverages", "Soft Drinks", "Cola", 202600, scenario.measureId);
    expect(updatedDraftYearStoreApiValue).toBe(Number(scenario.yearSecond));
    expect(updatedDraftYearDepartmentApiValue).toBe(Number(scenario.yearSecond));

    await expect(await gridCellByPinnedText(page, "Cola", `202600:${scenario.measureId}`))
      .toContainText(formatGridValue(scenario.yearSecond, scenario.decimals, scenario.displayAsPercent));

    const departmentStoreAfter = await normalizedCellText(await gridCellByPinnedText(page, "Store A", `202600:${scenario.measureId}`));
    const departmentTotalAfter = await normalizedCellText(await gridCellByPinnedText(page, "Beverages", `202600:${scenario.measureId}`));

    expect(departmentStoreAfter).not.toBe(departmentStoreBefore);
    expect(departmentTotalAfter).not.toBe(departmentTotalBefore);

    await openStoreLeafPath(page, "Beverages", "Soft Drinks", "Cola");
    await expect(await gridCellByPinnedText(page, "Cola", `202600:${scenario.measureId}`))
      .toContainText(formatGridValue(scenario.yearSecond, scenario.decimals, scenario.displayAsPercent));
  });

  test(`year ${scenario.label} edits survive save and repeated cross-view editing`, async ({ page, request }) => {
    await openStoreLeafPath(page, "Beverages", "Soft Drinks", "Cola");
    const splashResponsePromise = scenario.measureId === 7
      ? page.waitForResponse((response) =>
        response.request().method() === "POST" &&
        response.url().includes("/api/v1/actions/splash"))
      : null;
    await editCellByPinnedText(page, "Cola", `202600:${scenario.measureId}`, scenario.yearFirst);
    await expectReady(page);

    if (scenario.measureId === 7 && splashResponsePromise) {
      const splashResponse = await splashResponsePromise;
      const splashRequest = splashResponse.request().postDataJSON() as {
        sourceCell: { storeId: number; productNodeId: number; timePeriodId: number };
        totalValue: number;
        scopeRoots?: Array<{ storeId: number; productNodeId: number }>;
      };
      expect(splashRequest.sourceCell).toEqual({ storeId: 101, productNodeId: 2111, timePeriodId: 202600 });
      expect(splashRequest.totalValue).toBe(28.5);
      expect(splashRequest.scopeRoots).toEqual([{ storeId: 101, productNodeId: 2111 }]);

      const splashBody = await splashResponse.json() as {
        patch?: {
          cells?: Array<{
            storeId: number;
            productNodeId: number;
            timePeriodId: number;
            measureId: number;
            cell: { value: number };
          }>;
        };
      };
      const gpPatchCell = splashBody.patch?.cells?.find((cell) =>
        cell.storeId === 101 &&
        cell.productNodeId === 2111 &&
        cell.timePeriodId === 202600 &&
        cell.measureId === 7);
      expect(gpPatchCell?.cell.value).toBe(28.5);
    }

    await savePlanningChanges(page);

    const yearStoreApiValue = await fetchStoreLeafMeasureValue(request, "Beverages", "Soft Drinks", "Cola", 202600, scenario.measureId);
    const yearDepartmentApiValue = await fetchDepartmentLeafMeasureValue(request, "Beverages", "Soft Drinks", "Cola", 202600, scenario.measureId);
    expect(yearStoreApiValue).toBe(Number(scenario.yearFirst));
    expect(yearDepartmentApiValue).toBe(Number(scenario.yearFirst));

    await openDepartmentLeafPath(page, "Beverages", "Soft Drinks", "Cola");
    await expect(await gridCellByPinnedText(page, "Cola", `202600:${scenario.measureId}`))
      .toContainText(formatGridValue(scenario.yearFirst, scenario.decimals, scenario.displayAsPercent));

    const departmentStoreBefore = await normalizedCellText(await gridCellByPinnedText(page, "Store A", `202600:${scenario.measureId}`));
    const departmentTotalBefore = await normalizedCellText(await gridCellByPinnedText(page, "Beverages", `202600:${scenario.measureId}`));

    await editCellByPinnedText(page, "Cola", `202600:${scenario.measureId}`, scenario.yearSecond);
    await expectReady(page);

    await expect(await gridCellByPinnedText(page, "Cola", `202600:${scenario.measureId}`))
      .toContainText(formatGridValue(scenario.yearSecond, scenario.decimals, scenario.displayAsPercent));

    const departmentStoreAfter = await normalizedCellText(await gridCellByPinnedText(page, "Store A", `202600:${scenario.measureId}`));
    const departmentTotalAfter = await normalizedCellText(await gridCellByPinnedText(page, "Beverages", `202600:${scenario.measureId}`));

    expect(departmentStoreAfter).not.toBe(departmentStoreBefore);
    expect(departmentTotalAfter).not.toBe(departmentTotalBefore);

    await openStoreLeafPath(page, "Beverages", "Soft Drinks", "Cola");
    await expect(await gridCellByPinnedText(page, "Cola", `202600:${scenario.measureId}`))
      .toContainText(formatGridValue(scenario.yearSecond, scenario.decimals, scenario.displayAsPercent));
  });

  test(`month ${scenario.label} edits survive save and repeated cross-view editing`, async ({ page, request }) => {
    await openStoreLeafPath(page, "Beverages", "Tea", "Green Tea");
    await expandYears(page);
    await expectReady(page);
    await editCellByPinnedText(page, "Green Tea", `202603:${scenario.measureId}`, scenario.monthFirst);
    await expectReady(page);
    await savePlanningChanges(page);

    const monthStoreApiValue = await fetchStoreLeafMeasureValue(request, "Beverages", "Tea", "Green Tea", 202603, scenario.measureId);
    const monthDepartmentApiValue = await fetchDepartmentLeafMeasureValue(request, "Beverages", "Tea", "Green Tea", 202603, scenario.measureId);
    expect(monthStoreApiValue).toBe(Number(scenario.monthFirst));
    expect(monthDepartmentApiValue).toBe(Number(scenario.monthFirst));

    await openDepartmentLeafPath(page, "Beverages", "Tea", "Green Tea");
    await expandYears(page);
    await expectReady(page);
    await expect(await gridCellByPinnedText(page, "Green Tea", `202603:${scenario.measureId}`))
      .toContainText(formatGridValue(scenario.monthFirst, scenario.decimals, scenario.displayAsPercent));

    const departmentStoreBefore = await normalizedCellText(await gridCellByPinnedText(page, "Store A", `202603:${scenario.measureId}`));
    const departmentTotalBefore = await normalizedCellText(await gridCellByPinnedText(page, "Beverages", `202603:${scenario.measureId}`));

    await editCellByPinnedText(page, "Green Tea", `202603:${scenario.measureId}`, scenario.monthSecond);
    await expectReady(page);

    await expect(await gridCellByPinnedText(page, "Green Tea", `202603:${scenario.measureId}`))
      .toContainText(formatGridValue(scenario.monthSecond, scenario.decimals, scenario.displayAsPercent));

    const departmentStoreAfter = await normalizedCellText(await gridCellByPinnedText(page, "Store A", `202603:${scenario.measureId}`));
    const departmentTotalAfter = await normalizedCellText(await gridCellByPinnedText(page, "Beverages", `202603:${scenario.measureId}`));

    expect(departmentStoreAfter).not.toBe(departmentStoreBefore);
    expect(departmentTotalAfter).not.toBe(departmentTotalBefore);

    await openStoreLeafPath(page, "Beverages", "Tea", "Green Tea");
    await expandYears(page);
    await expectReady(page);
    await expect(await gridCellByPinnedText(page, "Green Tea", `202603:${scenario.measureId}`))
      .toContainText(formatGridValue(scenario.monthSecond, scenario.decimals, scenario.displayAsPercent));
  });

  test(`month ${scenario.label} draft edits remain visible across cross-view repeated editing without save`, async ({ page, request }) => {
    await openStoreLeafPath(page, "Beverages", "Tea", "Green Tea");
    await expandYears(page);
    await expectReady(page);
    await editCellByPinnedText(page, "Green Tea", `202603:${scenario.measureId}`, scenario.monthFirst);
    await expectReady(page);

    const draftMonthStoreApiValue = await fetchStoreLeafMeasureValue(request, "Beverages", "Tea", "Green Tea", 202603, scenario.measureId);
    const draftMonthDepartmentApiValue = await fetchDepartmentLeafMeasureValue(request, "Beverages", "Tea", "Green Tea", 202603, scenario.measureId);
    expect(draftMonthStoreApiValue).toBe(Number(scenario.monthFirst));
    expect(draftMonthDepartmentApiValue).toBe(Number(scenario.monthFirst));

    await openDepartmentLeafPath(page, "Beverages", "Tea", "Green Tea");
    await expandYears(page);
    await expectReady(page);
    await expect(await gridCellByPinnedText(page, "Green Tea", `202603:${scenario.measureId}`))
      .toContainText(formatGridValue(scenario.monthFirst, scenario.decimals, scenario.displayAsPercent));

    const departmentStoreBefore = await normalizedCellText(await gridCellByPinnedText(page, "Store A", `202603:${scenario.measureId}`));
    const departmentTotalBefore = await normalizedCellText(await gridCellByPinnedText(page, "Beverages", `202603:${scenario.measureId}`));

    await editCellByPinnedText(page, "Green Tea", `202603:${scenario.measureId}`, scenario.monthSecond);
    await expectReady(page);

    const updatedDraftMonthStoreApiValue = await fetchStoreLeafMeasureValue(request, "Beverages", "Tea", "Green Tea", 202603, scenario.measureId);
    const updatedDraftMonthDepartmentApiValue = await fetchDepartmentLeafMeasureValue(request, "Beverages", "Tea", "Green Tea", 202603, scenario.measureId);
    expect(updatedDraftMonthStoreApiValue).toBe(Number(scenario.monthSecond));
    expect(updatedDraftMonthDepartmentApiValue).toBe(Number(scenario.monthSecond));

    await expect(await gridCellByPinnedText(page, "Green Tea", `202603:${scenario.measureId}`))
      .toContainText(formatGridValue(scenario.monthSecond, scenario.decimals, scenario.displayAsPercent));

    const departmentStoreAfter = await normalizedCellText(await gridCellByPinnedText(page, "Store A", `202603:${scenario.measureId}`));
    const departmentTotalAfter = await normalizedCellText(await gridCellByPinnedText(page, "Beverages", `202603:${scenario.measureId}`));

    expect(departmentStoreAfter).not.toBe(departmentStoreBefore);
    expect(departmentTotalAfter).not.toBe(departmentTotalBefore);

    await openStoreLeafPath(page, "Beverages", "Tea", "Green Tea");
    await expandYears(page);
    await expectReady(page);
    await expect(await gridCellByPinnedText(page, "Green Tea", `202603:${scenario.measureId}`))
      .toContainText(formatGridValue(scenario.monthSecond, scenario.decimals, scenario.displayAsPercent));
  });
}

test("adds Department and Class rows and shows them in hierarchy maintenance", async ({ page }) => {
  await selectWorkspace(page, "hierarchy");
  await expect(page.getByText("Department / Class / Subclass Mapping")).toBeVisible();
  page.once("dialog", (dialog) => dialog.accept("Frozen E2E"));
  await page.getByRole("button", { name: "Add Department" }).click();
  await expectReady(page);

  await page.getByRole("button", { name: "Frozen E2E" }).click();
  page.once("dialog", (dialog) => dialog.accept("Ice Cream E2E"));
  await page.getByRole("button", { name: "Add Class" }).click();
  await expectReady(page);

  await expect(page.getByRole("button", { name: /Frozen E2E/ })).toBeVisible();
  await expect(page.getByText("Ice Cream E2E")).toBeVisible();
});

test("adds a store and reveals it immediately without a page reload", async ({ page }) => {
  const detachPromptHandler = await acceptPrompts(page, [
    "Store Z E2E",
    "Store A",
    "Baby Centre",
    "Central",
  ]);

  try {
    await page.getByRole("button", { name: "Add Store" }).click();
    await expectReady(page);
  } finally {
    detachPromptHandler();
  }

  await expect(page.locator(".view-menu-bar .year-picker").filter({ hasText: "Store Scope" }).locator("select")).toHaveValue("103");
  await expect(page.locator(".view-menu-bar .year-picker").filter({ hasText: "Store Scope" }).locator("option[value='103']")).toContainText("Store Z E2E");
});

test("imports a workbook in the new store-sheet format", async ({ page }) => {
  const workbookBuffer = createWorkbookBuffer([
    {
      Store: "Store B",
      Department: "Frozen",
      Class: "Ice Cream",
      Subclass: "Vanilla",
      Year: 2026,
      Month: "Jan",
      "Sales Revenue": 100,
      "Sold Qty": 50,
      ASP: 2,
      "Unit Cost": 1.2,
      "Total Costs": 60,
      GP: 40,
      "GP%": 40,
    },
  ]);

  await page.locator('input[type="file"]').setInputFiles({
    name: "import.xlsx",
    mimeType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    buffer: workbookBuffer,
  });
  await expectReady(page);
  await expect(page.locator(".status-card")).not.toContainText("Failed to fetch");
  await selectStoreScope(page, "102");
  await expandRowById(page, storeRootRowId);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store B" }).first()).toBeVisible();
  await expect(page.locator(".status-card")).not.toContainText("API unavailable.");
});

test("deletes a year with confirmation", async ({ page }) => {
  await page.locator(".layout-switcher .year-picker select").selectOption("202700");
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Delete Year" }).click();
  await expectReady(page);
  await expect(page.locator('.year-picker option[value="202700"]')).toHaveCount(0);
});

test("does not commit growth factors on Tab and preserves row expansion state", async ({ page }) => {
  await page.getByRole("button", { name: "Growth Factors" }).click();

  const storeRow = page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store A" }).first();
  await expect(storeRow).toBeVisible();

  const valueCell = await gridCellByPinnedText(page, "Store A", "202600:1");
  const beforeValue = await valueCell.textContent();
  let growthInput = valueCell.locator("input").first();
  await expect(growthInput).toBeVisible();
  await growthInput.fill("1.1");
  await growthInput.press("Tab");
  await expect(valueCell).toContainText(beforeValue ?? "");
  await expect(page.locator(".ag-cell-inline-editing")).toHaveCount(0);
  await expect(page.locator(".growth-factor-input").first()).toBeVisible();
});

test("keeps the edited branch open after a visible store leaf edit", async ({ page }) => {
  await expandYears(page);
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store A" }).first()).toBeVisible();
  await expandRowByLabel(page, "Store A");
  await expectReady(page);
  await expandRowByLabel(page, "Beverages");
  await expectReady(page);
  await expandRowByLabel(page, "Soft Drinks");
  await expectReady(page);

  const subclassRow = page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Cola" }).first();
  await expect(subclassRow).toBeVisible();

  await editCellByPinnedText(page, "Cola", "202601:1", "12345");
  await expectReady(page);

  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Store A" }).first()).toBeVisible();
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Beverages" }).first()).toBeVisible();
  await expect(page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: "Soft Drinks" }).first()).toBeVisible();
  await expect(subclassRow).toBeVisible();
  await expect(await gridCellByPinnedText(page, "Cola", "202601:1")).toContainText("12,345");
});

test("generates the following year from the active year", async ({ page }) => {
  await page.locator(".layout-switcher .year-picker select").selectOption("202700");
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Generate Next Year" }).click();
  await expectReady(page);
  await expect(page.locator('.year-picker option[value="202800"]')).toHaveCount(1);
});
