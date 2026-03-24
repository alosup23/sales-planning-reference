import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { expect, test } from "@playwright/test";

const storeRowId = (storeId: number, productNodeId: number) => `store-view:${storeId}:${productNodeId}`;
const storeRootRowId = "synthetic:Store Total:-10";
const departmentRootRowId = "synthetic:Department Total:-1";

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

async function toggleRowCaret(page: import("@playwright/test").Page, rowId: string) {
  const row = page.locator(`.ag-pinned-left-cols-container [row-id="${rowId}"]`);
  const toggle = row.locator(".ag-group-contracted:not(.ag-hidden), .ag-group-expanded:not(.ag-hidden)").first();
  await expect(toggle).toBeVisible();
  await toggle.click();
}

async function ensurePinnedRowVisible(page: import("@playwright/test").Page, rowId: string, ancestorRowIds: string[] = []) {
  const row = page.locator(`.ag-pinned-left-cols-container [row-id="${rowId}"]`).first();
  if (await row.count()) {
    return;
  }

  for (const ancestorRowId of ancestorRowIds) {
    const ancestorRow = page.locator(`.ag-pinned-left-cols-container [row-id="${ancestorRowId}"]`).first();
    if (await ancestorRow.count()) {
      const contractedToggle = ancestorRow.locator(".ag-group-contracted:not(.ag-hidden)").first();
      if (await contractedToggle.count()) {
        await contractedToggle.click();
      }
    }

    if (await row.count()) {
      return;
    }
  }

  await expect(row).toBeVisible();
}

async function gridCell(page: import("@playwright/test").Page, rowId: string, colId: string) {
  await ensurePinnedRowVisible(page, rowId, [storeRootRowId, departmentRootRowId]);
  const pinnedRow = page.locator(`.ag-pinned-left-cols-container [row-id="${rowId}"]`).first();
  await expect(pinnedRow).toBeVisible();
  const rowIndex = await pinnedRow.getAttribute("row-index");
  expect(rowIndex).not.toBeNull();
  return page.locator(`.ag-center-cols-container [row-index="${rowIndex}"] [col-id="${colId}"]`).first();
}

async function gridCellText(page: import("@playwright/test").Page, rowId: string, colId: string) {
  return (await gridCell(page, rowId, colId)).textContent();
}

async function gridCellByPinnedText(page: import("@playwright/test").Page, rowText: string, colId: string) {
  const pinnedRow = page.locator(".ag-pinned-left-cols-container .ag-row").filter({ hasText: rowText }).first();
  await expect(pinnedRow).toBeVisible();
  const rowIndex = await pinnedRow.getAttribute("row-index");
  expect(rowIndex).not.toBeNull();
  return page.locator(`.ag-center-cols-container [row-index="${rowIndex}"] [col-id="${colId}"]`).first();
}

async function editCell(page: import("@playwright/test").Page, rowId: string, colId: string, nextValue: string) {
  const cell = await gridCell(page, rowId, colId);
  await cell.dblclick();

  const editor = page.locator(".ag-cell-inline-editing input").last();
  await expect(editor).toBeVisible();
  await editor.fill(nextValue);
  await editor.press("Enter");
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

  const headers = ["Store", "Department", "Class", "Year", "Month", "Sales Revenue", "Sold Qty", "ASP", "Unit Cost", "Total Costs", "GP", "GP%"];
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
  await request.post("http://127.0.0.1:5080/api/v1/test/reset");
  await openGrid(page);
});

test("loads the live grid with FY26 and FY27 columns", async ({ page }) => {
  await expect(page.locator(".ag-header-group-text", { hasText: "FY26" }).first()).toBeVisible();
  await expect(page.locator(".ag-header-group-text", { hasText: "FY27" }).first()).toBeVisible();
  await expect(await gridCell(page, storeRowId(101, 2000), "202600:1")).toContainText(/[0-9,]+/);
});

test("supports expand and collapse controls for rows and years", async ({ page }) => {
  const pinnedRow = page.locator(`.ag-pinned-left-cols-container [row-id="${storeRowId(101, 2110)}"]`);
  await page.locator(`.ag-pinned-left-cols-container [row-id="${storeRootRowId}"]`).click({ button: "right" });
  await page.getByRole("button", { name: "Collapse All" }).click();
  await expect(pinnedRow).toHaveCount(0);

  await page.locator(`.ag-pinned-left-cols-container [row-id="${storeRootRowId}"]`).click({ button: "right" });
  await page.getByRole("button", { name: "Expand All" }).click();
  await expect(pinnedRow).toHaveCount(1);

  await page.getByRole("button", { name: "Expand Years" }).click();
  await expect(page.locator(".ag-header-group-text", { hasText: "FY26" }).first()).toBeVisible();
});

test("does not pin year total measure columns and keeps caret expansion working in every view", async ({ page }) => {
  await expect(page.locator(`.ag-pinned-left-cols-container [col-id="202600:1"]`)).toHaveCount(0);

  await page.locator(`.ag-pinned-left-cols-container [row-id="${storeRootRowId}"]`).click({ button: "right" });
  await page.getByRole("button", { name: "Expand All" }).click();
  await toggleRowCaret(page, storeRowId(101, 2100));
  await expect(page.locator(`.ag-pinned-left-cols-container [row-id="${storeRowId(101, 2110)}"]`)).toHaveCount(0);
  await toggleRowCaret(page, storeRowId(101, 2100));
  await expect(page.locator(`.ag-pinned-left-cols-container [row-id="${storeRowId(101, 2110)}"]`)).toBeVisible();

  await page.getByRole("button", { name: "Planning - by Department" }).click();
  await expectReady(page);
  const departmentStoreRowId = "department-view:department-store-class:Beverages:Store A";
  const departmentClassRowId = "department-view:department-store-class:Beverages:Store A:Soft Drinks";
  await page.locator(`.ag-pinned-left-cols-container [row-id="${departmentRootRowId}"]`).click({ button: "right" });
  await page.getByRole("button", { name: "Expand All" }).click();
  await toggleRowCaret(page, departmentStoreRowId);
  await expect(page.locator(`.ag-pinned-left-cols-container [row-id="${departmentClassRowId}"]`)).toHaveCount(0);
  await toggleRowCaret(page, departmentStoreRowId);
  await expect(page.locator(`.ag-pinned-left-cols-container [row-id="${departmentClassRowId}"]`)).toBeVisible();
});

test("editing a visible Sales Revenue year total refreshes the row total", async ({ page }) => {
  await page.locator(`.ag-pinned-left-cols-container [row-id="${storeRootRowId}"]`).click({ button: "right" });
  await page.getByRole("button", { name: "Expand All" }).click();
  const revenueCol = "202600:1";
  const targetCell = await gridCell(page, storeRowId(101, 2110), revenueCol);
  const before = await targetCell.textContent();

  await editCell(page, storeRowId(101, 2110), revenueCol, "12000");
  await expectReady(page);
  await expect(await gridCell(page, storeRowId(101, 2110), revenueCol)).not.toContainText(before ?? "");
});

test("department view totals stay aligned with store view after a leaf edit", async ({ page }) => {
  await page.locator(`.ag-pinned-left-cols-container [row-id="${storeRootRowId}"]`).click({ button: "right" });
  await page.getByRole("button", { name: "Expand All" }).click();

  await editCell(page, storeRowId(101, 2110), "202600:1", "12000");
  await expectReady(page);

  const storeDepartmentTotal = await (await gridCellByPinnedText(page, "Beverages", "202600:1")).textContent();
  await page.getByRole("button", { name: "Planning - by Department" }).click();
  await expectReady(page);
  await page.locator(`.ag-pinned-left-cols-container [row-id="${departmentRootRowId}"]`).click({ button: "right" });
  await page.getByRole("button", { name: "Expand All" }).click();

  await expect(await gridCellByPinnedText(page, "Beverages", "202600:1")).toContainText(storeDepartmentTotal ?? "");
});

test("adds Department and Class rows and shows them in hierarchy maintenance", async ({ page }) => {
  await page.locator(`.ag-pinned-left-cols-container [row-id="${storeRootRowId}"]`).click({ button: "right" });
  await page.getByRole("button", { name: "Expand All" }).click();
  await (await gridCell(page, storeRowId(101, 2000), "202600:1")).click();
  page.once("dialog", (dialog) => dialog.accept("Frozen E2E"));
  await page.getByRole("button", { name: "Add Department" }).click();
  await expectReady(page);

  await page.getByText("Frozen E2E", { exact: true }).click();
  page.once("dialog", (dialog) => dialog.accept("Ice Cream E2E"));
  await page.getByRole("button", { name: "Add Class" }).click();
  await expectReady(page);

  await page.getByRole("button", { name: "Hierarchy Maintenance" }).click();
  await expect(page.getByText("Department / Class Mapping")).toBeVisible();
  await expect(page.getByRole("button", { name: /Frozen E2E/ })).toBeVisible();
  await expect(page.getByText("Ice Cream E2E")).toBeVisible();
});

test("imports a workbook in the new store-sheet format", async ({ page }) => {
  const workbookBuffer = createWorkbookBuffer([
    {
      Store: "Store B",
      Department: "Frozen",
      Class: "Ice Cream",
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
  await page.locator(`.ag-pinned-left-cols-container [row-id="${storeRowId(101, 2000)}"]`).click({ button: "right" });
  await page.getByRole("button", { name: "Expand All" }).click();
  await page.getByRole("button", { name: "Expand Years" }).click();

  const importedRow = page.getByText("Ice Cream", { exact: true }).last();
  await expect(importedRow).toBeVisible();
});

test("deletes a year with confirmation", async ({ page }) => {
  await page.locator(".year-picker select").selectOption("202700");
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Delete Year" }).click();
  await expectReady(page);
  await expect(page.locator('.year-picker option[value="202700"]')).toHaveCount(0);
});

test("commits growth factors only on Enter and preserves row expansion state", async ({ page }) => {
  await page.getByRole("button", { name: "Growth Factors" }).click();
  await page.locator(`.ag-pinned-left-cols-container [row-id="${storeRootRowId}"]`).click({ button: "right" });
  await page.getByRole("button", { name: "Expand All" }).click();
  await toggleRowCaret(page, storeRowId(101, 2100));

  const aggregateRowId = storeRowId(101, 2100);
  const classRow = page.locator(`.ag-pinned-left-cols-container [row-id="${storeRowId(101, 2110)}"]`);
  await expect(classRow).toBeVisible();

  const valueCell = await gridCell(page, aggregateRowId, "202600:1");
  const beforeValue = await valueCell.textContent();
  const growthInput = valueCell.locator("input").first();
  await expect(growthInput).toBeVisible();
  await growthInput.fill("1.1");
  await growthInput.press("Tab");
  await expect(valueCell).toContainText(beforeValue ?? "");
  await expect(classRow).toBeVisible();

  await growthInput.fill("1.1");
  await growthInput.press("Enter");
  await expectReady(page);
  await expect(valueCell).not.toContainText(beforeValue ?? "");
  await expect(classRow).toBeVisible();
  await expect(page.locator(".ag-cell-inline-editing")).toHaveCount(0);
  await expect((await gridCell(page, aggregateRowId, "202600:1")).locator("input").first()).toHaveValue("1.0");
});

test("generates the following year from the active year", async ({ page }) => {
  await page.locator(".year-picker select").selectOption("202700");
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Generate Next Year" }).click();
  await expectReady(page);
  await expect(page.locator('.year-picker option[value="202800"]')).toHaveCount(1);
});
