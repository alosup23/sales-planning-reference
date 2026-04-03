import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { expect, test } from "@playwright/test";

const storeLabelsById: Record<number, string> = {
  101: "Store A",
  102: "Store B",
  103: "Store Z E2E",
};

const storeRowId = (storeId: number, productNodeId: number) =>
  `view:store:node:${storeId}:${productNodeId}:${encodeURIComponent(`Store Total>${storeLabelsById[storeId] ?? `Store ${storeId}`}`)}`;
const storeRootRowId = "view:store:root";
const departmentRootRowId = "view:department:root";

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
  await page.locator(".view-menu-bar select").first().selectOption(value);
}

async function selectStoreScope(page: import("@playwright/test").Page, storeId: string) {
  await page.locator(".view-menu-bar .year-picker").filter({ hasText: "Store Scope" }).locator("select").selectOption(storeId);
  await expectReady(page);
}

async function selectDepartmentLayout(page: import("@playwright/test").Page, value: "department-store-class" | "department-class-store") {
  await page.locator(".view-menu-bar .year-picker").filter({ hasText: "Layout" }).locator("select").selectOption(value);
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
  return page.locator(`.ag-center-cols-container [row-id="${rowId}"] [col-id="${colId}"]`).first();
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
  await page.getByRole("button", { name: "Save" }).click();
  await expectReady(page);
}

async function normalizedCellText(cell: import("@playwright/test").Locator) {
  return ((await cell.textContent()) ?? "").replace(/\s+/g, "");
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
  await expect(page.locator(".ag-header-group-text", { hasText: "Jan" }).first()).toBeVisible();
  await selectStoreScope(page, "102");
  await expectReady(page);

  await page.getByRole("button", { name: "Expand Years" }).click();
  await expectReady(page);
  await expect(page.locator(".ag-header-group-text", { hasText: "Jan" }).first()).toBeVisible();
});

test("does not pin year total measure columns and keeps caret expansion working in every view", async ({ page }) => {
  await expect(page.locator(`.ag-pinned-left-cols-container [col-id="202600:1"]`)).toHaveCount(0);

  await toggleRowCaret(page, storeRootRowId);
  await expectReady(page);
  await expectToggleLabel(page, storeRootRowId, "Collapse Store Total");
  await toggleRowCaret(page, storeRootRowId);
  await expectReady(page);
  await expectToggleLabel(page, storeRootRowId, "Expand Store Total");

  await selectWorkspace(page, "planning-department");
  await expectReady(page);
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
