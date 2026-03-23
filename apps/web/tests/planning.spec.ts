import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { expect, test } from "@playwright/test";

const gridCell = (rowId: string, colId: string) => `[row-id="${rowId}"] [col-id="${colId}"]`;

async function openGrid(page: import("@playwright/test").Page) {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Sales Budget & Planning" })).toBeVisible();
  await expect(page.getByText("Lock-safe planning grid ready.")).toBeVisible();
  await expect(page.locator(gridCell("101-2110", "202602"))).toContainText("750");
}

async function editCell(page: import("@playwright/test").Page, rowId: string, colId: string, nextValue: string) {
  const cell = page.locator(gridCell(rowId, colId));
  await cell.dblclick();

  const editor = page.locator(".ag-cell-inline-editing input").last();
  await expect(editor).toBeVisible();
  await editor.fill(nextValue);
  await editor.press("Enter");
}

async function selectRow(page: import("@playwright/test").Page, rowId: string) {
  await page.locator(gridCell(rowId, "202600")).click();
}

function createWorkbookBuffer(rows: Array<Record<string, string | number>>): Buffer {
  const tempRoot = mkdtempSync(path.join(tmpdir(), "sales-plan-xlsx-"));
  const workbookRoot = path.join(tempRoot, "xlsx");
  const cleanup = () => rmSync(tempRoot, { recursive: true, force: true });

  const xmlEscape = (value: string) => value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&apos;");

  const headers = ["Store", "Category", "Subcategory", "Jan", "Feb"];
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
    <sheet name="Plan" sheetId="1" r:id="rId1"/>
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

async function getRowIdByLabel(page: import("@playwright/test").Page, label: string) {
  const rowLabel = page.getByText(label, { exact: true }).first();
  await expect(rowLabel).toBeVisible();
  const rowId = await rowLabel.evaluate((element) => element.closest("[row-id]")?.getAttribute("row-id"));
  expect(rowId).toBeTruthy();
  return rowId as string;
}

test.describe.configure({ mode: "serial" });

test.beforeEach(async ({ page, request }) => {
  await request.post("http://127.0.0.1:5080/api/v1/test/reset");
  await openGrid(page);
});

test("loads the live planning grid with locked-cell styling", async ({ page }) => {
  await expect(page.locator(gridCell("101-2000", "202600"))).toContainText("17,253");
  await expect(page.locator(gridCell("101-2100", "202600"))).toContainText("11,930");
  await expect(page.locator(gridCell("101-2110", "202602"))).toHaveClass(/cell-locked/);
});

test("edits an unlocked leaf cell and rolls the value into the yearly aggregate", async ({ page }) => {
  await editCell(page, "101-2120", "202603", "333");

  await expect(page.locator(gridCell("101-2120", "202603"))).toContainText("333");
  await expect(page.locator(gridCell("101-2120", "202600"))).toContainText("3,343");
  await expect(page.locator(gridCell("101-2100", "202600"))).toContainText("12,008");
});

test("locks and unlocks a cell through the custom context menu", async ({ page }) => {
  const targetCell = page.locator(gridCell("101-2120", "202604"));

  await targetCell.click({ button: "right" });
  await page.getByRole("button", { name: "Lock cell" }).click();
  await expect(targetCell).toHaveClass(/cell-locked/);

  await targetCell.click({ button: "right" });
  await page.getByRole("button", { name: "Unlock cell" }).click();
  await expect(targetCell).not.toHaveClass(/cell-locked/);
});

test("splashes a yearly total across months while preserving the locked February value", async ({ page }) => {
  await editCell(page, "101-2110", "202600", "12000");
  await expect(page.locator(gridCell("101-2110", "202600"))).toContainText("12,000");
  await page.locator(gridCell("101-2110", "202600")).click();
  await page.getByRole("button", { name: "Splash selected row" }).click();

  await expect(page.locator(gridCell("101-2110", "202601"))).toContainText("1,023");
  await expect(page.locator(gridCell("101-2110", "202602"))).toContainText("750");
  await expect(page.locator(gridCell("101-2110", "202603"))).toContainText("895");
  await expect(page.locator(gridCell("101-2110", "202600"))).toContainText("12,000");
});

test("adds a category and subcategory from the toolbar", async ({ page }) => {
  const categoryName = "Frozen E2E";
  const subcategoryName = "Ice Cream E2E";

  await selectRow(page, "101-2000");
  page.once("dialog", (dialog) => dialog.accept(categoryName));
  await page.getByRole("button", { name: "Add Category" }).click();

  const categoryRowId = await getRowIdByLabel(page, categoryName);
  await selectRow(page, categoryRowId);
  await expect(page.getByRole("button", { name: "Add Subcategory" })).toBeEnabled();

  page.once("dialog", (dialog) => dialog.accept(subcategoryName));
  await page.getByRole("button", { name: "Add Subcategory" }).click();

  const subcategoryRowId = await getRowIdByLabel(page, subcategoryName);
  await expect(page.locator(gridCell(subcategoryRowId, "202600"))).toContainText("0");
});

test("imports a workbook through the upload control and refreshes the grid", async ({ page }) => {
  const workbookBuffer = createWorkbookBuffer([
    {
      Store: "Store C",
      Category: "Frozen",
      Subcategory: "Ice Cream",
      Jan: 100,
      Feb: 110,
    },
  ]);

  await page.locator('input[type="file"]').setInputFiles({
    name: "import.xlsx",
    mimeType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    buffer: workbookBuffer,
  });

  const importedRowId = await getRowIdByLabel(page, "Ice Cream");
  await expect(page.locator(gridCell(importedRowId, "202601"))).toContainText("100");
  await expect(page.locator(gridCell(importedRowId, "202602"))).toContainText("110");
  await expect(page.locator(gridCell(importedRowId, "202600"))).toContainText("210");
});
