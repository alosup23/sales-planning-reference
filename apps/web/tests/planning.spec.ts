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

test.describe.configure({ mode: "serial" });

test.beforeEach(async ({ page }) => {
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
