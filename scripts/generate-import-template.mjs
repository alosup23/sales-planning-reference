import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const outputPath = process.argv[2];
const storeNames = process.argv.slice(3);

if (!outputPath) {
  throw new Error("Usage: node scripts/generate-import-template.mjs <output-path> <store-name> [store-name...]");
}

if (storeNames.length === 0) {
  throw new Error("At least one store name is required.");
}

const headers = ["Store", "Department", "Class", "Year", "Month", "Sales Revenue", "Sold Qty", "ASP", "Unit Cost", "Total Costs", "GP", "GP%"];
const tempRoot = mkdtempSync(path.join(tmpdir(), "sales-plan-template-"));
const workbookRoot = path.join(tempRoot, "xlsx");

const xmlEscape = (value) => value
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll("\"", "&quot;")
  .replaceAll("'", "&apos;");

const cellRef = (columnIndex, rowIndex) => `${String.fromCharCode(65 + columnIndex)}${rowIndex}`;
const inlineStringCellXml = (value, columnIndex, rowIndex) =>
  `<c r="${cellRef(columnIndex, rowIndex)}" t="inlineStr"><is><t>${xmlEscape(value)}</t></is></c>`;

mkdirSync(path.join(workbookRoot, "_rels"), { recursive: true });
mkdirSync(path.join(workbookRoot, "xl", "_rels"), { recursive: true });
mkdirSync(path.join(workbookRoot, "xl", "worksheets"), { recursive: true });

const contentTypeOverrides = storeNames
  .map((_, index) => `  <Override PartName="/xl/worksheets/sheet${index + 1}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`)
  .join("\n");

writeFileSync(
  path.join(workbookRoot, "[Content_Types].xml"),
  `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
${contentTypeOverrides}
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

const workbookSheetsXml = storeNames
  .map((storeName, index) => `    <sheet name="${xmlEscape(storeName)}" sheetId="${index + 1}" r:id="rId${index + 1}"/>`)
  .join("\n");

writeFileSync(
  path.join(workbookRoot, "xl", "workbook.xml"),
  `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
${workbookSheetsXml}
  </sheets>
</workbook>`,
);

const workbookRelationshipsXml = storeNames
  .map((_, index) => `  <Relationship Id="rId${index + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet${index + 1}.xml"/>`)
  .join("\n");

writeFileSync(
  path.join(workbookRoot, "xl", "_rels", "workbook.xml.rels"),
  `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
${workbookRelationshipsXml}
  <Relationship Id="rId${storeNames.length + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
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

storeNames.forEach((storeName, index) => {
  const headerRowXml = headers
    .map((header, columnIndex) => inlineStringCellXml(header, columnIndex, 1))
    .join("");

  const sampleRowXml = [
    inlineStringCellXml(storeName, 0, 2),
    inlineStringCellXml("", 1, 2),
    inlineStringCellXml("", 2, 2),
    inlineStringCellXml("2026", 3, 2),
    inlineStringCellXml("Jan", 4, 2),
    inlineStringCellXml("", 5, 2),
    inlineStringCellXml("", 6, 2),
    inlineStringCellXml("1.00", 7, 2),
    inlineStringCellXml("0.00", 8, 2),
    inlineStringCellXml("", 9, 2),
    inlineStringCellXml("", 10, 2),
    inlineStringCellXml("", 11, 2),
  ].join("");

  writeFileSync(
    path.join(workbookRoot, "xl", "worksheets", `sheet${index + 1}.xml`),
    `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1">${headerRowXml}</row>
    <row r="2">${sampleRowXml}</row>
  </sheetData>
</worksheet>`,
  );
});

const resolvedOutputPath = path.resolve(outputPath);
mkdirSync(path.dirname(resolvedOutputPath), { recursive: true });
execFileSync("zip", ["-X", "-q", "-r", resolvedOutputPath, "."], { cwd: workbookRoot });
rmSync(tempRoot, { recursive: true, force: true });
