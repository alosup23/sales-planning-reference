import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const outputDir = path.resolve("docs/deliverables");

function xmlEscape(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function paragraphXml(block) {
  const styleMap = {
    title: "Title",
    heading1: "Heading1",
    heading2: "Heading2",
    heading3: "Heading3",
    normal: "Normal",
    bullet: "ListParagraph",
  };

  const styleId = styleMap[block.type] ?? "Normal";
  const text = block.type === "bullet" ? `- ${block.text}` : block.text;
  const preserve = text.startsWith(" ") || text.endsWith(" ") ? ' xml:space="preserve"' : "";
  return [
    "<w:p>",
    `<w:pPr><w:pStyle w:val="${styleId}"/></w:pPr>`,
    `<w:r><w:t${preserve}>${xmlEscape(text)}</w:t></w:r>`,
    "</w:p>",
  ].join("");
}

function createDocumentXml(doc) {
  const body = doc.blocks.map(paragraphXml).join("");
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:wpc="http://schemas.microsoft.com/office/word/2010/wordprocessingCanvas"
 xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"
 xmlns:o="urn:schemas-microsoft-com:office:office"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"
 xmlns:m="http://schemas.openxmlformats.org/officeDocument/2006/math"
 xmlns:v="urn:schemas-microsoft-com:vml"
 xmlns:wp14="http://schemas.microsoft.com/office/word/2010/wordprocessingDrawing"
 xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"
 xmlns:w10="urn:schemas-microsoft-com:office:word"
 xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
 xmlns:w14="http://schemas.microsoft.com/office/word/2010/wordml"
 xmlns:wpg="http://schemas.microsoft.com/office/word/2010/wordprocessingGroup"
 xmlns:wpi="http://schemas.microsoft.com/office/word/2010/wordprocessingInk"
 xmlns:wne="http://schemas.microsoft.com/office/word/2006/wordml"
 xmlns:wps="http://schemas.microsoft.com/office/word/2010/wordprocessingShape"
 mc:Ignorable="w14 wp14">
  <w:body>
    ${body}
    <w:sectPr>
      <w:pgSz w:w="12240" w:h="15840"/>
      <w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440" w:header="720" w:footer="720" w:gutter="0"/>
    </w:sectPr>
  </w:body>
</w:document>`;
}

function createStylesXml() {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:docDefaults>
    <w:rPrDefault>
      <w:rPr>
        <w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/>
        <w:sz w:val="22"/>
        <w:szCs w:val="22"/>
      </w:rPr>
    </w:rPrDefault>
  </w:docDefaults>
  <w:style w:type="paragraph" w:default="1" w:styleId="Normal">
    <w:name w:val="Normal"/>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Title">
    <w:name w:val="Title"/>
    <w:pPr><w:spacing w:after="240"/></w:pPr>
    <w:rPr><w:b/><w:sz w:val="32"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading1">
    <w:name w:val="heading 1"/>
    <w:basedOn w:val="Normal"/>
    <w:pPr><w:spacing w:before="240" w:after="120"/></w:pPr>
    <w:rPr><w:b/><w:sz w:val="28"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading2">
    <w:name w:val="heading 2"/>
    <w:basedOn w:val="Normal"/>
    <w:pPr><w:spacing w:before="180" w:after="80"/></w:pPr>
    <w:rPr><w:b/><w:sz w:val="24"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading3">
    <w:name w:val="heading 3"/>
    <w:basedOn w:val="Normal"/>
    <w:pPr><w:spacing w:before="120" w:after="60"/></w:pPr>
    <w:rPr><w:b/><w:sz w:val="22"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="ListParagraph">
    <w:name w:val="List Paragraph"/>
    <w:basedOn w:val="Normal"/>
    <w:pPr><w:ind w:left="720"/><w:spacing w:after="40"/></w:pPr>
  </w:style>
</w:styles>`;
}

function createContentTypesXml() {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>`;
}

function createRelsXml() {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>`;
}

function createDocumentRelsXml() {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>`;
}

function createCoreXml(title, createdIso) {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
 xmlns:dc="http://purl.org/dc/elements/1.1/"
 xmlns:dcterms="http://purl.org/dc/terms/"
 xmlns:dcmitype="http://purl.org/dc/dcmitype/"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:title>${xmlEscape(title)}</dc:title>
  <dc:creator>OpenAI Codex</dc:creator>
  <cp:lastModifiedBy>OpenAI Codex</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">${createdIso}</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">${createdIso}</dcterms:modified>
</cp:coreProperties>`;
}

function createAppXml() {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"
 xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>OpenAI Codex</Application>
</Properties>`;
}

function createDocx(documentSpec, outputPath) {
  const tempRoot = mkdtempSync(path.join(tmpdir(), "sales-planning-docx-"));
  const packageRoot = path.join(tempRoot, "docx");
  mkdirSync(path.join(packageRoot, "_rels"), { recursive: true });
  mkdirSync(path.join(packageRoot, "docProps"), { recursive: true });
  mkdirSync(path.join(packageRoot, "word", "_rels"), { recursive: true });

  const createdIso = new Date().toISOString();
  writeFileSync(path.join(packageRoot, "[Content_Types].xml"), createContentTypesXml());
  writeFileSync(path.join(packageRoot, "_rels", ".rels"), createRelsXml());
  writeFileSync(path.join(packageRoot, "docProps", "core.xml"), createCoreXml(documentSpec.title, createdIso));
  writeFileSync(path.join(packageRoot, "docProps", "app.xml"), createAppXml());
  writeFileSync(path.join(packageRoot, "word", "document.xml"), createDocumentXml(documentSpec));
  writeFileSync(path.join(packageRoot, "word", "styles.xml"), createStylesXml());
  writeFileSync(path.join(packageRoot, "word", "_rels", "document.xml.rels"), createDocumentRelsXml());

  execFileSync("zip", ["-X", "-q", "-r", outputPath, "."], { cwd: packageRoot });
  rmSync(tempRoot, { recursive: true, force: true });
}

function blocks(...items) {
  return items.flat();
}

function p(text) { return { type: "normal", text }; }
function b(text) { return { type: "bullet", text }; }
function h1(text) { return { type: "heading1", text }; }
function h2(text) { return { type: "heading2", text }; }
function h3(text) { return { type: "heading3", text }; }

const frdSummary = "This application is an enterprise sales planning platform with Excel-like interaction, multi-year planning, lock-aware top-down splashing, bottom-up roll-ups, synchronized store and department views, persistence, import/export, and deterministic measure calculations.";

const documents = [
  {
    fileName: "sales-planning-business-requirements-document.docx",
    title: "Sales Planning Business Requirements Document",
    blocks: blocks(
      { type: "title", text: "Sales Budget & Planning - Business Requirements Document" },
      p("Prepared for enterprise planning stakeholders. This document defines the business objectives, scope, rules, controls, and outcomes required for the Sales Budget & Planning platform."),
      h1("1. Executive Overview"),
      p(frdSummary),
      p("The system must provide an Excel-like planning experience while ensuring enterprise governance, calculation integrity, auditability, and data consistency across multiple synchronized business views."),
      h1("2. Business Objectives"),
      b("Enable planners to maintain annual and monthly sales plans at Store, Department, and Class levels."),
      b("Support both bottom-up planning and top-down planning with strict scope and lock controls."),
      b("Ensure all row and column totals remain mathematically correct for every supported measure."),
      b("Provide enterprise-grade import, export, persistence, and save behavior."),
      b("Provide synchronized views so the same underlying data can be analyzed from store-first and department-first perspectives."),
      h1("3. Business Scope"),
      h2("In Scope"),
      b("Store, Department, Class hierarchy maintenance."),
      b("Multiple fiscal years with monthly planning."),
      b("Measures: Sales Revenue, Sold Qty, ASP, Unit Cost, Total Costs, GP, GP%."),
      b("Leaf edits, aggregate edits, growth factor application, locking, year generation, import, export, save, and autosave."),
      h2("Out of Scope"),
      b("Workflow approvals beyond save and audit checkpoints."),
      b("External ERP integration and master data synchronization."),
      b("Advanced forecasting models beyond the defined planning formulas and splash methods."),
      h1("4. Stakeholders"),
      b("Planners: enter and adjust plan values."),
      b("Managers: lock values, review totals, approve planning assumptions externally if required."),
      b("Administrators: manage hierarchy structures and year structures."),
      b("Finance and FP&A users: validate totals, exports, and year-over-year plans."),
      h1("5. Business Capabilities"),
      b("Planning by Store."),
      b("Planning by Department."),
      b("Hierarchy maintenance for Department and Class mappings."),
      b("Multi-year column navigation with Year to Month expansion."),
      b("Expand and collapse of all row hierarchy levels."),
      b("Bottom-up roll-up and top-down splash behavior with deterministic recalculation."),
      b("Lock-aware planning controls."),
      b("Growth factor based uplifts or reductions."),
      b("Excel import and export in store-sheet format."),
      h1("6. Business Rules"),
      h2("Measures and formulas"),
      b("Sales Revenue = ASP * Sold Qty."),
      b("Total Costs = Unit Cost * Sold Qty."),
      b("GP = (ASP - Unit Cost) * Sold Qty."),
      b("GP% = (ASP - Unit Cost) / ASP * 100."),
      b("ASP defaults to 1.00 when no ASP exists at a leaf and one must be inferred."),
      h2("Planning behaviors"),
      b("Leaf edits change only the selected leaf intersection, recalculate dependent measures, and roll up through valid ancestors in the same year."),
      b("Aggregate edits do not directly overwrite descendants; they splash only within the selected branch and selected year."),
      b("No edit, splash, or recalculation may affect a different year."),
      b("Locked cells may not be changed by manual edit, splash, growth factor, import, or derived recalculation override."),
      h1("7. Calculation Integrity Controls"),
      b("Additive measures roll up by summation only: Sales Revenue, Sold Qty, Total Costs, GP."),
      b("Rate measures are always derived at aggregate level: ASP, Unit Cost, GP%."),
      b("Synthetic subtotal rows in alternate views must be built from bound data rows only and must not double-count synthetic rows."),
      b("Growth factor application must update the intended value once, then reset the growth factor control back to 1.0 without applying a second change."),
      h1("8. Save and Persistence"),
      b("Changes must persist to the backend database."),
      b("The user shall have a manual Save control."),
      b("Autosave shall run every five minutes when there are unsaved changes."),
      b("Application startup shall load data from the persisted database."),
      h1("9. Import and Export"),
      b("Import workbook shall use one worksheet per store."),
      b("Each sheet shall contain Store, Department, Class, Year, Month, Sales Revenue, Sold Qty, ASP, Unit Cost, Total Costs, GP, GP%."),
      b("Import shall insert or update records and validate formula consistency."),
      b("Invalid rows shall be written to an exception workbook in the same format."),
      b("Export shall generate the same workbook format used for import."),
      h1("10. Business Success Criteria"),
      b("Totals remain consistent across all views after leaf edits, aggregate edits, growth factor actions, import, and year generation."),
      b("Department totals in department view match corresponding department totals in store view."),
      b("Locks prevent unintended overwrite in all change paths."),
      b("Users can generate the next year using only editable inputs while calculated fields are recomputed."),
      b("The system can be used as the controlled planning source for enterprise budgeting activities.")
    ),
  },
  {
    fileName: "sales-planning-software-functional-specification.docx",
    title: "Sales Planning Software Functional Specification",
    blocks: blocks(
      { type: "title", text: "Sales Budget & Planning - Software Functional Specification" },
      p("This specification defines the detailed functional requirements, calculation rules, validations, and UI behaviors for the Sales Budget & Planning application."),
      h1("1. Functional Scope"),
      p("The application shall provide multi-year, multi-measure planning over Store, Department, and Class dimensions. The application shall support both row and column aggregation, synchronized multi-view presentation, growth factor inputs, lock-aware top-down splashing, bottom-up roll-ups, Excel import/export, and persisted storage."),
      h1("2. Data Dimensions"),
      b("Row hierarchy: Store, Department, Class."),
      b("Column hierarchy: Year, Month, Measure."),
      b("Measures: Sales Revenue, Sold Qty, ASP, Unit Cost, Total Costs, GP, GP%."),
      b("Scenario versioning is represented by the persisted scenario identifier."),
      h1("3. Measure Rules"),
      h2("3.1 Formulas"),
      b("Sales Revenue = ASP * Sold Qty."),
      b("Total Costs = Unit Cost * Sold Qty."),
      b("GP = (ASP - Unit Cost) * Sold Qty."),
      b("GP% = (ASP - Unit Cost) / ASP * 100."),
      h2("3.2 Measure editability"),
      b("Leaf-editable and aggregate-editable: Sales Revenue, Sold Qty, ASP, Unit Cost, GP%."),
      b("Calculated-only: Total Costs and GP."),
      h2("3.3 Aggregate derivation"),
      b("ASP at aggregate level = Sales Revenue / Sold Qty, default 1.00 if Sold Qty = 0."),
      b("Unit Cost at aggregate level = Total Costs / Sold Qty, default 0.00 if Sold Qty = 0."),
      b("GP% at aggregate level = (ASP - Unit Cost) / ASP * 100."),
      h1("4. Display and Rounding"),
      b("Sales Revenue: integer, rounded up."),
      b("Sold Qty: integer."),
      b("ASP: 2 decimals."),
      b("Unit Cost: 2 decimals."),
      b("Total Costs: integer, rounded up."),
      b("GP: integer, rounded up."),
      b("GP%: 1 decimal plus percent symbol."),
      h1("5. Leaf Edit Rules"),
      h2("5.1 Revenue edit"),
      b("Use current ASP or default ASP 1.00."),
      b("Derive Sold Qty from Revenue divided by ASP."),
      b("Normalize Sales Revenue to the nearest valid formula-consistent value."),
      b("Recalculate Total Costs, GP, and GP%."),
      h2("5.2 Sold Qty edit"),
      b("Recalculate Sales Revenue using ASP."),
      b("Recalculate Total Costs, GP, and GP%."),
      h2("5.3 ASP edit"),
      b("Recalculate Sales Revenue, GP, and GP%."),
      h2("5.4 Unit Cost edit"),
      b("Recalculate Total Costs, GP, and GP%."),
      h2("5.5 GP% edit"),
      b("Derive ASP from GP% and Unit Cost."),
      b("Recalculate Sales Revenue, Total Costs, and GP."),
      h1("6. Roll-Up Rules"),
      b("Leaf changes roll up only through valid ancestor rows and only within the same year."),
      b("Year totals roll up from months for the same row intersection."),
      b("Department totals roll up from bound child classes and must not double-count synthetic view rows."),
      b("Store totals roll up from child departments and classes."),
      h1("7. Aggregate Edit and Splash Rules"),
      b("Aggregate edits shall splash only into descendants within the selected branch and year."),
      b("Store aggregate edit affects only that store branch."),
      b("Department aggregate edit affects only that department branch."),
      b("Class aggregate edit affects only that class branch."),
      b("Year aggregate edit affects only months in that year."),
      b("No splash shall affect another year."),
      b("No splash shall affect sibling top-level branches."),
      h2("7.1 Allocation methods"),
      b("Equal distribution."),
      b("Existing plan distribution."),
      b("Seasonality profile."),
      b("Manual weights where provided."),
      h2("7.2 Lock-aware behavior"),
      b("Locked targets remain unchanged."),
      b("Remaining total is allocated across unlocked targets only."),
      b("If all targets are locked, splash fails."),
      h1("8. Growth Factor Rules"),
      b("Growth Factor shall be displayed only when the toolbar toggle is enabled."),
      b("Leaf Growth Factor applies to the selected leaf cell only, then rolls up."),
      b("Aggregate Growth Factor applies to the selected aggregate value, then splashes within scope."),
      b("Growth Factor applies only on Enter."),
      b("Blur does not commit the Growth Factor."),
      b("Escape cancels the draft value."),
      b("After successful apply, the Growth Factor input resets to 1.0 without applying another value change."),
      b("Growth Factor mode suppresses normal click-edit on value cells to avoid edit-mode conflicts."),
      h1("9. Lock Rules"),
      b("Locks apply to direct edits, splash actions, growth factor actions, import, and recalculation overwrite paths."),
      b("Ancestor locks shall block descendant changes that would violate the locked total."),
      h1("10. Hierarchy and View Rules"),
      b("Views supported: Planning - by Store, Planning - by Department, Hierarchy Maintenance."),
      b("Department view supports Department -> Store -> Class and Department -> Class -> Store layouts."),
      b("All views must reflect the same persisted data set."),
      b("Expand and collapse shall work at all visible hierarchy levels, including synthetic subtotal rows."),
      b("Expansion state shall remain stable across refresh and recalculation."),
      h1("11. Save and Autosave"),
      b("Manual Save writes a save checkpoint."),
      b("Autosave runs every five minutes when pending changes exist."),
      b("Startup loads persisted database state."),
      h1("12. Import and Export"),
      b("Import workbook requires one sheet per Store."),
      b("Month values must be three-letter month labels."),
      b("Import supports insert and update."),
      b("Invalid formula rows are written to an exception workbook."),
      b("Export uses the same workbook shape as import."),
      h1("13. Validation and Error Handling"),
      b("Calculated-only measures may not be directly edited."),
      b("Invalid inverse calculations shall be rejected."),
      b("Locked edits shall fail with clear messages."),
      b("Missing import columns shall fail validation."),
      b("Splash with zero eligible targets shall fail validation."),
      h1("14. Accuracy Controls"),
      b("All recalculation shall run deterministically after change application."),
      b("Additive measures shall never be derived by summing rate measures."),
      b("Derived rates shall be recalculated from additive bases."),
      b("Growth Factor reset shall not trigger a second recalculation."),
      b("Synchronized views shall be verified through automated tests.")
    ),
  },
  {
    fileName: "sales-planning-user-story-backlog.docx",
    title: "Sales Planning User Story Backlog",
    blocks: blocks(
      { type: "title", text: "Sales Budget & Planning - User Story Backlog and Acceptance Criteria" },
      p("This backlog organizes the functional requirements into formal user stories grouped by implementation epic."),
      h1("Epic 1 - Planning Grid and Navigation"),
      h2("Story 1.1 - Navigate the planning grid"),
      p("As a planner, I want to navigate the planning grid with keyboard and pointer controls so that I can edit values efficiently."),
      h3("Acceptance Criteria"),
      b("The user can click cells and move through the grid."),
      b("The grid shows multi-year headers and month detail under years."),
      b("The grid supports compact mode."),
      h2("Story 1.2 - Expand and collapse hierarchy levels"),
      p("As a planner, I want to expand and collapse Store, Department, and Class rows so that I can focus on the data I need."),
      h3("Acceptance Criteria"),
      b("Caret expand and collapse works at every visible hierarchy level in every view."),
      b("Right-click menu offers Expand, Collapse, Expand All, and Collapse All."),
      b("Rows do not auto-expand or auto-collapse after recalculation."),
      h1("Epic 2 - Bottom-Up Planning"),
      h2("Story 2.1 - Edit leaf Sales Revenue"),
      p("As a planner, I want to edit a Class-month Sales Revenue value so that the system recalculates the related leaf measures and rolls totals upward."),
      h3("Acceptance Criteria"),
      b("Editing Sales Revenue recalculates Sold Qty using ASP."),
      b("Sales Revenue is normalized to the nearest valid formula-consistent result."),
      b("Totals roll up only within the same year and valid ancestor branch."),
      h2("Story 2.2 - Edit leaf Sold Qty, ASP, Unit Cost, or GP%"),
      p("As a planner, I want leaf edits to any editable driver measure to recalculate dependent measures consistently."),
      h3("Acceptance Criteria"),
      b("Sold Qty edit recalculates Sales Revenue, Total Costs, GP, and GP%."),
      b("ASP edit recalculates Sales Revenue, GP, and GP%."),
      b("Unit Cost edit recalculates Total Costs, GP, and GP%."),
      b("GP% edit derives ASP and recalculates Sales Revenue, Total Costs, and GP."),
      h1("Epic 3 - Top-Down Planning and Splash"),
      h2("Story 3.1 - Edit aggregate Store totals"),
      p("As a planner, I want aggregate Store values to splash only to that Store branch so that other Stores are unaffected."),
      h3("Acceptance Criteria"),
      b("Store-level splash updates only Store descendants in the same year."),
      b("Other Stores remain unchanged."),
      h2("Story 3.2 - Edit aggregate Department or Class totals"),
      p("As a planner, I want aggregate Department and Class edits to affect only their selected branch and year."),
      h3("Acceptance Criteria"),
      b("Department splash affects only the selected Department subtree."),
      b("Class splash affects only the selected Class subtree."),
      b("No splash crosses to another year."),
      h1("Epic 4 - Locks and Data Protection"),
      h2("Story 4.1 - Lock a cell"),
      p("As a manager, I want to lock cells so that protected values cannot be overridden."),
      h3("Acceptance Criteria"),
      b("Locked cells reject manual edit, splash, import, and growth factor changes."),
      b("Ancestor locks protect descendant writes that would violate the locked ancestor."),
      h1("Epic 5 - Growth Factors"),
      h2("Story 5.1 - Apply Growth Factor at leaf"),
      p("As a planner, I want to apply a growth factor to a leaf value so that the value changes once and totals roll up."),
      h3("Acceptance Criteria"),
      b("Growth Factor applies only when Enter is pressed."),
      b("Blur does not apply the change."),
      b("Growth Factor resets to 1.0 after apply."),
      b("The applied value remains changed after the reset."),
      h2("Story 5.2 - Apply Growth Factor at aggregate"),
      p("As a planner, I want to apply a growth factor to an aggregate value so that the value splashes downward within scope."),
      h3("Acceptance Criteria"),
      b("Aggregate Growth Factor splashes only within the selected branch and year."),
      b("The affected totals roll up correctly afterward."),
      h1("Epic 6 - Multi-View Consistency"),
      h2("Story 6.1 - View the same data by Store and Department"),
      p("As a planner, I want to see the same data by Store and by Department so that I can review plans from different business perspectives."),
      h3("Acceptance Criteria"),
      b("Store-first and department-first views show the same underlying persisted data."),
      b("Department totals match between the two views."),
      h1("Epic 7 - Hierarchy Maintenance"),
      h2("Story 7.1 - Add Department and Class"),
      p("As an administrator, I want to maintain Department-Class mappings so that the hierarchy remains current."),
      h3("Acceptance Criteria"),
      b("Adding a Department updates hierarchy maintenance."),
      b("Adding a Class updates the Department-Class mapping."),
      b("Hierarchy Maintenance reflects imported valid mappings."),
      h2("Story 7.2 - Add a Store from an existing Store"),
      p("As an administrator, I want to create a new Store by copying hierarchy and data from an existing Store."),
      h3("Acceptance Criteria"),
      b("User is prompted for a source Store."),
      b("Store hierarchy and data are copied."),
      h1("Epic 8 - Year Management"),
      h2("Story 8.1 - Generate next year"),
      p("As a planner, I want to generate the next year from the active year so that I can start the next planning cycle quickly."),
      h3("Acceptance Criteria"),
      b("Only editable input measures are copied."),
      b("Calculated measures are recomputed in the new year."),
      b("Growth factors start at 1.0 in the generated year."),
      h2("Story 8.2 - Delete a year"),
      p("As an administrator, I want to delete a year and its related data when required."),
      h3("Acceptance Criteria"),
      b("Delete confirmation warns that deletes cannot be undone."),
      b("All related year data is removed."),
      h1("Epic 9 - Import and Export"),
      h2("Story 9.1 - Import workbook"),
      p("As a planner, I want to import workbook data by Store so that planning data can be loaded efficiently."),
      h3("Acceptance Criteria"),
      b("One sheet per Store is supported."),
      b("Rows are inserted or updated."),
      b("Invalid rows are written to an exception workbook."),
      h2("Story 9.2 - Export workbook"),
      p("As a planner, I want to export the current persisted data in the same format as the import workbook."),
      h3("Acceptance Criteria"),
      b("Export workbook structure matches the import structure."),
      b("All persisted store planning rows are exported."),
      h1("Epic 10 - Save and Persistence"),
      h2("Story 10.1 - Save planning changes"),
      p("As a planner, I want to save my work and have the system autosave pending changes."),
      h3("Acceptance Criteria"),
      b("Manual Save checkpoints current changes."),
      b("Autosave runs every five minutes when unsaved changes exist."),
      b("Application reload starts from persisted database state.")
    ),
  },
  {
    fileName: "sales-planning-solution-design-document.docx",
    title: "Sales Planning Solution Design Document",
    blocks: blocks(
      { type: "title", text: "Sales Budget & Planning - Solution Design Document" },
      p("This document describes the current implemented reference solution and the target production-ready architecture approach derived from it."),
      h1("1. Solution Summary"),
      p("The current solution is a web application implemented with React, TypeScript, AG Grid Enterprise, a .NET API, and SQLite persistence. It provides an Excel-like planning surface with synchronized store and department views, deterministic calculation rules, lock-aware top-down planning, bottom-up roll-ups, year generation, and Excel import/export."),
      h1("2. Current Application Components"),
      h2("Frontend"),
      b("React and TypeScript application."),
      b("AG Grid Enterprise for hierarchical spreadsheet-like rendering."),
      b("Planning sheets implemented in App.tsx and PlanningGrid.tsx."),
      h2("Backend"),
      b(".NET API exposing grid, edit, splash, lock, growth factor, import, export, save, and year-generation endpoints."),
      b("PlanningService as the application orchestration and calculation layer."),
      b("SQLite repository as the persisted storage implementation."),
      h2("Testing"),
      b("Backend unit tests validating calculation and scope rules."),
      b("Playwright end-to-end tests validating user interaction flows."),
      h1("3. Functional Design Principles"),
      b("Single source of truth in persisted backend storage."),
      b("Deterministic recalculation after every successful change."),
      b("Additive and derived measure separation."),
      b("Same-year-only change propagation."),
      b("Scope-limited aggregate splashing."),
      b("Stable row expansion state across refresh."),
      h1("4. Logical Data Model"),
      b("Store hierarchy with Store as top row dimension."),
      b("Department and Class product hierarchy beneath Store."),
      b("Time dimension with Year parent periods and Month leaf periods."),
      b("Planning cells keyed by scenario, measure, store, product node, and time period."),
      b("Cell attributes include effective value, input value, lock state, row version, and growth factor."),
      h1("5. Calculation Design"),
      h2("Leaf calculation pipeline"),
      b("Normalize edited leaf input."),
      b("Derive dependent editable values as needed."),
      b("Recalculate calculated measures."),
      b("Persist leaf state to working set."),
      h2("Aggregate calculation pipeline"),
      b("Determine eligible descendant leaf targets within scope and year."),
      b("Apply splash allocation to targets only."),
      b("Recompute additive totals."),
      b("Recompute derived rates from additive totals."),
      h2("Controls ensuring accuracy"),
      b("No direct editing of calculated-only measures."),
      b("Rate measures never summed at aggregate level."),
      b("Synthetic view rows built from bound data rows only."),
      b("Growth factor reset performed after apply without second recalculation."),
      h1("6. View Projection Design"),
      b("Store view projects base grid rows under a Store Total synthetic root."),
      b("Department view projects the same persisted rows under Department-oriented synthetic subtotal rows."),
      b("Department projection must filter to bound rows only when constructing synthetic subtotal values."),
      h1("7. Import and Export Design"),
      b("ClosedXML is used for workbook parsing and workbook generation."),
      b("Import validates headers and formulas per row."),
      b("Valid rows upsert leaf inputs and trigger recalculation."),
      b("Invalid rows are written to an exception workbook."),
      b("Export writes one sheet per Store in import-compatible format."),
      h1("8. UI Interaction Design"),
      b("Standard value editing is provided by AG Grid cell editing."),
      b("Growth Factor editing is a separate inline input mode gated by toolbar toggle."),
      b("When Growth Factor mode is enabled, value click-edit is suppressed to avoid edit-mode conflicts."),
      b("Context menu provides locking, splash, and hierarchy expand/collapse operations."),
      h1("9. Persistence and Save Design"),
      b("SQLite stores planning state for the current reference implementation."),
      b("Manual save creates an explicit checkpoint action."),
      b("Autosave creates periodic checkpoints when unsaved changes exist."),
      h1("10. Production Readiness Roadmap"),
      b("Replace SQLite with PostgreSQL or SQL Server."),
      b("Add SSO and role-based authorization."),
      b("Introduce background jobs for import and export."),
      b("Add structured logging, monitoring, and alerting."),
      b("Add migrations, backup strategy, and deployment automation."),
      h1("11. Validation Strategy"),
      b("Unit tests validate formulas, lock enforcement, scoped splash, year generation, and growth factor behavior."),
      b("End-to-end tests validate expansion state, view consistency, import/export flows, and UI interaction behavior."),
      h1("12. Deliverable Status"),
      p("The current repository is a validated reference implementation. It demonstrates the required functional behavior and the controls necessary to ensure calculation accuracy, while still requiring infrastructure hardening for full production deployment.")
    ),
  },
  {
    fileName: "sales-planning-enterprise-readiness-assessment-and-target-architecture-recommendation.docx",
    title: "Sales Planning Enterprise Readiness Assessment and Target Architecture Recommendation",
    blocks: blocks(
      { type: "title", text: "Sales Budget & Planning - Enterprise Readiness Assessment and Target Architecture Recommendation" },
      p("This document assesses the current solution against enterprise retail sales demand planning and forecasting requirements, identifies capability gaps, and recommends a cost-effective target architecture and deployment approach for Malaysia-based test, UAT, and production growth."),
      h1("1. Executive Summary"),
      p("The current application is a strong planning reference implementation, but it is not yet a full enterprise demand planning platform. To become a production-ready retail planning solution, it should evolve to a canonical leaf-grain data model, effective-dated master data, governed workflow, cloud-native persistence, forecast services, pricing optimization, and explainable AI insight generation."),
      b("Recommended cloud region: AWS Asia Pacific (Malaysia), ap-southeast-5."),
      b("Recommended production persistence: Amazon RDS for PostgreSQL plus Amazon S3."),
      b("Recommended low-cost test and UAT path: React static hosting plus serverless API and managed storage aligned to AWS free-eligible services where possible."),
      h1("2. Current State Assessment"),
      h2("Strengths"),
      b("Excel-like grid interaction with hierarchical rows and grouped time columns."),
      b("Deterministic bottom-up roll-up and lock-aware top-down splash behavior."),
      b("Synchronized store-first and department-first views over the same logical data set."),
      b("Persisted storage, import/export, year generation, growth factor support, and automated regression tests."),
      h2("Gaps to Enterprise Readiness"),
      b("Current persistence is SQLite-based and optimized for local demo use rather than enterprise concurrency, recovery, and scale."),
      b("The product hierarchy currently stops at Class and needs Subclass as the canonical planning leaf for retail demand planning."),
      b("Store clusters, regions, assortment lifecycle, delist behavior, and ramp profiles are not yet implemented as effective-dated master data."),
      b("Workflow, security, approval states, immutable audit depth, and operational observability require additional implementation."),
      b("Forecasting, seasonality intelligence, price-band optimization, and AI insight generation are not yet formalized as enterprise services."),
      h1("3. Canonical Target Operating Model"),
      p("The enterprise target model should treat the application as a governed planning platform with a single authoritative planning grain and multiple synchronized analytical projections."),
      h2("Authoritative planning grain"),
      b("Scenario Version x Store x Subclass x Month."),
      b("Editable input measures persisted at the canonical grain."),
      b("Calculated measures recomputed deterministically from editable inputs and additive totals."),
      h2("Supported synchronized views"),
      b("Store -> Department -> Class -> Subclass."),
      b("Department -> Store -> Class -> Subclass."),
      b("Department -> Class -> Store -> Subclass."),
      b("All views must remain projections over the same canonical persisted facts and must not create independent business totals."),
      h1("4. Enterprise Master Data Enhancements"),
      h2("Store and organizational structures"),
      b("Store master data must support Region and Cluster assignments."),
      b("Clusters shall support business-defined groupings such as Baby Mart, Baby Centre, and Baby Mall."),
      b("Store-to-Region and Store-to-Cluster assignments shall be effective-dated."),
      h2("Product hierarchy"),
      b("Department, Class, and Subclass shall be maintained as separate effective-dated dimensions."),
      b("Department-Class-Subclass mappings shall be versioned and time-enabled."),
      b("Add, delist, suspend, or relaunch actions shall preserve history and avoid destructive restatement of prior periods."),
      h2("Ramp profiles"),
      b("Ramp profiles shall support new store openings, new subclass launches, phased growth, temporary suspension, and delisting wind-down."),
      b("Ramp profiles shall be assignable by Store, Cluster, Region, Department, Class, or Subclass depending on business rules."),
      h1("5. Planning and Calculation Control Model"),
      h2("Planning behavior"),
      b("Leaf edits remain bottom-up only."),
      b("Aggregate edits remain top-down only."),
      b("Top-down splash must be strictly limited to the selected branch and the selected year."),
      b("Locks must be enforced for manual edits, splash actions, imports, growth factors, recalculation, and workflow-protected periods."),
      h2("Accuracy controls"),
      b("Additive measures shall roll up by summation only."),
      b("Rate and ratio measures shall be recomputed from additive bases and never summed directly."),
      b("Recalculation order shall be deterministic and idempotent."),
      b("Cross-view reconciliation checks shall ensure that Store, Department, and Class totals remain aligned after every committed action."),
      h1("6. Forecasting Capability Recommendations"),
      p("Retail demand planning requires more than manual planning and splash behavior. The platform should add governed forecast services with model selection, overrides, and reconciliation."),
      h2("Recommended forecast model family"),
      b("Seasonal Naive as mandatory benchmark."),
      b("ETS or Holt-Winters for stable seasonal series."),
      b("AutoARIMA for mature smoother demand patterns."),
      b("Croston-family models for intermittent subclass-store demand."),
      b("Gradient boosting or similar causal machine learning models with price, promo, holiday, cluster, and region drivers."),
      b("Hierarchical reconciliation so totals remain coherent across all supported views."),
      h2("Forecast governance"),
      b("Forecast model selection shall be traceable and explainable."),
      b("Planner overrides shall be captured separately from statistical baseline forecasts."),
      b("Closed months shall be actualized and protected from normal planning edits."),
      h1("7. Seasonality and Pricing Optimization Recommendations"),
      b("Seasonality should support subclass profiles with class, department, cluster, and region fallback logic."),
      b("The system should support event and promotional overlays, including movable holiday effects."),
      b("Pricing analysis should be maintained at subclass level with optional cluster and region pooling."),
      b("The solution should recommend price bands with expected quantity, revenue, GP, and GP% trade-offs."),
      b("The solution should support planner objectives for volume optimization, GP optimization, or balanced objective selection."),
      h1("8. AI Insight Layer Recommendations"),
      p("AI should operate as a governed advisory layer that reads trusted data and produces explainable recommendations rather than writing directly into planning cells."),
      h2("Recommended AI insight use cases"),
      b("Identify low-GP subclasses and stores with margin erosion."),
      b("Explain quantity or GP variance versus prior year, baseline forecast, or approved plan."),
      b("Suggest price-band opportunities for subclasses based on model outputs and historical performance."),
      b("Summarize forecast exceptions, lock conflicts, and material override activity for planners and reviewers."),
      h2("AI control principles"),
      b("AI recommendations must cite the relevant drivers and supporting metrics."),
      b("AI must not bypass standard edit, lock, splash, or approval controls."),
      b("AI actions must be user-reviewed before they become planning changes."),
      h1("9. Target Cloud Architecture Recommendation"),
      h2("Recommended production architecture"),
      b("React web application hosted as static assets with CDN delivery."),
      b(".NET API and calculation services running in managed containers."),
      b("Python forecast and AI workers running asynchronously."),
      b("Amazon RDS for PostgreSQL as the authoritative transactional planning store."),
      b("Amazon S3 for imports, exports, archived plan snapshots, exception files, model artifacts, and cold history."),
      b("Queue and event services for async import, export, forecast, reconciliation, and AI insight jobs."),
      h2("Recommended AWS services"),
      b("Frontend: Amazon S3 plus CloudFront."),
      b("API and calculation services: AWS App Runner or Amazon ECS with Fargate."),
      b("Transactional persistence: Amazon RDS for PostgreSQL."),
      b("Object storage: Amazon S3."),
      b("Asynchronous orchestration: Amazon SQS and EventBridge."),
      b("Secrets and configuration: AWS Secrets Manager and Systems Manager Parameter Store."),
      b("Identity integration: enterprise OIDC or SAML provider, optionally via Amazon Cognito where appropriate."),
      h1("10. Cost-Effective Test and UAT Recommendation"),
      p("For a low-cost test and UAT deployment, the target should minimize always-on infrastructure while still exercising real cloud persistence and integration patterns."),
      h2("Recommended demo and UAT path"),
      b("Frontend hosted as static assets."),
      b("Serverless or scale-to-zero API path where feasible."),
      b("Cloud persistence using a managed service with free-eligible or low-cost usage patterns."),
      b("S3-based import and export storage."),
      b("No always-on analytics cluster in test or UAT."),
      h2("Cloud persistence recommendation by stage"),
      b("Local development and CI demo: SQLite or local file-backed persistence remains acceptable."),
      b("Test and UAT: favor serverless persistence if the objective is lowest cost and free-eligible usage."),
      b("Production and growth stage: migrate authoritative persistence to PostgreSQL on managed database infrastructure."),
      h1("11. Scale and Capacity Assessment"),
      p("The business scale of 60 stores growing to 100 or more, with 10-15 departments, 15 classes per department, 20 subclasses per class, and 5 years of monthly data, is well within the capacity of a managed PostgreSQL transactional architecture when modeled at canonical leaf grain."),
      b("Use a wide-row planning fact shape for hot transactional data where measure set is known and fixed."),
      b("Archive historical exports, snapshots, and analytical extracts to object storage."),
      b("Add read replicas, caching, and partitioning only when profiling proves the need."),
      h1("12. Security, Governance, and Operational Enhancements"),
      b("Add SSO and role-based plus scope-based authorization by Region, Cluster, Store, Department, and Subclass."),
      b("Add immutable audit capture for every edit, splash, import, delete, year-generation action, lock, and AI recommendation acceptance."),
      b("Add scenario lifecycle states such as Draft, Working, Submitted, Approved, Published, and Archived."),
      b("Add structured logs, metrics, tracing, alerting, backup, retention, and disaster recovery procedures."),
      h1("13. Production Readiness Roadmap"),
      h2("Phase 1 - Enterprise Demo"),
      b("Introduce Subclass, Cluster, Region, effective-dated hierarchy extensions, and ramp-profile support."),
      b("Add cloud persistence abstraction and low-cost AWS test deployment."),
      b("Add forecast service interfaces and demo-visible forecast outputs."),
      h2("Phase 2 - Pilot"),
      b("Move authoritative persistence to PostgreSQL."),
      b("Add SSO, scoped authorization, workflow states, and background job processing."),
      b("Add import hardening, forecast monitoring, and operational observability."),
      h2("Phase 3 - Production"),
      b("Add high availability, backup and restore automation, read scaling, reconciliation monitoring, and formal support procedures."),
      h1("14. Recommendation Summary"),
      p("The current application should be evolved, not discarded. Its planning-rule core is valuable, but enterprise readiness depends on formalizing the canonical data model, introducing effective-dated master data and cloud persistence, and adding forecast, pricing, governance, and AI advisory capabilities on top of the existing deterministic planning engine.")
    ),
  },
];

mkdirSync(outputDir, { recursive: true });
for (const documentSpec of documents) {
  createDocx(documentSpec, path.join(outputDir, documentSpec.fileName));
}

const summaryPath = path.join(outputDir, "README.txt");
writeFileSync(
  summaryPath,
  [
    "Generated deliverables:",
    ...documents.map((documentSpec) => `- ${documentSpec.fileName}`),
    "",
    `Generated at: ${new Date().toISOString()}`,
  ].join("\n"),
);

console.log(`Generated ${documents.length} DOCX deliverables in ${outputDir}`);
