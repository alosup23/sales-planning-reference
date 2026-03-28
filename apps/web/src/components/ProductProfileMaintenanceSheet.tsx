import { useEffect, useMemo, useState } from "react";
import type {
  ProductHierarchyCatalog,
  ProductHierarchySubclass,
  ProductProfile,
  ProductProfileOption,
} from "../lib/types";

type ProductProfileMaintenanceSheetProps = {
  profiles: ProductProfile[];
  totalCount: number;
  pageNumber: number;
  pageSize: number;
  searchTerm: string;
  hierarchyRows: ProductHierarchyCatalog[];
  subclassRows: ProductHierarchySubclass[];
  options: ProductProfileOption[];
  onSearchChange: (value: string) => void;
  onPageChange: (pageNumber: number) => void;
  onSave: (profile: ProductProfile) => Promise<void>;
  onDelete: (profile: ProductProfile) => Promise<void>;
  onInactivate: (profile: ProductProfile) => Promise<void>;
  onImport: (file: File) => Promise<void>;
  onExport: () => Promise<void>;
  onUpsertOption: (fieldName: string, value: string, isActive: boolean) => Promise<void>;
  onDeleteOption: (fieldName: string, value: string) => Promise<void>;
  onSaveHierarchy: (row: ProductHierarchyCatalog) => Promise<void>;
  onDeleteHierarchy: (row: ProductHierarchyCatalog) => Promise<void>;
};

const optionFields = [
  { label: "Department", fieldName: "department" },
  { label: "Class", fieldName: "class" },
  { label: "Subclass", fieldName: "subclass" },
  { label: "Product Group", fieldName: "prodGroup" },
  { label: "Product Type", fieldName: "prodType" },
  { label: "Brand", fieldName: "brand" },
  { label: "Brand Type", fieldName: "brandType" },
  { label: "Gender", fieldName: "gender" },
  { label: "Size", fieldName: "size" },
  { label: "Collection", fieldName: "collection" },
  { label: "Promo", fieldName: "promo" },
  { label: "Ramadhan Promo", fieldName: "ramadhanPromo" },
  { label: "Active Flag", fieldName: "activeFlag" },
  { label: "Order Flag", fieldName: "orderFlag" },
  { label: "Launch Month", fieldName: "launchMonth" },
  { label: "Supplier", fieldName: "supplier" },
  { label: "Lifecycle Stage", fieldName: "lifecycleStage" },
  { label: "Age Stage", fieldName: "ageStage" },
  { label: "Gender Target", fieldName: "genderTarget" },
  { label: "Material", fieldName: "material" },
  { label: "Pack Size", fieldName: "packSize" },
  { label: "Size Range", fieldName: "sizeRange" },
  { label: "Colour Family", fieldName: "colourFamily" },
  { label: "Price Ladder Group", fieldName: "priceLadderGroup" },
  { label: "Good Better Best Tier", fieldName: "goodBetterBestTier" },
  { label: "Season Code", fieldName: "seasonCode" },
  { label: "Event Code", fieldName: "eventCode" },
  { label: "Substitute Group", fieldName: "substituteGroup" },
  { label: "Companion Group", fieldName: "companionGroup" },
  { label: "Replenishment Type", fieldName: "replenishmentType" },
] as const;

function blankProduct(): ProductProfile {
  return {
    skuVariant: "",
    description: "",
    description2: "",
    price: 0,
    cost: 0,
    dptNo: "",
    clssNo: "",
    brandNo: "",
    department: "",
    class: "",
    brand: "",
    revDepartment: "",
    revClass: "",
    subclass: "",
    prodGroup: "",
    prodType: "",
    activeFlag: "1",
    orderFlag: "",
    brandType: "",
    launchMonth: "",
    gender: "",
    size: "",
    collection: "",
    promo: "",
    ramadhanPromo: "",
    isActive: true,
    supplier: "",
    lifecycleStage: "",
    ageStage: "",
    genderTarget: "",
    material: "",
    packSize: "",
    sizeRange: "",
    colourFamily: "",
    kviFlag: false,
    markdownEligible: false,
    markdownFloorPrice: null,
    minimumMarginPct: null,
    priceLadderGroup: "",
    goodBetterBestTier: "",
    seasonCode: "",
    eventCode: "",
    launchDate: "",
    endOfLifeDate: "",
    substituteGroup: "",
    companionGroup: "",
    replenishmentType: "",
    leadTimeDays: null,
    moq: null,
    casePack: null,
    startingInventory: null,
    projectedStockOnHand: null,
    sellThroughTargetPct: null,
    weeksOfCoverTarget: null,
  };
}

function blankHierarchy(): ProductHierarchyCatalog {
  return {
    dptNo: "",
    clssNo: "",
    department: "",
    class: "",
    prodGroup: "",
    isActive: true,
  };
}

export function ProductProfileMaintenanceSheet({
  profiles,
  totalCount,
  pageNumber,
  pageSize,
  searchTerm,
  hierarchyRows,
  subclassRows,
  options,
  onSearchChange,
  onPageChange,
  onSave,
  onDelete,
  onInactivate,
  onImport,
  onExport,
  onUpsertOption,
  onDeleteOption,
  onSaveHierarchy,
  onDeleteHierarchy,
}: ProductProfileMaintenanceSheetProps) {
  const [selectedSkuVariant, setSelectedSkuVariant] = useState<string>(profiles[0]?.skuVariant ?? "");
  const [draft, setDraft] = useState<ProductProfile>(profiles[0] ?? blankProduct());
  const [hierarchyDraft, setHierarchyDraft] = useState<ProductHierarchyCatalog>(blankHierarchy());
  const [optionFieldName, setOptionFieldName] = useState<string>(optionFields[0].fieldName);
  const [optionValue, setOptionValue] = useState("");

  useEffect(() => {
    const selected = profiles.find((profile) => profile.skuVariant === selectedSkuVariant) ?? null;
    setDraft(selected ?? blankProduct());
  }, [profiles, selectedSkuVariant]);

  const optionsByField = useMemo(() => {
    const grouped = new Map<string, ProductProfileOption[]>();
    for (const option of options) {
      const items = grouped.get(option.fieldName) ?? [];
      items.push(option);
      grouped.set(option.fieldName, items);
    }

    return grouped;
  }, [options]);

  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));

  const setField = <K extends keyof ProductProfile>(key: K, value: ProductProfile[K]) => {
    setDraft((current) => ({ ...current, [key]: value }));
  };

  return (
    <section className="store-profile-sheet">
      <div className="maintenance-toolbar">
        <div>
          <div className="eyebrow">Product profile maintenance</div>
          <h2>Product Profile</h2>
        </div>
        <div className="toolbar-actions toolbar-actions-wrap">
          <button type="button" className="secondary-button" onClick={() => setDraft(blankProduct())}>
            New SKU
          </button>
          <label className="secondary-button file-button">
            Import Product Profiles
            <input
              type="file"
              accept=".xlsx"
              hidden
              onChange={(event) => {
                const file = event.target.files?.[0];
                if (file) {
                  void onImport(file);
                }

                event.currentTarget.value = "";
              }}
            />
          </label>
          <button type="button" className="secondary-button" onClick={() => void onExport()}>
            Export Product Profiles
          </button>
        </div>
      </div>

      <div className="store-option-form">
        <label className="labeled-field">
          <span>Search</span>
          <input value={searchTerm} onChange={(event) => onSearchChange(event.target.value)} placeholder="Search SKU, Department, Class, Subclass..." />
        </label>
        <div className="toolbar-actions">
          <button type="button" className="secondary-button" disabled={pageNumber <= 1} onClick={() => onPageChange(pageNumber - 1)}>
            Prev
          </button>
          <span className="sheet-stat">Page {pageNumber} / {totalPages}</span>
          <button type="button" className="secondary-button" disabled={pageNumber >= totalPages} onClick={() => onPageChange(pageNumber + 1)}>
            Next
          </button>
          <span className="sheet-stat">{totalCount.toLocaleString()} SKUs</span>
        </div>
      </div>

      <div className="store-profile-layout">
        <aside className="store-profile-list">
          <div className="eyebrow">SKU variants</div>
          <div className="store-profile-list-items">
            {profiles.map((profile) => (
              <button
                key={profile.skuVariant}
                type="button"
                className={`store-profile-list-item${selectedSkuVariant === profile.skuVariant ? " store-profile-list-item-active" : ""}`}
                onClick={() => setSelectedSkuVariant(profile.skuVariant)}
              >
                <strong>{profile.description}</strong>
                <span>{profile.skuVariant}</span>
                <span>{profile.department} / {profile.class} / {profile.subclass}</span>
              </button>
            ))}
          </div>
        </aside>

        <div className="store-profile-editor">
          <div className="store-profile-form-grid">
            <LabeledInput label="SKU Variant" value={draft.skuVariant} onChange={(value) => setField("skuVariant", value)} />
            <LabeledInput label="Description" value={draft.description} onChange={(value) => setField("description", value)} />
            <LabeledInput label="Description2" value={draft.description2 ?? ""} onChange={(value) => setField("description2", value)} />
            <LabeledInput label="Price" value={draft.price} type="number" step="0.01" onChange={(value) => setField("price", Number(value) || 0)} />
            <LabeledInput label="Cost" value={draft.cost} type="number" step="0.01" onChange={(value) => setField("cost", Number(value) || 0)} />
            <LabeledInput label="DptNo" value={draft.dptNo} onChange={(value) => setField("dptNo", value)} />
            <LabeledInput label="ClssNo" value={draft.clssNo} onChange={(value) => setField("clssNo", value)} />
            <LabeledInput label="BrandNo" value={draft.brandNo ?? ""} onChange={(value) => setField("brandNo", value)} />
            <LabeledSelect label="Department" optionFieldName="department" value={draft.department} options={optionsByField.get("department") ?? []} onChange={(value) => setField("department", value)} />
            <LabeledSelect label="Class" optionFieldName="class" value={draft.class} options={optionsByField.get("class") ?? []} onChange={(value) => setField("class", value)} />
            <LabeledSelect label="Subclass" optionFieldName="subclass" value={draft.subclass} options={optionsByField.get("subclass") ?? []} onChange={(value) => setField("subclass", value)} />
            <LabeledSelect label="Prod Group" optionFieldName="prodGroup" value={draft.prodGroup ?? ""} options={optionsByField.get("prodGroup") ?? []} onChange={(value) => setField("prodGroup", value)} />
            <LabeledInput label="Brand" value={draft.brand ?? ""} onChange={(value) => setField("brand", value)} />
            <LabeledInput label="Rev. Dept" value={draft.revDepartment ?? ""} onChange={(value) => setField("revDepartment", value)} />
            <LabeledInput label="Rev. Class" value={draft.revClass ?? ""} onChange={(value) => setField("revClass", value)} />
            <LabeledInput label="Prod Type" value={draft.prodType ?? ""} onChange={(value) => setField("prodType", value)} />
            <LabeledInput label="Order Flag" value={draft.orderFlag ?? ""} onChange={(value) => setField("orderFlag", value)} />
            <LabeledInput label="Active Flag" value={draft.activeFlag ?? ""} onChange={(value) => setField("activeFlag", value)} />
            <LabeledInput label="Brand Type" value={draft.brandType ?? ""} onChange={(value) => setField("brandType", value)} />
            <LabeledInput label="Launch Month" value={draft.launchMonth ?? ""} onChange={(value) => setField("launchMonth", value)} />
            <LabeledInput label="Gender" value={draft.gender ?? ""} onChange={(value) => setField("gender", value)} />
            <LabeledInput label="Size" value={draft.size ?? ""} onChange={(value) => setField("size", value)} />
            <LabeledInput label="Collection" value={draft.collection ?? ""} onChange={(value) => setField("collection", value)} />
            <LabeledInput label="Promo" value={draft.promo ?? ""} onChange={(value) => setField("promo", value)} />
            <LabeledInput label="Ramadhan Promo" value={draft.ramadhanPromo ?? ""} onChange={(value) => setField("ramadhanPromo", value)} />
            <LabeledSelect label="Supplier" optionFieldName="supplier" value={draft.supplier ?? ""} options={optionsByField.get("supplier") ?? []} onChange={(value) => setField("supplier", value)} />
            <LabeledSelect label="Lifecycle Stage" optionFieldName="lifecycleStage" value={draft.lifecycleStage ?? ""} options={optionsByField.get("lifecycleStage") ?? []} onChange={(value) => setField("lifecycleStage", value)} />
            <LabeledSelect label="Age Stage" optionFieldName="ageStage" value={draft.ageStage ?? ""} options={optionsByField.get("ageStage") ?? []} onChange={(value) => setField("ageStage", value)} />
            <LabeledSelect label="Gender Target" optionFieldName="genderTarget" value={draft.genderTarget ?? ""} options={optionsByField.get("genderTarget") ?? []} onChange={(value) => setField("genderTarget", value)} />
            <LabeledSelect label="Material" optionFieldName="material" value={draft.material ?? ""} options={optionsByField.get("material") ?? []} onChange={(value) => setField("material", value)} />
            <LabeledSelect label="Pack Size" optionFieldName="packSize" value={draft.packSize ?? ""} options={optionsByField.get("packSize") ?? []} onChange={(value) => setField("packSize", value)} />
            <LabeledSelect label="Size Range" optionFieldName="sizeRange" value={draft.sizeRange ?? ""} options={optionsByField.get("sizeRange") ?? []} onChange={(value) => setField("sizeRange", value)} />
            <LabeledSelect label="Colour Family" optionFieldName="colourFamily" value={draft.colourFamily ?? ""} options={optionsByField.get("colourFamily") ?? []} onChange={(value) => setField("colourFamily", value)} />
            <LabeledSelect label="Price Ladder Group" optionFieldName="priceLadderGroup" value={draft.priceLadderGroup ?? ""} options={optionsByField.get("priceLadderGroup") ?? []} onChange={(value) => setField("priceLadderGroup", value)} />
            <LabeledSelect label="Good Better Best Tier" optionFieldName="goodBetterBestTier" value={draft.goodBetterBestTier ?? ""} options={optionsByField.get("goodBetterBestTier") ?? []} onChange={(value) => setField("goodBetterBestTier", value)} />
            <LabeledSelect label="Season Code" optionFieldName="seasonCode" value={draft.seasonCode ?? ""} options={optionsByField.get("seasonCode") ?? []} onChange={(value) => setField("seasonCode", value)} />
            <LabeledSelect label="Event Code" optionFieldName="eventCode" value={draft.eventCode ?? ""} options={optionsByField.get("eventCode") ?? []} onChange={(value) => setField("eventCode", value)} />
            <LabeledInput label="Launch Date" value={draft.launchDate ?? ""} type="date" onChange={(value) => setField("launchDate", value)} />
            <LabeledInput label="End Of Life Date" value={draft.endOfLifeDate ?? ""} type="date" onChange={(value) => setField("endOfLifeDate", value)} />
            <LabeledSelect label="Substitute Group" optionFieldName="substituteGroup" value={draft.substituteGroup ?? ""} options={optionsByField.get("substituteGroup") ?? []} onChange={(value) => setField("substituteGroup", value)} />
            <LabeledSelect label="Companion Group" optionFieldName="companionGroup" value={draft.companionGroup ?? ""} options={optionsByField.get("companionGroup") ?? []} onChange={(value) => setField("companionGroup", value)} />
            <LabeledSelect label="Replenishment Type" optionFieldName="replenishmentType" value={draft.replenishmentType ?? ""} options={optionsByField.get("replenishmentType") ?? []} onChange={(value) => setField("replenishmentType", value)} />
            <LabeledInput label="Lead Time Days" value={draft.leadTimeDays ?? ""} type="number" step="1" onChange={(value) => setField("leadTimeDays", parseNullableInteger(value))} />
            <LabeledInput label="MOQ" value={draft.moq ?? ""} type="number" step="1" onChange={(value) => setField("moq", parseNullableInteger(value))} />
            <LabeledInput label="Case Pack" value={draft.casePack ?? ""} type="number" step="1" onChange={(value) => setField("casePack", parseNullableInteger(value))} />
            <LabeledInput label="Starting Inventory" value={draft.startingInventory ?? ""} type="number" step="0.01" onChange={(value) => setField("startingInventory", parseNullableNumber(value))} />
            <LabeledInput label="Projected Stock On Hand" value={draft.projectedStockOnHand ?? ""} type="number" step="0.01" onChange={(value) => setField("projectedStockOnHand", parseNullableNumber(value))} />
            <LabeledInput label="Sell Through Target %" value={draft.sellThroughTargetPct ?? ""} type="number" step="0.01" onChange={(value) => setField("sellThroughTargetPct", parseNullableNumber(value))} />
            <LabeledInput label="Weeks Of Cover Target" value={draft.weeksOfCoverTarget ?? ""} type="number" step="0.01" onChange={(value) => setField("weeksOfCoverTarget", parseNullableNumber(value))} />
            <LabeledInput label="Markdown Floor Price" value={draft.markdownFloorPrice ?? ""} type="number" step="0.01" onChange={(value) => setField("markdownFloorPrice", parseNullableNumber(value))} />
            <LabeledInput label="Minimum Margin %" value={draft.minimumMarginPct ?? ""} type="number" step="0.01" onChange={(value) => setField("minimumMarginPct", parseNullableNumber(value))} />
            <label className="labeled-field checkbox-field">
              <span>KVI Flag</span>
              <input type="checkbox" checked={draft.kviFlag ?? false} onChange={(event) => setField("kviFlag", event.target.checked)} />
            </label>
            <label className="labeled-field checkbox-field">
              <span>Markdown Eligible</span>
              <input type="checkbox" checked={draft.markdownEligible ?? false} onChange={(event) => setField("markdownEligible", event.target.checked)} />
            </label>
            <label className="labeled-field checkbox-field">
              <span>Active</span>
              <input type="checkbox" checked={draft.isActive} onChange={(event) => setField("isActive", event.target.checked)} />
            </label>
          </div>

          <div className="maintenance-toolbar store-profile-actions">
            <div className="toolbar-actions toolbar-actions-wrap">
              <button type="button" className="secondary-button secondary-button-active" onClick={() => void onSave(draft)}>
                Save SKU
              </button>
              {draft.skuVariant ? (
                <>
                  <button type="button" className="secondary-button" onClick={() => void onInactivate(draft)}>
                    Inactivate
                  </button>
                  <button type="button" className="secondary-button danger-button" onClick={() => void onDelete(draft)}>
                    Delete
                  </button>
                </>
              ) : null}
            </div>
          </div>
        </div>
      </div>

      <section className="store-option-sheet">
        <div className="maintenance-toolbar">
          <div>
            <div className="eyebrow">Department / class maintenance</div>
            <h3>Product Hierarchy</h3>
          </div>
        </div>
        <div className="store-option-form">
          <LabeledInput label="DptNo" value={hierarchyDraft.dptNo} onChange={(value) => setHierarchyDraft((current) => ({ ...current, dptNo: value }))} />
          <LabeledInput label="ClssNo" value={hierarchyDraft.clssNo} onChange={(value) => setHierarchyDraft((current) => ({ ...current, clssNo: value }))} />
          <LabeledInput label="Department" value={hierarchyDraft.department} onChange={(value) => setHierarchyDraft((current) => ({ ...current, department: value }))} />
          <LabeledInput label="Class" value={hierarchyDraft.class} onChange={(value) => setHierarchyDraft((current) => ({ ...current, class: value }))} />
          <LabeledInput label="Prod Group" value={hierarchyDraft.prodGroup} onChange={(value) => setHierarchyDraft((current) => ({ ...current, prodGroup: value }))} />
          <button type="button" className="secondary-button" onClick={() => void onSaveHierarchy(hierarchyDraft)}>
            Save Hierarchy Row
          </button>
        </div>
        <div className="store-option-groups">
          <div className="store-option-card">
            <strong>Department / Class</strong>
            <ul>
              {hierarchyRows.map((row) => (
                <li key={`${row.dptNo}:${row.clssNo}`}>
                  <button type="button" className="link-button" onClick={() => setHierarchyDraft(row)}>
                    {row.department} / {row.class}
                  </button>
                  <div className="option-actions">
                    <button type="button" className="link-button danger-link" onClick={() => void onDeleteHierarchy(row)}>
                      Delete
                    </button>
                  </div>
                </li>
              ))}
            </ul>
          </div>
          <div className="store-option-card">
            <strong>Derived Department / Class / Subclass</strong>
            <ul>
              {subclassRows.map((row) => (
                <li key={`${row.department}:${row.class}:${row.subclass}`}>
                  <span>{row.department} / {row.class} / {row.subclass}</span>
                </li>
              ))}
            </ul>
          </div>
        </div>
      </section>

      <section className="store-option-sheet">
        <div className="maintenance-toolbar">
          <div>
            <div className="eyebrow">Option maintenance</div>
            <h3>Product Field Values</h3>
          </div>
        </div>
        <div className="store-option-form">
          <label className="labeled-field">
            <span>Field</span>
            <select value={optionFieldName} onChange={(event) => setOptionFieldName(event.target.value as (typeof optionFields)[number]["fieldName"])}>
              {optionFields.map((field) => (
                <option key={field.fieldName} value={field.fieldName}>
                  {field.label}
                </option>
              ))}
            </select>
          </label>
          <LabeledInput label="Value" value={optionValue} onChange={setOptionValue} />
          <button
            type="button"
            className="secondary-button"
            onClick={() => {
              if (!optionValue.trim()) {
                return;
              }

              void onUpsertOption(optionFieldName, optionValue, true);
              setOptionValue("");
            }}
          >
            Add Option
          </button>
        </div>

        <div className="store-option-groups">
          {optionFields.map((field) => (
            <div key={field.fieldName} className="store-option-card">
              <strong>{field.label}</strong>
              <ul>
                {(optionsByField.get(field.fieldName) ?? []).map((option) => (
                  <li key={`${option.fieldName}:${option.value}`}>
                    <span>{option.value}</span>
                    <div className="option-actions">
                      <button type="button" className="link-button" onClick={() => void onUpsertOption(option.fieldName, option.value, !option.isActive)}>
                        {option.isActive ? "Inactivate" : "Activate"}
                      </button>
                      <button type="button" className="link-button danger-link" onClick={() => void onDeleteOption(option.fieldName, option.value)}>
                        Delete
                      </button>
                    </div>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </section>
    </section>
  );
}

function LabeledInput({
  label,
  value,
  onChange,
  type = "text",
  step,
}: {
  label: string;
  value: string | number;
  onChange: (value: string) => void;
  type?: string;
  step?: string;
}) {
  return (
    <label className="labeled-field">
      <span>{label}</span>
      <input type={type} step={step} value={value} onChange={(event) => onChange(event.target.value)} />
    </label>
  );
}

function LabeledSelect({
  label,
  optionFieldName,
  value,
  options,
  onChange,
}: {
  label: string;
  optionFieldName: string;
  value: string;
  options: ProductProfileOption[];
  onChange: (value: string) => void;
}) {
  return (
    <label className="labeled-field">
      <span>{label}</span>
      <select value={value ?? ""} onChange={(event) => onChange(event.target.value)} data-option-field={optionFieldName}>
        <option value="">Select</option>
        {options.map((option) => (
          <option key={`${option.fieldName}:${option.value}`} value={option.value}>
            {option.value}
          </option>
        ))}
      </select>
    </label>
  );
}

function parseNullableNumber(value: string): number | null {
  if (!value.trim()) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseNullableInteger(value: string): number | null {
  if (!value.trim()) {
    return null;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : null;
}
