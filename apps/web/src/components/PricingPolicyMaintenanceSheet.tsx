import { useEffect, useState } from "react";
import type { PricingPolicy } from "../lib/types";

type PricingPolicyMaintenanceSheetProps = {
  policies: PricingPolicy[];
  totalCount: number;
  pageNumber: number;
  pageSize: number;
  searchTerm: string;
  onSearchChange: (value: string) => void;
  onPageChange: (pageNumber: number) => void;
  onSave: (policy: PricingPolicy) => Promise<void>;
  onDelete: (policy: PricingPolicy) => Promise<void>;
  onInactivate: (policy: PricingPolicy) => Promise<void>;
  onImport: (file: File) => Promise<void>;
  onExport: () => Promise<void>;
};

function blankPricingPolicy(): PricingPolicy {
  return {
    pricingPolicyId: null,
    department: "",
    class: "",
    subclass: "",
    brand: "",
    priceLadderGroup: "",
    minPrice: null,
    maxPrice: null,
    markdownFloorPrice: null,
    minimumMarginPct: null,
    kviFlag: false,
    markdownEligible: true,
    isActive: true,
  };
}

export function PricingPolicyMaintenanceSheet({
  policies,
  totalCount,
  pageNumber,
  pageSize,
  searchTerm,
  onSearchChange,
  onPageChange,
  onSave,
  onDelete,
  onInactivate,
  onImport,
  onExport,
}: PricingPolicyMaintenanceSheetProps) {
  const [selectedId, setSelectedId] = useState<number | null>(policies[0]?.pricingPolicyId ?? null);
  const [draft, setDraft] = useState<PricingPolicy>(policies[0] ?? blankPricingPolicy());

  useEffect(() => {
    const selected = policies.find((policy) => policy.pricingPolicyId === selectedId) ?? null;
    setDraft(selected ?? blankPricingPolicy());
  }, [policies, selectedId]);

  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  const setField = <K extends keyof PricingPolicy>(key: K, value: PricingPolicy[K]) => {
    setDraft((current) => ({ ...current, [key]: value }));
  };

  return (
    <section className="store-profile-sheet">
      <div className="maintenance-toolbar">
        <div>
          <div className="eyebrow">Pricing policy maintenance</div>
          <h2>Pricing Policy</h2>
        </div>
        <div className="toolbar-actions toolbar-actions-wrap">
          <button type="button" className="secondary-button" onClick={() => setDraft(blankPricingPolicy())}>
            New Policy
          </button>
          <label className="secondary-button file-button">
            Import Pricing Policy
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
            Export Pricing Policy
          </button>
        </div>
      </div>

      <div className="store-option-form">
        <label className="labeled-field">
          <span>Search</span>
          <input value={searchTerm} onChange={(event) => onSearchChange(event.target.value)} placeholder="Search department, class, brand..." />
        </label>
        <div className="toolbar-actions">
          <button type="button" className="secondary-button" disabled={pageNumber <= 1} onClick={() => onPageChange(pageNumber - 1)}>
            Prev
          </button>
          <span className="sheet-stat">Page {pageNumber} / {totalPages}</span>
          <button type="button" className="secondary-button" disabled={pageNumber >= totalPages} onClick={() => onPageChange(pageNumber + 1)}>
            Next
          </button>
          <span className="sheet-stat">{totalCount.toLocaleString()} rows</span>
        </div>
      </div>

      <div className="store-profile-layout">
        <aside className="store-profile-list">
          <div className="eyebrow">Pricing rows</div>
          <div className="store-profile-list-items">
            {policies.map((policy) => (
              <button
                key={policy.pricingPolicyId ?? `${policy.department}:${policy.class}:${policy.subclass}:${policy.brand}`}
                type="button"
                className={`store-profile-list-item${selectedId === policy.pricingPolicyId ? " store-profile-list-item-active" : ""}`}
                onClick={() => setSelectedId(policy.pricingPolicyId ?? null)}
              >
                <strong>{policy.department || "All Departments"}</strong>
                <span>{policy.class || "All Classes"} / {policy.subclass || "All Subclasses"}</span>
                <span>{policy.brand || "All Brands"}</span>
              </button>
            ))}
          </div>
        </aside>

        <div className="store-profile-editor">
          <div className="store-profile-form-grid">
            <LabeledInput label="Department" value={draft.department ?? ""} onChange={(value) => setField("department", value)} />
            <LabeledInput label="Class" value={draft.class ?? ""} onChange={(value) => setField("class", value)} />
            <LabeledInput label="Subclass" value={draft.subclass ?? ""} onChange={(value) => setField("subclass", value)} />
            <LabeledInput label="Brand" value={draft.brand ?? ""} onChange={(value) => setField("brand", value)} />
            <LabeledInput label="Price Ladder Group" value={draft.priceLadderGroup ?? ""} onChange={(value) => setField("priceLadderGroup", value)} />
            <LabeledInput label="Min Price" value={draft.minPrice ?? ""} type="number" step="0.01" onChange={(value) => setField("minPrice", parseNullableNumber(value))} />
            <LabeledInput label="Max Price" value={draft.maxPrice ?? ""} type="number" step="0.01" onChange={(value) => setField("maxPrice", parseNullableNumber(value))} />
            <LabeledInput label="Markdown Floor Price" value={draft.markdownFloorPrice ?? ""} type="number" step="0.01" onChange={(value) => setField("markdownFloorPrice", parseNullableNumber(value))} />
            <LabeledInput label="Minimum Margin Pct" value={draft.minimumMarginPct ?? ""} type="number" step="0.01" onChange={(value) => setField("minimumMarginPct", parseNullableNumber(value))} />
            <label className="labeled-field checkbox-field">
              <span>KVI Flag</span>
              <input type="checkbox" checked={draft.kviFlag} onChange={(event) => setField("kviFlag", event.target.checked)} />
            </label>
            <label className="labeled-field checkbox-field">
              <span>Markdown Eligible</span>
              <input type="checkbox" checked={draft.markdownEligible} onChange={(event) => setField("markdownEligible", event.target.checked)} />
            </label>
            <label className="labeled-field checkbox-field">
              <span>Active</span>
              <input type="checkbox" checked={draft.isActive} onChange={(event) => setField("isActive", event.target.checked)} />
            </label>
          </div>

          <div className="maintenance-toolbar store-profile-actions">
            <div className="toolbar-actions toolbar-actions-wrap">
              <button type="button" className="secondary-button secondary-button-active" onClick={() => void onSave(draft)}>
                Save Policy
              </button>
              {draft.pricingPolicyId ? (
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

function parseNullableNumber(value: string): number | null {
  if (!value.trim()) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}
