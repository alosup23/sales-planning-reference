import { useEffect, useState } from "react";
import type { VendorSupplyProfile } from "../lib/types";

type VendorSupplyProfileMaintenanceSheetProps = {
  profiles: VendorSupplyProfile[];
  totalCount: number;
  pageNumber: number;
  pageSize: number;
  searchTerm: string;
  onSearchChange: (value: string) => void;
  onPageChange: (pageNumber: number) => void;
  onSave: (profile: VendorSupplyProfile) => Promise<void>;
  onDelete: (profile: VendorSupplyProfile) => Promise<void>;
  onInactivate: (profile: VendorSupplyProfile) => Promise<void>;
  onImport: (file: File) => Promise<void>;
  onExport: () => Promise<void>;
};

function blankVendorSupplyProfile(): VendorSupplyProfile {
  return {
    vendorSupplyProfileId: null,
    supplier: "",
    brand: "",
    leadTimeDays: null,
    moq: null,
    casePack: null,
    replenishmentType: "",
    paymentTerms: "",
    isActive: true,
  };
}

export function VendorSupplyProfileMaintenanceSheet({
  profiles,
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
}: VendorSupplyProfileMaintenanceSheetProps) {
  const [selectedId, setSelectedId] = useState<number | null>(profiles[0]?.vendorSupplyProfileId ?? null);
  const [draft, setDraft] = useState<VendorSupplyProfile>(profiles[0] ?? blankVendorSupplyProfile());

  useEffect(() => {
    const selected = profiles.find((profile) => profile.vendorSupplyProfileId === selectedId) ?? null;
    setDraft(selected ?? blankVendorSupplyProfile());
  }, [profiles, selectedId]);

  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  const setField = <K extends keyof VendorSupplyProfile>(key: K, value: VendorSupplyProfile[K]) => {
    setDraft((current) => ({ ...current, [key]: value }));
  };

  return (
    <section className="store-profile-sheet">
      <div className="maintenance-toolbar">
        <div>
          <div className="eyebrow">Vendor supply maintenance</div>
          <h2>Vendor Supply Profile</h2>
        </div>
        <div className="toolbar-actions toolbar-actions-wrap">
          <button type="button" className="secondary-button" onClick={() => setDraft(blankVendorSupplyProfile())}>
            New Vendor Row
          </button>
          <label className="secondary-button file-button">
            Import Vendor Supply Profile
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
            Export Vendor Supply Profile
          </button>
        </div>
      </div>

      <div className="store-option-form">
        <label className="labeled-field">
          <span>Search</span>
          <input value={searchTerm} onChange={(event) => onSearchChange(event.target.value)} placeholder="Search supplier, brand, replenishment..." />
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
          <div className="eyebrow">Vendor rows</div>
          <div className="store-profile-list-items">
            {profiles.map((profile) => (
              <button
                key={profile.vendorSupplyProfileId ?? `${profile.supplier}:${profile.brand ?? ""}`}
                type="button"
                className={`store-profile-list-item${selectedId === profile.vendorSupplyProfileId ? " store-profile-list-item-active" : ""}`}
                onClick={() => setSelectedId(profile.vendorSupplyProfileId ?? null)}
              >
                <strong>{profile.supplier}</strong>
                <span>{profile.brand || "All Brands"}</span>
              </button>
            ))}
          </div>
        </aside>

        <div className="store-profile-editor">
          <div className="store-profile-form-grid">
            <LabeledInput label="Supplier" value={draft.supplier} onChange={(value) => setField("supplier", value)} />
            <LabeledInput label="Brand" value={draft.brand ?? ""} onChange={(value) => setField("brand", value)} />
            <LabeledInput label="Lead Time Days" value={draft.leadTimeDays ?? ""} type="number" step="1" onChange={(value) => setField("leadTimeDays", parseNullableInteger(value))} />
            <LabeledInput label="MOQ" value={draft.moq ?? ""} type="number" step="1" onChange={(value) => setField("moq", parseNullableInteger(value))} />
            <LabeledInput label="Case Pack" value={draft.casePack ?? ""} type="number" step="1" onChange={(value) => setField("casePack", parseNullableInteger(value))} />
            <LabeledInput label="Replenishment Type" value={draft.replenishmentType ?? ""} onChange={(value) => setField("replenishmentType", value)} />
            <LabeledInput label="Payment Terms" value={draft.paymentTerms ?? ""} onChange={(value) => setField("paymentTerms", value)} />
            <label className="labeled-field checkbox-field">
              <span>Active</span>
              <input type="checkbox" checked={draft.isActive} onChange={(event) => setField("isActive", event.target.checked)} />
            </label>
          </div>

          <div className="maintenance-toolbar store-profile-actions">
            <div className="toolbar-actions toolbar-actions-wrap">
              <button type="button" className="secondary-button secondary-button-active" onClick={() => void onSave(draft)}>
                Save Vendor Row
              </button>
              {draft.vendorSupplyProfileId ? (
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

function parseNullableInteger(value: string): number | null {
  if (!value.trim()) {
    return null;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : null;
}
