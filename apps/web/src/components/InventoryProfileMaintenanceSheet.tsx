import { useEffect, useState } from "react";
import type { InventoryProfile } from "../lib/types";

type InventoryProfileMaintenanceSheetProps = {
  profiles: InventoryProfile[];
  totalCount: number;
  pageNumber: number;
  pageSize: number;
  searchTerm: string;
  onSearchChange: (value: string) => void;
  onPageChange: (pageNumber: number) => void;
  onSave: (profile: InventoryProfile) => Promise<void>;
  onDelete: (profile: InventoryProfile) => Promise<void>;
  onInactivate: (profile: InventoryProfile) => Promise<void>;
  onImport: (file: File) => Promise<void>;
  onExport: () => Promise<void>;
};

function blankInventoryProfile(): InventoryProfile {
  return {
    inventoryProfileId: null,
    storeCode: "",
    productCode: "",
    startingInventory: 0,
    inboundQty: null,
    reservedQty: null,
    projectedStockOnHand: null,
    safetyStock: null,
    weeksOfCoverTarget: null,
    sellThroughTargetPct: null,
    isActive: true,
  };
}

export function InventoryProfileMaintenanceSheet({
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
}: InventoryProfileMaintenanceSheetProps) {
  const [selectedId, setSelectedId] = useState<number | null>(profiles[0]?.inventoryProfileId ?? null);
  const [draft, setDraft] = useState<InventoryProfile>(profiles[0] ?? blankInventoryProfile());

  useEffect(() => {
    const selected = profiles.find((profile) => profile.inventoryProfileId === selectedId) ?? null;
    setDraft(selected ?? blankInventoryProfile());
  }, [profiles, selectedId]);

  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));

  const setField = <K extends keyof InventoryProfile>(key: K, value: InventoryProfile[K]) => {
    setDraft((current) => ({ ...current, [key]: value }));
  };

  return (
    <section className="store-profile-sheet">
      <div className="maintenance-toolbar">
        <div>
          <div className="eyebrow">Inventory profile maintenance</div>
          <h2>Inventory Profile</h2>
        </div>
        <div className="toolbar-actions toolbar-actions-wrap">
          <button type="button" className="secondary-button" onClick={() => setDraft(blankInventoryProfile())}>
            New Inventory Row
          </button>
          <label className="secondary-button file-button">
            Import Inventory Profile
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
            Export Inventory Profile
          </button>
        </div>
      </div>

      <div className="store-option-form">
        <label className="labeled-field">
          <span>Search</span>
          <input value={searchTerm} onChange={(event) => onSearchChange(event.target.value)} placeholder="Search store or product code..." />
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
          <div className="eyebrow">Inventory rows</div>
          <div className="store-profile-list-items">
            {profiles.map((profile) => (
              <button
                key={`${profile.inventoryProfileId ?? `${profile.storeCode}:${profile.productCode}`}`}
                type="button"
                className={`store-profile-list-item${selectedId === profile.inventoryProfileId ? " store-profile-list-item-active" : ""}`}
                onClick={() => setSelectedId(profile.inventoryProfileId ?? null)}
              >
                <strong>{profile.storeCode}</strong>
                <span>{profile.productCode}</span>
              </button>
            ))}
          </div>
        </aside>

        <div className="store-profile-editor">
          <div className="store-profile-form-grid">
            <LabeledInput label="CompCode" value={draft.storeCode} onChange={(value) => setField("storeCode", value)} />
            <LabeledInput label="Product Code" value={draft.productCode} onChange={(value) => setField("productCode", value)} />
            <LabeledInput label="Starting Inventory" value={draft.startingInventory} type="number" step="0.01" onChange={(value) => setField("startingInventory", Number(value) || 0)} />
            <LabeledInput label="Inbound Qty" value={draft.inboundQty ?? ""} type="number" step="0.01" onChange={(value) => setField("inboundQty", parseNullableNumber(value))} />
            <LabeledInput label="Reserved Qty" value={draft.reservedQty ?? ""} type="number" step="0.01" onChange={(value) => setField("reservedQty", parseNullableNumber(value))} />
            <LabeledInput label="Projected Stock On Hand" value={draft.projectedStockOnHand ?? ""} type="number" step="0.01" onChange={(value) => setField("projectedStockOnHand", parseNullableNumber(value))} />
            <LabeledInput label="Safety Stock" value={draft.safetyStock ?? ""} type="number" step="0.01" onChange={(value) => setField("safetyStock", parseNullableNumber(value))} />
            <LabeledInput label="Weeks Of Cover Target" value={draft.weeksOfCoverTarget ?? ""} type="number" step="0.01" onChange={(value) => setField("weeksOfCoverTarget", parseNullableNumber(value))} />
            <LabeledInput label="Sell Through Target Pct" value={draft.sellThroughTargetPct ?? ""} type="number" step="0.01" onChange={(value) => setField("sellThroughTargetPct", parseNullableNumber(value))} />
            <label className="labeled-field checkbox-field">
              <span>Active</span>
              <input type="checkbox" checked={draft.isActive} onChange={(event) => setField("isActive", event.target.checked)} />
            </label>
          </div>

          <div className="maintenance-toolbar store-profile-actions">
            <div className="toolbar-actions toolbar-actions-wrap">
              <button type="button" className="secondary-button secondary-button-active" onClick={() => void onSave(draft)}>
                Save Inventory Row
              </button>
              {draft.inventoryProfileId ? (
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
