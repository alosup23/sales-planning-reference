import { useEffect, useState } from "react";
import type { SeasonalityEventProfile } from "../lib/types";

type SeasonalityEventMaintenanceSheetProps = {
  profiles: SeasonalityEventProfile[];
  totalCount: number;
  pageNumber: number;
  pageSize: number;
  searchTerm: string;
  onSearchChange: (value: string) => void;
  onPageChange: (pageNumber: number) => void;
  onSave: (profile: SeasonalityEventProfile) => Promise<void>;
  onDelete: (profile: SeasonalityEventProfile) => Promise<void>;
  onInactivate: (profile: SeasonalityEventProfile) => Promise<void>;
  onImport: (file: File) => Promise<void>;
  onExport: () => Promise<void>;
};

function blankSeasonalityProfile(): SeasonalityEventProfile {
  return {
    seasonalityEventProfileId: null,
    department: "",
    class: "",
    subclass: "",
    seasonCode: "",
    eventCode: "",
    month: 1,
    weight: 1,
    promoWindow: "",
    peakFlag: false,
    isActive: true,
  };
}

export function SeasonalityEventMaintenanceSheet({
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
}: SeasonalityEventMaintenanceSheetProps) {
  const [selectedId, setSelectedId] = useState<number | null>(profiles[0]?.seasonalityEventProfileId ?? null);
  const [draft, setDraft] = useState<SeasonalityEventProfile>(profiles[0] ?? blankSeasonalityProfile());

  useEffect(() => {
    const selected = profiles.find((profile) => profile.seasonalityEventProfileId === selectedId) ?? null;
    setDraft(selected ?? blankSeasonalityProfile());
  }, [profiles, selectedId]);

  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  const setField = <K extends keyof SeasonalityEventProfile>(key: K, value: SeasonalityEventProfile[K]) => {
    setDraft((current) => ({ ...current, [key]: value }));
  };

  return (
    <section className="store-profile-sheet">
      <div className="maintenance-toolbar">
        <div>
          <div className="eyebrow">Seasonality and events maintenance</div>
          <h2>Seasonality & Events</h2>
        </div>
        <div className="toolbar-actions toolbar-actions-wrap">
          <button type="button" className="secondary-button" onClick={() => setDraft(blankSeasonalityProfile())}>
            New Seasonality Row
          </button>
          <label className="secondary-button file-button">
            Import Seasonality & Events
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
            Export Seasonality & Events
          </button>
        </div>
      </div>

      <div className="store-option-form">
        <label className="labeled-field">
          <span>Search</span>
          <input value={searchTerm} onChange={(event) => onSearchChange(event.target.value)} placeholder="Search department, season, event..." />
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
          <div className="eyebrow">Seasonality rows</div>
          <div className="store-profile-list-items">
            {profiles.map((profile) => (
              <button
                key={profile.seasonalityEventProfileId ?? `${profile.department}:${profile.class}:${profile.subclass}:${profile.month}`}
                type="button"
                className={`store-profile-list-item${selectedId === profile.seasonalityEventProfileId ? " store-profile-list-item-active" : ""}`}
                onClick={() => setSelectedId(profile.seasonalityEventProfileId ?? null)}
              >
                <strong>{profile.department || "All Departments"}</strong>
                <span>{profile.class || "All Classes"} / {profile.subclass || "All Subclasses"}</span>
                <span>{profile.seasonCode || "No Season"} / {profile.eventCode || "No Event"} / M{profile.month}</span>
              </button>
            ))}
          </div>
        </aside>

        <div className="store-profile-editor">
          <div className="store-profile-form-grid">
            <LabeledInput label="Department" value={draft.department ?? ""} onChange={(value) => setField("department", value)} />
            <LabeledInput label="Class" value={draft.class ?? ""} onChange={(value) => setField("class", value)} />
            <LabeledInput label="Subclass" value={draft.subclass ?? ""} onChange={(value) => setField("subclass", value)} />
            <LabeledInput label="Season Code" value={draft.seasonCode ?? ""} onChange={(value) => setField("seasonCode", value)} />
            <LabeledInput label="Event Code" value={draft.eventCode ?? ""} onChange={(value) => setField("eventCode", value)} />
            <LabeledInput label="Month" value={draft.month} type="number" step="1" onChange={(value) => setField("month", Math.max(1, Math.min(12, Number(value) || 1)))} />
            <LabeledInput label="Weight" value={draft.weight} type="number" step="0.01" onChange={(value) => setField("weight", Number(value) || 0)} />
            <LabeledInput label="Promo Window" value={draft.promoWindow ?? ""} onChange={(value) => setField("promoWindow", value)} />
            <label className="labeled-field checkbox-field">
              <span>Peak Flag</span>
              <input type="checkbox" checked={draft.peakFlag} onChange={(event) => setField("peakFlag", event.target.checked)} />
            </label>
            <label className="labeled-field checkbox-field">
              <span>Active</span>
              <input type="checkbox" checked={draft.isActive} onChange={(event) => setField("isActive", event.target.checked)} />
            </label>
          </div>

          <div className="maintenance-toolbar store-profile-actions">
            <div className="toolbar-actions toolbar-actions-wrap">
              <button type="button" className="secondary-button secondary-button-active" onClick={() => void onSave(draft)}>
                Save Seasonality Row
              </button>
              {draft.seasonalityEventProfileId ? (
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
