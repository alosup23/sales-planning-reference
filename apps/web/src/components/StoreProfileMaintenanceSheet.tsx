import { useEffect, useMemo, useState } from "react";
import type { StoreProfile, StoreProfileOption } from "../lib/types";

type StoreProfileMaintenanceSheetProps = {
  stores: StoreProfile[];
  options: StoreProfileOption[];
  onSave: (store: StoreProfile) => Promise<void>;
  onDelete: (store: StoreProfile) => Promise<void>;
  onInactivate: (store: StoreProfile) => Promise<void>;
  onImport: (file: File) => Promise<void>;
  onExport: () => Promise<void>;
  onUpsertOption: (fieldName: string, value: string, isActive: boolean) => Promise<void>;
  onDeleteOption: (fieldName: string, value: string) => Promise<void>;
};

const optionFields: Array<{ key: keyof StoreProfile; label: string; optionFieldName: string }> = [
  { key: "clusterLabel", label: "Branch Type", optionFieldName: "clusterLabel" },
  { key: "regionLabel", label: "Region", optionFieldName: "regionLabel" },
  { key: "state", label: "State", optionFieldName: "state" },
  { key: "sssg", label: "SSSG", optionFieldName: "sssg" },
  { key: "salesType", label: "Sales Type", optionFieldName: "salesType" },
  { key: "status", label: "Status", optionFieldName: "status" },
  { key: "buildingStatus", label: "Building Status", optionFieldName: "buildingStatus" },
  { key: "lifecycleState", label: "Lifecycle State", optionFieldName: "lifecycleState" },
  { key: "rampProfileCode", label: "Ramp Profile", optionFieldName: "rampProfileCode" },
  { key: "storeClusterRole", label: "Store Cluster Role", optionFieldName: "storeClusterRole" },
  { key: "storeFormatTier", label: "Store Format Tier", optionFieldName: "storeFormatTier" },
  { key: "catchmentType", label: "Catchment Type", optionFieldName: "catchmentType" },
  { key: "demographicSegment", label: "Demographic Segment", optionFieldName: "demographicSegment" },
  { key: "climateZone", label: "Climate Zone", optionFieldName: "climateZone" },
  { key: "storeOpeningSeason", label: "Store Opening Season", optionFieldName: "storeOpeningSeason" },
  { key: "storePriority", label: "Store Priority", optionFieldName: "storePriority" },
];

function blankStore(): StoreProfile {
  return {
    storeId: 0,
    storeCode: "",
    branchName: "",
    state: "",
    clusterLabel: "",
    latitude: null,
    longitude: null,
    regionLabel: "",
    openingDate: "",
    sssg: "",
    salesType: "",
    status: "Active",
    storey: "",
    buildingStatus: "",
    gta: null,
    nta: null,
    rsom: "",
    dm: "",
    rental: null,
    lifecycleState: "active",
    rampProfileCode: "",
    isActive: true,
    storeClusterRole: "",
    storeCapacitySqFt: null,
    storeFormatTier: "",
    catchmentType: "",
    demographicSegment: "",
    climateZone: "",
    fulfilmentEnabled: false,
    onlineFulfilmentNode: false,
    storeOpeningSeason: "",
    storeClosureDate: "",
    refurbishmentDate: "",
    storePriority: "",
  };
}

export function StoreProfileMaintenanceSheet({
  stores,
  options,
  onSave,
  onDelete,
  onInactivate,
  onImport,
  onExport,
  onUpsertOption,
  onDeleteOption,
}: StoreProfileMaintenanceSheetProps) {
  const [selectedStoreId, setSelectedStoreId] = useState<number>(stores[0]?.storeId ?? 0);
  const [draft, setDraft] = useState<StoreProfile>(stores[0] ?? blankStore());
  const [optionFieldName, setOptionFieldName] = useState(optionFields[0].optionFieldName);
  const [optionValue, setOptionValue] = useState("");

  useEffect(() => {
    const selected = stores.find((store) => store.storeId === selectedStoreId) ?? null;
    setDraft(selected ?? blankStore());
  }, [selectedStoreId, stores]);

  const optionsByField = useMemo(() => {
    const grouped = new Map<string, StoreProfileOption[]>();
    for (const option of options) {
      const fieldOptions = grouped.get(option.fieldName) ?? [];
      fieldOptions.push(option);
      grouped.set(option.fieldName, fieldOptions);
    }

    return grouped;
  }, [options]);

  const setField = <K extends keyof StoreProfile>(key: K, value: StoreProfile[K]) => {
    setDraft((current) => ({ ...current, [key]: value }));
  };

  return (
    <section className="store-profile-sheet">
      <div className="maintenance-toolbar">
        <div>
          <div className="eyebrow">Store profile maintenance</div>
          <h2>Store Profile</h2>
        </div>
        <div className="toolbar-actions toolbar-actions-wrap">
          <button type="button" className="secondary-button" onClick={() => setDraft(blankStore())}>
            New Store
          </button>
          <label className="secondary-button file-button">
            Import Store Profiles
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
            Export Store Profiles
          </button>
        </div>
      </div>

      <div className="store-profile-layout">
        <aside className="store-profile-list">
          <div className="eyebrow">Stores</div>
          <div className="store-profile-list-items">
            {stores.map((store) => (
              <button
                key={store.storeId}
                type="button"
                className={`store-profile-list-item${selectedStoreId === store.storeId ? " store-profile-list-item-active" : ""}`}
                onClick={() => setSelectedStoreId(store.storeId)}
              >
                <strong>{store.branchName}</strong>
                <span>{store.storeCode}</span>
              </button>
            ))}
          </div>
        </aside>

        <div className="store-profile-editor">
          <div className="store-profile-form-grid">
            <LabeledInput label="CompCode" value={draft.storeCode} onChange={(value) => setField("storeCode", value)} />
            <LabeledInput label="BranchName" value={draft.branchName} onChange={(value) => setField("branchName", value)} />
            <LabeledSelect label="Branch Type" optionFieldName="clusterLabel" value={draft.clusterLabel} options={optionsByField.get("clusterLabel") ?? []} onChange={(value) => setField("clusterLabel", value)} />
            <LabeledSelect label="Region" optionFieldName="regionLabel" value={draft.regionLabel} options={optionsByField.get("regionLabel") ?? []} onChange={(value) => setField("regionLabel", value)} />
            <LabeledSelect label="State" optionFieldName="state" value={draft.state ?? ""} options={optionsByField.get("state") ?? []} onChange={(value) => setField("state", value)} />
            <LabeledInput label="Latitude" value={draft.latitude ?? ""} type="number" step="0.000001" onChange={(value) => setField("latitude", parseNullableNumber(value))} />
            <LabeledInput label="Longitude" value={draft.longitude ?? ""} type="number" step="0.000001" onChange={(value) => setField("longitude", parseNullableNumber(value))} />
            <LabeledInput label="Opening Date" value={draft.openingDate ?? ""} type="date" onChange={(value) => setField("openingDate", value)} />
            <LabeledSelect label="SSSG" optionFieldName="sssg" value={draft.sssg ?? ""} options={optionsByField.get("sssg") ?? []} onChange={(value) => setField("sssg", value)} />
            <LabeledSelect label="Sales Type" optionFieldName="salesType" value={draft.salesType ?? ""} options={optionsByField.get("salesType") ?? []} onChange={(value) => setField("salesType", value)} />
            <LabeledSelect label="Status" optionFieldName="status" value={draft.status ?? ""} options={optionsByField.get("status") ?? []} onChange={(value) => setField("status", value)} />
            <LabeledInput label="Storey" value={draft.storey ?? ""} onChange={(value) => setField("storey", value)} />
            <LabeledSelect label="Building Status" optionFieldName="buildingStatus" value={draft.buildingStatus ?? ""} options={optionsByField.get("buildingStatus") ?? []} onChange={(value) => setField("buildingStatus", value)} />
            <LabeledInput label="GTA" value={draft.gta ?? ""} type="number" step="0.01" onChange={(value) => setField("gta", parseNullableNumber(value))} />
            <LabeledInput label="NTA" value={draft.nta ?? ""} type="number" step="0.01" onChange={(value) => setField("nta", parseNullableNumber(value))} />
            <LabeledInput label="RSOM" value={draft.rsom ?? ""} onChange={(value) => setField("rsom", value)} />
            <LabeledInput label="DM" value={draft.dm ?? ""} onChange={(value) => setField("dm", value)} />
            <LabeledInput label="Rental" value={draft.rental ?? ""} type="number" step="0.01" onChange={(value) => setField("rental", parseNullableNumber(value))} />
            <LabeledSelect label="Lifecycle State" optionFieldName="lifecycleState" value={draft.lifecycleState} options={optionsByField.get("lifecycleState") ?? []} onChange={(value) => setField("lifecycleState", value)} />
            <LabeledSelect label="Ramp Profile" optionFieldName="rampProfileCode" value={draft.rampProfileCode ?? ""} options={optionsByField.get("rampProfileCode") ?? []} onChange={(value) => setField("rampProfileCode", value)} />
            <LabeledSelect label="Store Cluster Role" optionFieldName="storeClusterRole" value={draft.storeClusterRole ?? ""} options={optionsByField.get("storeClusterRole") ?? []} onChange={(value) => setField("storeClusterRole", value)} />
            <LabeledInput label="Store Capacity SqFt" value={draft.storeCapacitySqFt ?? ""} type="number" step="0.01" onChange={(value) => setField("storeCapacitySqFt", parseNullableNumber(value))} />
            <LabeledSelect label="Store Format Tier" optionFieldName="storeFormatTier" value={draft.storeFormatTier ?? ""} options={optionsByField.get("storeFormatTier") ?? []} onChange={(value) => setField("storeFormatTier", value)} />
            <LabeledSelect label="Catchment Type" optionFieldName="catchmentType" value={draft.catchmentType ?? ""} options={optionsByField.get("catchmentType") ?? []} onChange={(value) => setField("catchmentType", value)} />
            <LabeledSelect label="Demographic Segment" optionFieldName="demographicSegment" value={draft.demographicSegment ?? ""} options={optionsByField.get("demographicSegment") ?? []} onChange={(value) => setField("demographicSegment", value)} />
            <LabeledSelect label="Climate Zone" optionFieldName="climateZone" value={draft.climateZone ?? ""} options={optionsByField.get("climateZone") ?? []} onChange={(value) => setField("climateZone", value)} />
            <LabeledSelect label="Store Opening Season" optionFieldName="storeOpeningSeason" value={draft.storeOpeningSeason ?? ""} options={optionsByField.get("storeOpeningSeason") ?? []} onChange={(value) => setField("storeOpeningSeason", value)} />
            <LabeledInput label="Store Closure Date" value={draft.storeClosureDate ?? ""} type="date" onChange={(value) => setField("storeClosureDate", value)} />
            <LabeledInput label="Refurbishment Date" value={draft.refurbishmentDate ?? ""} type="date" onChange={(value) => setField("refurbishmentDate", value)} />
            <LabeledSelect label="Store Priority" optionFieldName="storePriority" value={draft.storePriority ?? ""} options={optionsByField.get("storePriority") ?? []} onChange={(value) => setField("storePriority", value)} />
            <label className="labeled-field checkbox-field">
              <span>Fulfilment Enabled</span>
              <input type="checkbox" checked={draft.fulfilmentEnabled ?? false} onChange={(event) => setField("fulfilmentEnabled", event.target.checked)} />
            </label>
            <label className="labeled-field checkbox-field">
              <span>Online Fulfilment Node</span>
              <input type="checkbox" checked={draft.onlineFulfilmentNode ?? false} onChange={(event) => setField("onlineFulfilmentNode", event.target.checked)} />
            </label>
            <label className="labeled-field checkbox-field">
              <span>Active</span>
              <input type="checkbox" checked={draft.isActive} onChange={(event) => setField("isActive", event.target.checked)} />
            </label>
          </div>

          <div className="maintenance-toolbar store-profile-actions">
            <div className="toolbar-actions toolbar-actions-wrap">
              <button type="button" className="secondary-button secondary-button-active" onClick={() => void onSave(draft)}>
                Save Store
              </button>
              {draft.storeId > 0 ? (
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
            <div className="eyebrow">Option maintenance</div>
            <h3>Store Field Values</h3>
          </div>
        </div>
        <div className="store-option-form">
          <label className="labeled-field">
            <span>Field</span>
            <select value={optionFieldName} onChange={(event) => setOptionFieldName(event.target.value)}>
              {optionFields.map((field) => (
                <option key={field.optionFieldName} value={field.optionFieldName}>
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
            <div key={field.optionFieldName} className="store-option-card">
              <strong>{field.label}</strong>
              <ul>
                {(optionsByField.get(field.optionFieldName) ?? []).map((option) => (
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

function parseNullableNumber(value: string): number | null {
  if (!value.trim()) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
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
  options: StoreProfileOption[];
  onChange: (value: string) => void;
}) {
  return (
    <label className="labeled-field">
      <span>{label}</span>
      <input list={`${optionFieldName}-options`} value={value} onChange={(event) => onChange(event.target.value)} />
      <datalist id={`${optionFieldName}-options`}>
        {options.filter((option) => option.isActive).map((option) => (
          <option key={`${option.fieldName}:${option.value}`} value={option.value} />
        ))}
      </datalist>
    </label>
  );
}
