import { useEffect, useState } from "react";
import type { HierarchyDepartment } from "../lib/types";

type HierarchyMaintenanceSheetProps = {
  departments: HierarchyDepartment[];
  onAddDepartment: () => Promise<void>;
  onAddClass: (departmentLabel: string) => Promise<void>;
};

export function HierarchyMaintenanceSheet({
  departments,
  onAddDepartment,
  onAddClass,
}: HierarchyMaintenanceSheetProps) {
  const [selectedDepartment, setSelectedDepartment] = useState<string | null>(departments[0]?.departmentLabel ?? null);

  useEffect(() => {
    if (!selectedDepartment || departments.some((department) => department.departmentLabel === selectedDepartment)) {
      return;
    }

    setSelectedDepartment(departments[0]?.departmentLabel ?? null);
  }, [departments, selectedDepartment]);

  return (
    <section className="maintenance-shell">
      <div className="maintenance-toolbar">
        <div>
          <div className="eyebrow">Maintenance</div>
          <strong>Department / Class Mapping</strong>
        </div>
        <div className="toolbar-actions">
          <button type="button" className="secondary-button" onClick={() => void onAddDepartment()}>
            Add Department
          </button>
          <button
            type="button"
            className="secondary-button"
            disabled={!selectedDepartment}
            onClick={() => {
              if (!selectedDepartment) {
                return;
              }

              void onAddClass(selectedDepartment);
            }}
          >
            Add Class
          </button>
        </div>
      </div>

      <div className="maintenance-grid">
        <div className="maintenance-header">
          <div>Department</div>
          <div>Classes</div>
        </div>
        {departments.map((department) => (
          <button
            key={department.departmentLabel}
            type="button"
            className={`maintenance-row${selectedDepartment === department.departmentLabel ? " maintenance-row-selected" : ""}`}
            onClick={() => setSelectedDepartment(department.departmentLabel)}
          >
            <span>{department.departmentLabel}</span>
            <span>{department.classLabels.length > 0 ? department.classLabels.join(", ") : "No classes yet"}</span>
          </button>
        ))}
      </div>
    </section>
  );
}
