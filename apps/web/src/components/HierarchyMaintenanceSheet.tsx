import { useEffect, useMemo, useState } from "react";
import type { HierarchyClass, HierarchyDepartment } from "../lib/types";

type HierarchyMaintenanceSheetProps = {
  departments: HierarchyDepartment[];
  onAddDepartment: () => Promise<void>;
  onAddClass: (departmentLabel: string) => Promise<void>;
  onAddSubclass: (departmentLabel: string, classLabel: string) => Promise<void>;
};

export function HierarchyMaintenanceSheet({
  departments,
  onAddDepartment,
  onAddClass,
  onAddSubclass,
}: HierarchyMaintenanceSheetProps) {
  const [selectedDepartment, setSelectedDepartment] = useState<string | null>(departments[0]?.departmentLabel ?? null);
  const selectedDepartmentRecord = useMemo(
    () => departments.find((department) => department.departmentLabel === selectedDepartment) ?? null,
    [departments, selectedDepartment],
  );
  const [selectedClass, setSelectedClass] = useState<string | null>(selectedDepartmentRecord?.classes[0]?.classLabel ?? null);

  useEffect(() => {
    if (!selectedDepartment || departments.some((department) => department.departmentLabel === selectedDepartment)) {
      return;
    }

    setSelectedDepartment(departments[0]?.departmentLabel ?? null);
  }, [departments, selectedDepartment]);

  useEffect(() => {
    if (!selectedDepartmentRecord) {
      setSelectedClass(null);
      return;
    }

    if (!selectedClass || selectedDepartmentRecord.classes.some((item) => item.classLabel === selectedClass)) {
      return;
    }

    setSelectedClass(selectedDepartmentRecord.classes[0]?.classLabel ?? null);
  }, [selectedClass, selectedDepartmentRecord]);

  const selectedClassRecord = selectedDepartmentRecord?.classes.find((item) => item.classLabel === selectedClass) ?? null;

  return (
    <section className="maintenance-shell">
      <div className="maintenance-toolbar">
        <div>
          <div className="eyebrow">Maintenance</div>
          <strong>Department / Class / Subclass Mapping</strong>
        </div>
        <div className="toolbar-actions toolbar-actions-wrap">
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
          <button
            type="button"
            className="secondary-button"
            disabled={!selectedDepartment || !selectedClass}
            onClick={() => {
              if (!selectedDepartment || !selectedClass) {
                return;
              }

              void onAddSubclass(selectedDepartment, selectedClass);
            }}
          >
            Add Subclass
          </button>
        </div>
      </div>

      <div className="maintenance-grid maintenance-grid-deep">
        <div className="maintenance-header maintenance-header-deep">
          <div>Department</div>
          <div>Class</div>
          <div>Subclasses</div>
          <div>Lifecycle / Ramp</div>
        </div>
        {departments.map((department) => (
          <DepartmentRow
            key={department.departmentLabel}
            department={department}
            selectedDepartment={selectedDepartment}
            selectedClass={selectedClass}
            onSelectDepartment={setSelectedDepartment}
            onSelectClass={setSelectedClass}
          />
        ))}
      </div>

      <div className="formula-bar">
        {selectedDepartmentRecord
          ? `${selectedDepartmentRecord.departmentLabel} is ${selectedDepartmentRecord.lifecycleState}. ${
              selectedClassRecord
                ? `${selectedClassRecord.classLabel} has ${selectedClassRecord.subclasses.length} subclasses and ramp ${selectedClassRecord.rampProfileCode ?? "none"}.`
                : "Select a class to inspect its subclasses and ramp profile."
            }`
          : "Select a department to inspect the mapped class and subclass hierarchy."}
      </div>
    </section>
  );
}

function DepartmentRow({
  department,
  selectedDepartment,
  selectedClass,
  onSelectDepartment,
  onSelectClass,
}: {
  department: HierarchyDepartment;
  selectedDepartment: string | null;
  selectedClass: string | null;
  onSelectDepartment: (value: string) => void;
  onSelectClass: (value: string) => void;
}) {
  return (
    <>
      {department.classes.length === 0 ? (
        <button
          type="button"
          className={`maintenance-row${selectedDepartment === department.departmentLabel ? " maintenance-row-selected" : ""}`}
          onClick={() => onSelectDepartment(department.departmentLabel)}
        >
          <span>{department.departmentLabel}</span>
          <span>No classes yet</span>
          <span>No subclasses yet</span>
          <span>{department.lifecycleState} / {department.rampProfileCode ?? "none"}</span>
        </button>
      ) : (
        department.classes.map((classItem, index) => (
          <ClassRow
            key={`${department.departmentLabel}:${classItem.classLabel}`}
            department={department}
            classItem={classItem}
            selectedDepartment={selectedDepartment}
            selectedClass={selectedClass}
            showDepartmentLabel={index === 0}
            onSelectDepartment={onSelectDepartment}
            onSelectClass={onSelectClass}
          />
        ))
      )}
    </>
  );
}

function ClassRow({
  department,
  classItem,
  selectedDepartment,
  selectedClass,
  showDepartmentLabel,
  onSelectDepartment,
  onSelectClass,
}: {
  department: HierarchyDepartment;
  classItem: HierarchyClass;
  selectedDepartment: string | null;
  selectedClass: string | null;
  showDepartmentLabel: boolean;
  onSelectDepartment: (value: string) => void;
  onSelectClass: (value: string) => void;
}) {
  const selected = selectedDepartment === department.departmentLabel && selectedClass === classItem.classLabel;

  return (
    <button
      type="button"
      className={`maintenance-row${selected ? " maintenance-row-selected" : ""}`}
      onClick={() => {
        onSelectDepartment(department.departmentLabel);
        onSelectClass(classItem.classLabel);
      }}
    >
      <span>{showDepartmentLabel ? department.departmentLabel : ""}</span>
      <span>{classItem.classLabel}</span>
      <span>{classItem.subclasses.length > 0 ? classItem.subclasses.map((item) => item.subclassLabel).join(", ") : "No subclasses yet"}</span>
      <span>{classItem.lifecycleState} / {classItem.rampProfileCode ?? "none"}</span>
    </button>
  );
}
