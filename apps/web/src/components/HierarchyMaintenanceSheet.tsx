import { useEffect, useState } from "react";
import type { HierarchyCategory } from "../lib/types";

type HierarchyMaintenanceSheetProps = {
  categories: HierarchyCategory[];
  onAddCategory: () => Promise<void>;
  onAddSubcategory: (categoryLabel: string) => Promise<void>;
};

export function HierarchyMaintenanceSheet({
  categories,
  onAddCategory,
  onAddSubcategory,
}: HierarchyMaintenanceSheetProps) {
  const [selectedCategory, setSelectedCategory] = useState<string | null>(categories[0]?.categoryLabel ?? null);

  useEffect(() => {
    if (!selectedCategory || categories.some((category) => category.categoryLabel === selectedCategory)) {
      return;
    }

    setSelectedCategory(categories[0]?.categoryLabel ?? null);
  }, [categories, selectedCategory]);

  return (
    <section className="maintenance-shell">
      <div className="maintenance-toolbar">
        <div>
          <div className="eyebrow">Maintenance</div>
          <strong>Category / Subcategory Mapping</strong>
        </div>
        <div className="toolbar-actions">
          <button type="button" className="secondary-button" onClick={() => void onAddCategory()}>
            Add Category
          </button>
          <button
            type="button"
            className="secondary-button"
            disabled={!selectedCategory}
            onClick={() => {
              if (!selectedCategory) {
                return;
              }

              void onAddSubcategory(selectedCategory);
            }}
          >
            Add Subcategory
          </button>
        </div>
      </div>

      <div className="maintenance-grid">
        <div className="maintenance-header">
          <div>Category</div>
          <div>Subcategories</div>
        </div>
        {categories.map((category) => (
          <button
            key={category.categoryLabel}
            type="button"
            className={`maintenance-row${selectedCategory === category.categoryLabel ? " maintenance-row-selected" : ""}`}
            onClick={() => setSelectedCategory(category.categoryLabel)}
          >
            <span>{category.categoryLabel}</span>
            <span>{category.subcategoryLabels.length > 0 ? category.subcategoryLabels.join(", ") : "No subcategories yet"}</span>
          </button>
        ))}
      </div>
    </section>
  );
}
