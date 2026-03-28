namespace SalesPlanning.Api.Infrastructure.Postgres;

internal sealed record PostgresTableDefinition(
    string Name,
    string[] Columns,
    string[] PrimaryKeyColumns);

internal static class PostgresTableDefinitions
{
    public static readonly IReadOnlyList<PostgresTableDefinition> All =
    [
        new("time_periods",
            ["time_period_id", "parent_time_period_id", "label", "grain", "sort_order"],
            ["time_period_id"]),
        new("store_metadata",
            ["store_id", "store_label", "cluster_label", "region_label", "lifecycle_state", "ramp_profile_code",
             "effective_from_time_period_id", "effective_to_time_period_id", "store_code", "state", "latitude", "longitude",
             "opening_date", "sssg", "sales_type", "status", "storey", "building_status", "gta", "nta", "rsom", "dm",
             "rental", "is_active"],
            ["store_id"]),
        new("product_nodes",
            ["product_node_id", "store_id", "parent_product_node_id", "label", "level", "path_json", "is_leaf",
             "node_kind", "lifecycle_state", "ramp_profile_code", "effective_from_time_period_id", "effective_to_time_period_id"],
            ["product_node_id"]),
        new("planning_cells",
            ["scenario_version_id", "measure_id", "store_id", "product_node_id", "time_period_id", "input_value",
             "override_value", "is_system_generated_override", "derived_value", "effective_value", "growth_factor",
             "is_locked", "lock_reason", "locked_by", "row_version", "cell_kind"],
            ["scenario_version_id", "measure_id", "store_id", "product_node_id", "time_period_id"]),
        new("audits",
            ["action_id", "action_type", "method", "user_id", "comment", "created_at"],
            ["action_id"]),
        new("audit_deltas",
            ["audit_delta_id", "action_id", "scenario_version_id", "measure_id", "store_id", "product_node_id", "time_period_id",
             "old_value", "new_value", "was_locked", "change_kind"],
            ["audit_delta_id"]),
        new("hierarchy_categories",
            ["category_label"],
            ["category_label"]),
        new("hierarchy_subcategories",
            ["category_label", "subcategory_label"],
            ["category_label", "subcategory_label"]),
        new("store_profile_options",
            ["field_name", "option_value", "is_active"],
            ["field_name", "option_value"]),
        new("product_profiles",
            ["sku_variant", "description", "description2", "price", "cost", "dpt_no", "clss_no", "brand_no",
             "department", "class", "brand", "rev_department", "rev_class", "subclass", "prod_group", "prod_type",
             "active_flag", "order_flag", "brand_type", "launch_month", "gender", "size", "collection", "promo",
             "ramadhan_promo", "is_active"],
            ["sku_variant"]),
        new("product_profile_options",
            ["field_name", "option_value", "is_active"],
            ["field_name", "option_value"]),
        new("product_hierarchy_catalog",
            ["dpt_no", "clss_no", "department", "class", "prod_group", "is_active"],
            ["dpt_no", "clss_no"]),
        new("product_subclass_catalog",
            ["department", "class", "subclass", "is_active"],
            ["department", "class", "subclass"]),
        new("hierarchy_departments_v2",
            ["department_label", "lifecycle_state", "ramp_profile_code", "effective_from_time_period_id", "effective_to_time_period_id"],
            ["department_label"]),
        new("hierarchy_classes_v2",
            ["department_label", "class_label", "lifecycle_state", "ramp_profile_code", "effective_from_time_period_id", "effective_to_time_period_id"],
            ["department_label", "class_label"]),
        new("hierarchy_subclasses_v2",
            ["department_label", "class_label", "subclass_label", "lifecycle_state", "ramp_profile_code",
             "effective_from_time_period_id", "effective_to_time_period_id"],
            ["department_label", "class_label", "subclass_label"])
    ];

    public static readonly IReadOnlyList<string> FullSnapshotDeleteOrder =
    [
        "audit_deltas",
        "audits",
        "planning_cells",
        "product_nodes",
        "store_profile_options",
        "store_metadata",
        "product_profile_options",
        "product_profiles",
        "product_subclass_catalog",
        "product_hierarchy_catalog",
        "hierarchy_subclasses_v2",
        "hierarchy_classes_v2",
        "hierarchy_departments_v2",
        "hierarchy_subcategories",
        "hierarchy_categories",
        "time_periods"
    ];

    public static readonly string SqliteCacheSchema = """
        create table if not exists product_nodes (
            product_node_id integer primary key,
            store_id integer not null,
            parent_product_node_id integer null,
            label text not null,
            level integer not null,
            path_json text not null,
            is_leaf integer not null,
            node_kind text null,
            lifecycle_state text null,
            ramp_profile_code text null,
            effective_from_time_period_id integer null,
            effective_to_time_period_id integer null
        );
        create table if not exists time_periods (
            time_period_id integer primary key,
            parent_time_period_id integer null,
            label text not null,
            grain text not null,
            sort_order integer not null
        );
        create table if not exists planning_cells (
            scenario_version_id integer not null,
            measure_id integer not null,
            store_id integer not null,
            product_node_id integer not null,
            time_period_id integer not null,
            input_value real null,
            override_value real null,
            is_system_generated_override integer not null,
            derived_value real not null,
            effective_value real not null,
            growth_factor real not null default 1.0,
            is_locked integer not null,
            lock_reason text null,
            locked_by text null,
            row_version integer not null,
            cell_kind text not null,
            primary key (scenario_version_id, measure_id, store_id, product_node_id, time_period_id)
        );
        create table if not exists audits (
            action_id integer primary key,
            action_type text not null,
            method text not null,
            user_id text not null,
            comment text null,
            created_at text not null
        );
        create table if not exists audit_deltas (
            audit_delta_id integer primary key,
            action_id integer not null,
            scenario_version_id integer not null,
            measure_id integer not null,
            store_id integer not null,
            product_node_id integer not null,
            time_period_id integer not null,
            old_value real not null,
            new_value real not null,
            was_locked integer not null,
            change_kind text not null
        );
        create table if not exists hierarchy_categories (
            category_label text primary key
        );
        create table if not exists hierarchy_subcategories (
            category_label text not null,
            subcategory_label text not null,
            primary key (category_label, subcategory_label)
        );
        create table if not exists store_metadata (
            store_id integer primary key,
            store_label text not null,
            cluster_label text not null,
            region_label text not null,
            lifecycle_state text not null default 'active',
            ramp_profile_code text null,
            effective_from_time_period_id integer null,
            effective_to_time_period_id integer null,
            store_code text null,
            state text null,
            latitude real null,
            longitude real null,
            opening_date text null,
            sssg text null,
            sales_type text null,
            status text null,
            storey text null,
            building_status text null,
            gta real null,
            nta real null,
            rsom text null,
            dm text null,
            rental real null,
            is_active integer not null default 1
        );
        create table if not exists store_profile_options (
            field_name text not null,
            option_value text not null,
            is_active integer not null default 1,
            primary key (field_name, option_value)
        );
        create table if not exists product_profiles (
            sku_variant text primary key,
            description text not null,
            description2 text null,
            price real not null,
            cost real not null,
            dpt_no text not null,
            clss_no text not null,
            brand_no text null,
            department text not null,
            class text not null,
            brand text null,
            rev_department text null,
            rev_class text null,
            subclass text not null,
            prod_group text null,
            prod_type text null,
            active_flag text null,
            order_flag text null,
            brand_type text null,
            launch_month text null,
            gender text null,
            size text null,
            collection text null,
            promo text null,
            ramadhan_promo text null,
            is_active integer not null default 1
        );
        create table if not exists product_profile_options (
            field_name text not null,
            option_value text not null,
            is_active integer not null default 1,
            primary key (field_name, option_value)
        );
        create table if not exists product_hierarchy_catalog (
            dpt_no text not null,
            clss_no text not null,
            department text not null,
            class text not null,
            prod_group text not null,
            is_active integer not null default 1,
            primary key (dpt_no, clss_no)
        );
        create table if not exists product_subclass_catalog (
            department text not null,
            class text not null,
            subclass text not null,
            is_active integer not null default 1,
            primary key (department, class, subclass)
        );
        create table if not exists hierarchy_departments_v2 (
            department_label text primary key,
            lifecycle_state text not null default 'active',
            ramp_profile_code text null,
            effective_from_time_period_id integer null,
            effective_to_time_period_id integer null
        );
        create table if not exists hierarchy_classes_v2 (
            department_label text not null,
            class_label text not null,
            lifecycle_state text not null default 'active',
            ramp_profile_code text null,
            effective_from_time_period_id integer null,
            effective_to_time_period_id integer null,
            primary key (department_label, class_label)
        );
        create table if not exists hierarchy_subclasses_v2 (
            department_label text not null,
            class_label text not null,
            subclass_label text not null,
            lifecycle_state text not null default 'active',
            ramp_profile_code text null,
            effective_from_time_period_id integer null,
            effective_to_time_period_id integer null,
            primary key (department_label, class_label, subclass_label)
        );
        create index if not exists idx_planning_cells_scenario_measure on planning_cells (scenario_version_id, measure_id);
        create index if not exists idx_audit_deltas_lookup on audit_deltas (scenario_version_id, measure_id, store_id, product_node_id);
        """;

    public static PostgresTableDefinition Get(string tableName) =>
        All.Single(definition => string.Equals(definition.Name, tableName, StringComparison.Ordinal));
}
