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
             "rental", "store_cluster_role", "store_capacity_sqft", "store_format_tier", "catchment_type",
             "demographic_segment", "climate_zone", "fulfilment_enabled", "online_fulfilment_node",
             "store_opening_season", "store_closure_date", "refurbishment_date", "store_priority", "is_active"],
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
             "ramadhan_promo", "supplier", "lifecycle_stage", "age_stage", "gender_target", "material", "pack_size",
             "size_range", "colour_family", "kvi_flag", "markdown_eligible", "markdown_floor_price",
             "minimum_margin_pct", "price_ladder_group", "good_better_best_tier", "season_code", "event_code",
             "launch_date", "end_of_life_date", "substitute_group", "companion_group", "replenishment_type",
             "lead_time_days", "moq", "case_pack", "starting_inventory", "projected_stock_on_hand",
             "sell_through_target_pct", "weeks_of_cover_target", "is_active"],
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
            ["department_label", "class_label", "subclass_label"]),
        new("inventory_profiles",
            ["inventory_profile_id", "store_code", "product_code", "starting_inventory", "inbound_qty", "reserved_qty",
             "projected_stock_on_hand", "safety_stock", "weeks_of_cover_target", "sell_through_target_pct", "is_active"],
            ["inventory_profile_id"]),
        new("pricing_policies",
            ["pricing_policy_id", "department", "class_label", "subclass", "brand", "price_ladder_group", "min_price",
             "max_price", "markdown_floor_price", "minimum_margin_pct", "kvi_flag", "markdown_eligible", "is_active"],
            ["pricing_policy_id"]),
        new("seasonality_event_profiles",
            ["seasonality_event_profile_id", "department", "class_label", "subclass", "season_code", "event_code",
             "month", "weight", "promo_window", "peak_flag", "is_active"],
            ["seasonality_event_profile_id"]),
        new("vendor_supply_profiles",
            ["vendor_supply_profile_id", "supplier", "brand", "lead_time_days", "moq", "case_pack",
             "replenishment_type", "payment_terms", "is_active"],
            ["vendor_supply_profile_id"]),
        new("planning_command_batches",
            ["command_batch_id", "scenario_version_id", "user_id", "command_kind", "command_scope_json", "is_undone",
             "superseded_by_batch_id", "created_at", "undone_at"],
            ["command_batch_id"]),
        new("planning_command_cell_deltas",
            ["command_delta_id", "command_batch_id", "scenario_version_id", "measure_id", "store_id", "product_node_id",
             "time_period_id", "old_input_value", "new_input_value", "old_override_value", "new_override_value",
             "old_is_system_generated_override", "new_is_system_generated_override", "old_derived_value", "new_derived_value",
             "old_effective_value", "new_effective_value", "old_growth_factor", "new_growth_factor",
             "old_is_locked", "new_is_locked", "old_lock_reason", "new_lock_reason", "old_locked_by", "new_locked_by",
             "old_row_version", "new_row_version", "old_cell_kind", "new_cell_kind", "change_kind"],
            ["command_delta_id"])
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
        "vendor_supply_profiles",
        "seasonality_event_profiles",
        "pricing_policies",
        "inventory_profiles",
        "planning_command_cell_deltas",
        "planning_command_batches",
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
            action_id bigserial primary key,
            action_type text not null,
            method text not null,
            user_id text not null,
            comment text null,
            created_at text not null
        );
        create table if not exists audit_deltas (
            audit_delta_id bigserial primary key,
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
            store_cluster_role text null,
            store_capacity_sqft real null,
            store_format_tier text null,
            catchment_type text null,
            demographic_segment text null,
            climate_zone text null,
            fulfilment_enabled integer not null default 0,
            online_fulfilment_node integer not null default 0,
            store_opening_season text null,
            store_closure_date text null,
            refurbishment_date text null,
            store_priority text null,
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
            supplier text null,
            lifecycle_stage text null,
            age_stage text null,
            gender_target text null,
            material text null,
            pack_size text null,
            size_range text null,
            colour_family text null,
            kvi_flag integer not null default 0,
            markdown_eligible integer not null default 1,
            markdown_floor_price real null,
            minimum_margin_pct real null,
            price_ladder_group text null,
            good_better_best_tier text null,
            season_code text null,
            event_code text null,
            launch_date text null,
            end_of_life_date text null,
            substitute_group text null,
            companion_group text null,
            replenishment_type text null,
            lead_time_days integer null,
            moq integer null,
            case_pack integer null,
            starting_inventory real null,
            projected_stock_on_hand real null,
            sell_through_target_pct real null,
            weeks_of_cover_target real null,
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
        create table if not exists inventory_profiles (
            inventory_profile_id integer primary key,
            store_code text not null,
            product_code text not null,
            starting_inventory real not null,
            inbound_qty real null,
            reserved_qty real null,
            projected_stock_on_hand real null,
            safety_stock real null,
            weeks_of_cover_target real null,
            sell_through_target_pct real null,
            is_active integer not null default 1
        );
        create table if not exists pricing_policies (
            pricing_policy_id integer primary key,
            department text null,
            class_label text null,
            subclass text null,
            brand text null,
            price_ladder_group text null,
            min_price real null,
            max_price real null,
            markdown_floor_price real null,
            minimum_margin_pct real null,
            kvi_flag integer not null default 0,
            markdown_eligible integer not null default 1,
            is_active integer not null default 1
        );
        create table if not exists seasonality_event_profiles (
            seasonality_event_profile_id integer primary key,
            department text null,
            class_label text null,
            subclass text null,
            season_code text null,
            event_code text null,
            month integer not null,
            weight real not null,
            promo_window text null,
            peak_flag integer not null default 0,
            is_active integer not null default 1
        );
        create table if not exists vendor_supply_profiles (
            vendor_supply_profile_id integer primary key,
            supplier text not null,
            brand text null,
            lead_time_days integer null,
            moq integer null,
            case_pack integer null,
            replenishment_type text null,
            payment_terms text null,
            is_active integer not null default 1
        );
        create table if not exists planning_command_batches (
            command_batch_id integer primary key,
            scenario_version_id integer not null,
            user_id text not null,
            command_kind text not null,
            command_scope_json text null,
            is_undone integer not null default 0,
            superseded_by_batch_id integer null,
            created_at text not null,
            undone_at text null
        );
        create table if not exists planning_command_cell_deltas (
            command_delta_id integer primary key,
            command_batch_id integer not null,
            scenario_version_id integer not null,
            measure_id integer not null,
            store_id integer not null,
            product_node_id integer not null,
            time_period_id integer not null,
            old_input_value real null,
            new_input_value real null,
            old_override_value real null,
            new_override_value real null,
            old_is_system_generated_override integer not null default 0,
            new_is_system_generated_override integer not null default 0,
            old_derived_value real not null,
            new_derived_value real not null,
            old_effective_value real not null,
            new_effective_value real not null,
            old_growth_factor real not null default 1.0,
            new_growth_factor real not null default 1.0,
            old_is_locked integer not null default 0,
            new_is_locked integer not null default 0,
            old_lock_reason text null,
            new_lock_reason text null,
            old_locked_by text null,
            new_locked_by text null,
            old_row_version integer not null,
            new_row_version integer not null,
            old_cell_kind text not null,
            new_cell_kind text not null,
            change_kind text not null
        );
        create index if not exists idx_planning_cells_scenario_measure on planning_cells (scenario_version_id, measure_id);
        create index if not exists idx_audit_deltas_lookup on audit_deltas (scenario_version_id, measure_id, store_id, product_node_id);
        create index if not exists idx_inventory_profiles_lookup on inventory_profiles (store_code, product_code);
        create index if not exists idx_pricing_policies_lookup on pricing_policies (department, class_label, subclass, brand, price_ladder_group);
        create index if not exists idx_seasonality_profiles_lookup on seasonality_event_profiles (department, class_label, subclass, season_code, event_code, month);
        create index if not exists idx_vendor_supply_profiles_lookup on vendor_supply_profiles (supplier, brand);
        create index if not exists idx_command_batches_lookup on planning_command_batches (scenario_version_id, user_id, created_at);
        create index if not exists idx_command_deltas_lookup on planning_command_cell_deltas (command_batch_id, scenario_version_id, measure_id, store_id, product_node_id, time_period_id);
        """;

    public static PostgresTableDefinition Get(string tableName) =>
        All.Single(definition => string.Equals(definition.Name, tableName, StringComparison.Ordinal));
}
