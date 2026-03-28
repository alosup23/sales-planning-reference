create table if not exists schema_migrations (
    migration_id text primary key,
    applied_at timestamptz not null default now()
);

create table if not exists seed_runs (
    seed_key text primary key,
    source_name text not null,
    status text not null,
    started_at timestamptz not null default now(),
    completed_at timestamptz null,
    details_json text null
);

create table if not exists planning_data_state (
    state_key text primary key,
    data_version bigint not null,
    updated_at timestamptz not null default now()
);

insert into planning_data_state (state_key, data_version)
values ('default', 0)
on conflict (state_key) do nothing;

create table if not exists product_nodes (
    product_node_id bigint primary key,
    store_id bigint not null,
    parent_product_node_id bigint null,
    label text not null,
    level integer not null,
    path_json text not null,
    is_leaf integer not null,
    node_kind text null,
    lifecycle_state text null,
    ramp_profile_code text null,
    effective_from_time_period_id bigint null,
    effective_to_time_period_id bigint null
);

create table if not exists time_periods (
    time_period_id bigint primary key,
    parent_time_period_id bigint null,
    label text not null,
    grain text not null,
    sort_order integer not null
);

create table if not exists planning_cells (
    scenario_version_id bigint not null,
    measure_id bigint not null,
    store_id bigint not null,
    product_node_id bigint not null,
    time_period_id bigint not null,
    input_value numeric null,
    override_value numeric null,
    is_system_generated_override integer not null,
    derived_value numeric not null,
    effective_value numeric not null,
    growth_factor numeric not null default 1.0,
    is_locked integer not null,
    lock_reason text null,
    locked_by text null,
    row_version bigint not null,
    cell_kind text not null,
    primary key (scenario_version_id, measure_id, store_id, product_node_id, time_period_id)
);

create table if not exists audits (
    action_id bigint primary key,
    action_type text not null,
    method text not null,
    user_id text not null,
    comment text null,
    created_at text not null
);

create table if not exists audit_deltas (
    audit_delta_id bigint primary key,
    action_id bigint not null,
    scenario_version_id bigint not null,
    measure_id bigint not null,
    store_id bigint not null,
    product_node_id bigint not null,
    time_period_id bigint not null,
    old_value numeric not null,
    new_value numeric not null,
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
    store_id bigint primary key,
    store_label text not null,
    cluster_label text not null,
    region_label text not null,
    lifecycle_state text not null default 'active',
    ramp_profile_code text null,
    effective_from_time_period_id bigint null,
    effective_to_time_period_id bigint null,
    store_code text null,
    state text null,
    latitude numeric null,
    longitude numeric null,
    opening_date text null,
    sssg text null,
    sales_type text null,
    status text null,
    storey text null,
    building_status text null,
    gta numeric null,
    nta numeric null,
    rsom text null,
    dm text null,
    rental numeric null,
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
    price numeric not null,
    cost numeric not null,
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
    effective_from_time_period_id bigint null,
    effective_to_time_period_id bigint null
);

create table if not exists hierarchy_classes_v2 (
    department_label text not null,
    class_label text not null,
    lifecycle_state text not null default 'active',
    ramp_profile_code text null,
    effective_from_time_period_id bigint null,
    effective_to_time_period_id bigint null,
    primary key (department_label, class_label)
);

create table if not exists hierarchy_subclasses_v2 (
    department_label text not null,
    class_label text not null,
    subclass_label text not null,
    lifecycle_state text not null default 'active',
    ramp_profile_code text null,
    effective_from_time_period_id bigint null,
    effective_to_time_period_id bigint null,
    primary key (department_label, class_label, subclass_label)
);

create index if not exists idx_planning_cells_scenario_measure
    on planning_cells (scenario_version_id, measure_id);

create index if not exists idx_audit_deltas_lookup
    on audit_deltas (scenario_version_id, measure_id, store_id, product_node_id);
