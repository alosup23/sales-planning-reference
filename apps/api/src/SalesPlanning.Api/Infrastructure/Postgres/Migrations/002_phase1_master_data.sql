alter table if exists store_metadata add column if not exists store_cluster_role text null;
alter table if exists store_metadata add column if not exists store_capacity_sqft numeric null;
alter table if exists store_metadata add column if not exists store_format_tier text null;
alter table if exists store_metadata add column if not exists catchment_type text null;
alter table if exists store_metadata add column if not exists demographic_segment text null;
alter table if exists store_metadata add column if not exists climate_zone text null;
alter table if exists store_metadata add column if not exists fulfilment_enabled integer not null default 0;
alter table if exists store_metadata add column if not exists online_fulfilment_node integer not null default 0;
alter table if exists store_metadata add column if not exists store_opening_season text null;
alter table if exists store_metadata add column if not exists store_closure_date text null;
alter table if exists store_metadata add column if not exists refurbishment_date text null;
alter table if exists store_metadata add column if not exists store_priority text null;

alter table if exists product_profiles add column if not exists supplier text null;
alter table if exists product_profiles add column if not exists lifecycle_stage text null;
alter table if exists product_profiles add column if not exists age_stage text null;
alter table if exists product_profiles add column if not exists gender_target text null;
alter table if exists product_profiles add column if not exists material text null;
alter table if exists product_profiles add column if not exists pack_size text null;
alter table if exists product_profiles add column if not exists size_range text null;
alter table if exists product_profiles add column if not exists colour_family text null;
alter table if exists product_profiles add column if not exists kvi_flag integer not null default 0;
alter table if exists product_profiles add column if not exists markdown_eligible integer not null default 1;
alter table if exists product_profiles add column if not exists markdown_floor_price numeric null;
alter table if exists product_profiles add column if not exists minimum_margin_pct numeric null;
alter table if exists product_profiles add column if not exists price_ladder_group text null;
alter table if exists product_profiles add column if not exists good_better_best_tier text null;
alter table if exists product_profiles add column if not exists season_code text null;
alter table if exists product_profiles add column if not exists event_code text null;
alter table if exists product_profiles add column if not exists launch_date text null;
alter table if exists product_profiles add column if not exists end_of_life_date text null;
alter table if exists product_profiles add column if not exists substitute_group text null;
alter table if exists product_profiles add column if not exists companion_group text null;
alter table if exists product_profiles add column if not exists replenishment_type text null;
alter table if exists product_profiles add column if not exists lead_time_days integer null;
alter table if exists product_profiles add column if not exists moq integer null;
alter table if exists product_profiles add column if not exists case_pack integer null;
alter table if exists product_profiles add column if not exists starting_inventory numeric null;
alter table if exists product_profiles add column if not exists projected_stock_on_hand numeric null;
alter table if exists product_profiles add column if not exists sell_through_target_pct numeric null;
alter table if exists product_profiles add column if not exists weeks_of_cover_target numeric null;

create table if not exists inventory_profiles (
    inventory_profile_id bigserial primary key,
    store_code text not null,
    product_code text not null,
    starting_inventory numeric not null,
    inbound_qty numeric null,
    reserved_qty numeric null,
    projected_stock_on_hand numeric null,
    safety_stock numeric null,
    weeks_of_cover_target numeric null,
    sell_through_target_pct numeric null,
    is_active integer not null default 1
);

create table if not exists pricing_policies (
    pricing_policy_id bigserial primary key,
    department text null,
    class_label text null,
    subclass text null,
    brand text null,
    price_ladder_group text null,
    min_price numeric null,
    max_price numeric null,
    markdown_floor_price numeric null,
    minimum_margin_pct numeric null,
    kvi_flag integer not null default 0,
    markdown_eligible integer not null default 1,
    is_active integer not null default 1
);

create table if not exists seasonality_event_profiles (
    seasonality_event_profile_id bigserial primary key,
    department text null,
    class_label text null,
    subclass text null,
    season_code text null,
    event_code text null,
    month integer not null,
    weight numeric not null,
    promo_window text null,
    peak_flag integer not null default 0,
    is_active integer not null default 1
);

create table if not exists vendor_supply_profiles (
    vendor_supply_profile_id bigserial primary key,
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
    command_batch_id bigserial primary key,
    scenario_version_id bigint not null,
    user_id text not null,
    command_kind text not null,
    command_scope_json text null,
    is_undone integer not null default 0,
    superseded_by_batch_id bigint null,
    created_at timestamptz not null default now()
);

create table if not exists planning_command_cell_deltas (
    command_delta_id bigserial primary key,
    command_batch_id bigint not null,
    scenario_version_id bigint not null,
    measure_id bigint not null,
    store_id bigint not null,
    product_node_id bigint not null,
    time_period_id bigint not null,
    old_input_value numeric null,
    new_input_value numeric null,
    old_override_value numeric null,
    new_override_value numeric null,
    old_effective_value numeric not null,
    new_effective_value numeric not null,
    old_is_locked integer not null default 0,
    new_is_locked integer not null default 0,
    change_kind text not null
);

create index if not exists idx_inventory_profiles_lookup on inventory_profiles (store_code, product_code);
create index if not exists idx_pricing_policies_lookup on pricing_policies (department, class_label, subclass, brand, price_ladder_group);
create index if not exists idx_seasonality_profiles_lookup on seasonality_event_profiles (department, class_label, subclass, season_code, event_code, month);
create index if not exists idx_vendor_supply_profiles_lookup on vendor_supply_profiles (supplier, brand);
create index if not exists idx_command_batches_lookup on planning_command_batches (scenario_version_id, user_id, created_at desc);
create index if not exists idx_command_deltas_lookup on planning_command_cell_deltas (command_batch_id, scenario_version_id, measure_id, store_id, product_node_id, time_period_id);
