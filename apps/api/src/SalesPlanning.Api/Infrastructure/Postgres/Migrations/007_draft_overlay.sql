create table if not exists planning_draft_cells (
    scenario_version_id bigint not null,
    user_id text not null,
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
    updated_at timestamptz not null default now(),
    primary key (scenario_version_id, user_id, measure_id, store_id, product_node_id, time_period_id)
);

create index if not exists idx_planning_draft_cells_user_lookup
    on planning_draft_cells (scenario_version_id, user_id, store_id, product_node_id, time_period_id);

create table if not exists planning_draft_command_batches (
    command_batch_id bigserial primary key,
    scenario_version_id bigint not null,
    user_id text not null,
    command_kind text not null,
    command_scope_json text null,
    is_undone integer not null default 0,
    superseded_by_batch_id bigint null,
    created_at timestamptz not null default now(),
    undone_at timestamptz null
);

create table if not exists planning_draft_command_cell_deltas (
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
    old_is_system_generated_override integer not null default 0,
    new_is_system_generated_override integer not null default 0,
    old_derived_value numeric not null default 0,
    new_derived_value numeric not null default 0,
    old_effective_value numeric not null,
    new_effective_value numeric not null,
    old_growth_factor numeric not null default 1.0,
    new_growth_factor numeric not null default 1.0,
    old_is_locked integer not null default 0,
    new_is_locked integer not null default 0,
    old_lock_reason text null,
    new_lock_reason text null,
    old_locked_by text null,
    new_locked_by text null,
    old_row_version bigint not null default 0,
    new_row_version bigint not null default 0,
    old_cell_kind text not null default 'input',
    new_cell_kind text not null default 'input',
    change_kind text not null
);

create index if not exists idx_draft_command_batches_lookup
    on planning_draft_command_batches (scenario_version_id, user_id, created_at desc);

create index if not exists idx_draft_command_deltas_lookup
    on planning_draft_command_cell_deltas (command_batch_id, scenario_version_id, measure_id, store_id, product_node_id, time_period_id);
