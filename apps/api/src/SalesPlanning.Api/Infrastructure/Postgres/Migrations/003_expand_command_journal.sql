alter table if exists planning_command_batches add column if not exists undone_at timestamptz null;

alter table if exists planning_command_cell_deltas add column if not exists old_is_system_generated_override integer not null default 0;
alter table if exists planning_command_cell_deltas add column if not exists new_is_system_generated_override integer not null default 0;
alter table if exists planning_command_cell_deltas add column if not exists old_derived_value numeric not null default 0;
alter table if exists planning_command_cell_deltas add column if not exists new_derived_value numeric not null default 0;
alter table if exists planning_command_cell_deltas add column if not exists old_growth_factor numeric not null default 1.0;
alter table if exists planning_command_cell_deltas add column if not exists new_growth_factor numeric not null default 1.0;
alter table if exists planning_command_cell_deltas add column if not exists old_lock_reason text null;
alter table if exists planning_command_cell_deltas add column if not exists new_lock_reason text null;
alter table if exists planning_command_cell_deltas add column if not exists old_locked_by text null;
alter table if exists planning_command_cell_deltas add column if not exists new_locked_by text null;
alter table if exists planning_command_cell_deltas add column if not exists old_row_version bigint not null default 0;
alter table if exists planning_command_cell_deltas add column if not exists new_row_version bigint not null default 0;
alter table if exists planning_command_cell_deltas add column if not exists old_cell_kind text not null default 'input';
alter table if exists planning_command_cell_deltas add column if not exists new_cell_kind text not null default 'input';
