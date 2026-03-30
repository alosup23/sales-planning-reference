create table if not exists planning_save_checkpoints (
    save_checkpoint_id bigserial primary key,
    scenario_version_id bigint not null,
    user_id text not null,
    mode text not null,
    saved_at timestamptz not null,
    data_version bigint not null
);

create index if not exists idx_planning_save_checkpoints_scenario_saved_at
    on planning_save_checkpoints (scenario_version_id, saved_at desc);
