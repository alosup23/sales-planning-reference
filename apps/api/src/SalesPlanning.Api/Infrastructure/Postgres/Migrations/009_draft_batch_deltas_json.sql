alter table planning_draft_command_batches
    add column if not exists deltas_json text null;
