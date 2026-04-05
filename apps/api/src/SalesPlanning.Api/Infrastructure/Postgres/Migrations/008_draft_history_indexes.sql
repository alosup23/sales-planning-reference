create index if not exists idx_draft_command_batches_state_lookup
    on planning_draft_command_batches (
        scenario_version_id,
        user_id,
        superseded_by_batch_id,
        is_undone,
        command_batch_id desc
    );
