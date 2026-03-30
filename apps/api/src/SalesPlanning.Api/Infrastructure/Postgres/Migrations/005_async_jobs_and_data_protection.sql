create table if not exists planning_async_jobs (
    job_id text primary key,
    category text not null,
    operation text not null,
    requested_by text not null,
    payload_json text not null,
    upload_content bytea null,
    upload_file_name text null,
    status text not null,
    progress_percent integer not null default 0,
    progress_message text not null,
    created_at timestamptz not null default now(),
    started_at timestamptz null,
    completed_at timestamptz null,
    error_message text null,
    summary_json text null,
    download_content bytea null,
    download_file_name text null,
    download_content_type text null,
    worker_id text null,
    attempt_count integer not null default 0,
    retain_until timestamptz null
);

create index if not exists idx_planning_async_jobs_status_created_at
    on planning_async_jobs (status, created_at);

create index if not exists idx_planning_async_jobs_retain_until
    on planning_async_jobs (retain_until);

create table if not exists planning_reconciliation_schedules (
    schedule_key text primary key,
    scenario_version_id bigint not null,
    is_enabled integer not null default 1,
    interval_minutes integer not null default 1440,
    next_run_at timestamptz not null default now() + interval '1 day',
    last_enqueued_at timestamptz null,
    retain_days integer not null default 30
);

insert into planning_reconciliation_schedules (schedule_key, scenario_version_id, is_enabled, interval_minutes, next_run_at, retain_days)
values ('default-scenario-1', 1, 1, 1440, now() + interval '1 day', 30)
on conflict (schedule_key) do nothing;

create table if not exists planning_reconciliation_reports (
    report_id text primary key,
    scenario_version_id bigint not null,
    requested_by text not null,
    is_scheduled boolean not null default false,
    mismatch_count integer not null,
    checked_cell_count integer not null,
    status text not null,
    report_json text not null,
    created_at timestamptz not null default now(),
    retain_until timestamptz null
);

create index if not exists idx_planning_reconciliation_reports_created_at
    on planning_reconciliation_reports (created_at desc);

create index if not exists idx_planning_reconciliation_reports_retain_until
    on planning_reconciliation_reports (retain_until);

create table if not exists app_data_protection_keys (
    friendly_name text primary key,
    xml text not null,
    created_at timestamptz not null default now()
);
