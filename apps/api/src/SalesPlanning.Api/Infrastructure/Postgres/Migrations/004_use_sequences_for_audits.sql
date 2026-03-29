create sequence if not exists audits_action_id_seq;

do $$
declare
    next_action_id bigint;
begin
    select greatest(coalesce(max(action_id), 1000), 1000) + 1
    into next_action_id
    from audits;

    perform setval('audits_action_id_seq', next_action_id, false);
end $$;

alter table if exists audits
    alter column action_id set default nextval('audits_action_id_seq');

alter sequence if exists audits_action_id_seq
    owned by audits.action_id;

create sequence if not exists audit_deltas_audit_delta_id_seq;

do $$
declare
    next_audit_delta_id bigint;
begin
    select coalesce(max(audit_delta_id), 0) + 1
    into next_audit_delta_id
    from audit_deltas;

    perform setval('audit_deltas_audit_delta_id_seq', next_audit_delta_id, false);
end $$;

alter table if exists audit_deltas
    alter column audit_delta_id set default nextval('audit_deltas_audit_delta_id_seq');

alter sequence if exists audit_deltas_audit_delta_id_seq
    owned by audit_deltas.audit_delta_id;
