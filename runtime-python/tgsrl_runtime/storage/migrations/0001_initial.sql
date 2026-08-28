CREATE TABLE intents (
    execution_id TEXT NOT NULL,
    stage_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    job_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (execution_id, stage_id, version),
    UNIQUE (idempotency_key)
);

CREATE INDEX intents_latest_idx
    ON intents (execution_id, stage_id, version DESC);

CREATE TABLE trace_events (
    event_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX trace_events_run_page_idx
    ON trace_events (run_id, sequence, event_id);

CREATE INDEX trace_events_execution_page_idx
    ON trace_events (execution_id, sequence, event_id);

CREATE TABLE replays (
    replay_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    replay_id TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    state INTEGER NOT NULL,
    payload BLOB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX replays_page_idx
    ON replays (replay_seq, replay_id);

CREATE TABLE experiments (
    experiment_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    experiment_id TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    state INTEGER NOT NULL,
    payload BLOB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX experiments_page_idx
    ON experiments (experiment_seq, experiment_id);

CREATE TABLE runtime_manifests (
    run_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE runtime_units (
    run_id TEXT NOT NULL,
    runtime_unit_id TEXT NOT NULL,
    stage_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    state INTEGER NOT NULL,
    payload BLOB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, runtime_unit_id)
);

CREATE INDEX runtime_units_page_idx
    ON runtime_units (run_id, stage_id, runtime_unit_id);

CREATE TABLE sandboxes (
    sandbox_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    state INTEGER NOT NULL,
    payload BLOB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX sandboxes_page_idx
    ON sandboxes (run_id, sandbox_id);

CREATE TABLE runtime_events (
    runtime_event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    sandbox_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    payload BLOB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX runtime_events_run_page_idx
    ON runtime_events (run_id, runtime_event_seq, event_id);

CREATE INDEX runtime_events_job_page_idx
    ON runtime_events (job_id, runtime_event_seq, event_id);

CREATE TABLE runtime_checkpoints (
    checkpoint_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    checkpoint_ref TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (run_id, checkpoint_ref)
);

CREATE INDEX runtime_checkpoints_run_idx
    ON runtime_checkpoints (run_id, checkpoint_seq DESC);

CREATE TABLE idempotency_records (
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    response_payload BLOB NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (scope, key)
);

CREATE TABLE delete_audit (
    audit_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    scope TEXT NOT NULL,
    cutoff TEXT NOT NULL,
    deleted_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT ''
);
