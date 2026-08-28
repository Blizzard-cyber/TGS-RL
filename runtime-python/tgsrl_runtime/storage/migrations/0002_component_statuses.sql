CREATE TABLE component_statuses (
    component_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    component TEXT NOT NULL,
    revision INTEGER NOT NULL,
    health INTEGER NOT NULL,
    payload BLOB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (run_id, component)
);

CREATE INDEX component_statuses_page_idx
    ON component_statuses (run_id, component_seq, component);
