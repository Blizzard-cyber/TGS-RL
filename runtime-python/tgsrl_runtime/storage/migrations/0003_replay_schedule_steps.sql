CREATE TABLE replay_schedule_steps (
    replay_id TEXT NOT NULL,
    start_key_digest TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    step_digest TEXT NOT NULL,
    decision_payload BLOB NOT NULL,
    decision_sha256 TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    PRIMARY KEY (replay_id, start_key_digest, ordinal)
);

CREATE INDEX replay_schedule_steps_replay_idx
    ON replay_schedule_steps (replay_id, start_key_digest, ordinal);
