CREATE TABLE IF NOT EXISTS error_issue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id INT NOT NULL,
    fingerprint BIGINT NOT NULL,
    title TEXT NOT NULL,
    "status" SMALLINT NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX idx_project_fingerprint ON error_issue (project_id, fingerprint);