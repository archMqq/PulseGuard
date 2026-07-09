CREATE TABLE IF NOT EXISTS error_event (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    issue_id UUID NOT NULL,
    "level" SMALLINT NOT NULL,
    payload JSONB NOT NULL,
    "timestamp" TIMESTAMP NOT NULL,
    received_at TIMESTAMP NOT NULL

    CONSTRAINT fk_error_issue FOREIGN KEY (issue_id) REFERENCES error_issue(id) ON DELETE CASCADE 
);

CREATE INDEX idx_error_event_timestamp ON error_event ("timestamp", DESC)