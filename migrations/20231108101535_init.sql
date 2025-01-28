-- Add migration script here
CREATE TABLE cache (
    key VARCHAR(255) PRIMARY KEY,
    size BIGINT NOT NULL,
    date DATETIME,
    last_access DATETIME,
    times_accessed INT DEFAULT 0,
    expiration DATETIME,
    importance BIGINT NOT NULL DEFAULT 0,
    content_type VARCHAR(255) NOT NULL DEFAULT 'application/octet-stream',
    -- storage tier - will implement post-mvp
    tier varchar(255) NOT NULL DEFAULT 'base'
);
