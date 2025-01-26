-- Add migration script here

CREATE TABLE cache (
    key VARCHAR(255) PRIMARY KEY,
    size INT NOT NULL,
    date DATETIME,
    last_access DATETIME,
    times_accessed INT DEFAULT 0,
    importance INT
);
