-- 001_init: baseline schema (week_trendings + posts).
-- Idempotent: safe to apply on the DB already created via create_all.
CREATE TABLE IF NOT EXISTS week_trendings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    week_label VARCHAR(16) NOT NULL UNIQUE,
    themes_url TEXT NOT NULL,
    trends_url TEXT NOT NULL,
    dropped_url TEXT,
    source_prefix TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    week_id INTEGER NOT NULL REFERENCES week_trendings(id) ON DELETE CASCADE,
    platform VARCHAR(16) NOT NULL CHECK (platform IN ('facebook', 'instagram', 'linkedin')),
    account_type VARCHAR(16) NOT NULL CHECK (account_type IN ('company', 'personal')),
    headline TEXT NOT NULL,
    body TEXT NOT NULL,
    hashtags TEXT NOT NULL CHECK (json_valid(hashtags) AND json_array_length(hashtags) BETWEEN 10 AND 20),
    model VARCHAR(64),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UNIQUE (week_id, platform, account_type)
);

CREATE INDEX IF NOT EXISTS ix_posts_week_id ON posts (week_id);
