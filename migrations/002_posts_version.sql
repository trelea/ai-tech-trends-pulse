-- 002_posts_version: multiple content generations per week.
-- sqlite cannot drop a UNIQUE constraint, so rebuild the table with the
-- version column and the widened unique key. Existing rows keep version=1.
CREATE TABLE posts_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    week_id INTEGER NOT NULL REFERENCES week_trendings(id) ON DELETE CASCADE,
    platform VARCHAR(16) NOT NULL CHECK (platform IN ('facebook', 'instagram', 'linkedin')),
    account_type VARCHAR(16) NOT NULL CHECK (account_type IN ('company', 'personal')),
    headline TEXT NOT NULL,
    body TEXT NOT NULL,
    hashtags TEXT NOT NULL CHECK (json_valid(hashtags) AND json_array_length(hashtags) BETWEEN 10 AND 20),
    model VARCHAR(64),
    version INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UNIQUE (week_id, platform, account_type, version)
);

INSERT INTO posts_new (id, week_id, platform, account_type, headline, body, hashtags, model, version, created_at, updated_at)
    SELECT id, week_id, platform, account_type, headline, body, hashtags, model, 1, created_at, updated_at FROM posts;

DROP TABLE posts;

ALTER TABLE posts_new RENAME TO posts;

CREATE INDEX ix_posts_week_id ON posts (week_id);
