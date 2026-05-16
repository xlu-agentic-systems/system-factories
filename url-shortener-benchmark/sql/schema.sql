CREATE TABLE IF NOT EXISTS url_mappings_base62 (
    short_url VARCHAR(8) PRIMARY KEY,
    long_url TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS url_mappings_base36 (
    short_url VARCHAR(8) PRIMARY KEY,
    long_url TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS collision_failures (
    method TEXT NOT NULL,
    long_url TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    last_short_url VARCHAR(8) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

