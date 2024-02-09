CREATE TABLE title_basics (
    tconst TEXT PRIMARY KEY,
    title_type TEXT NOT NULL,
    primary_title TEXT NOT NULL,
    original_title TEXT NOT NULL,
    is_adult BOOLEAN NOT NULL,
    start_year INTEGER,
    end_year INTEGER,
    runtime_minutes REAL,
    genres TEXT,
    CONSTRAINT valid_years CHECK (start_year IS NULL OR start_year >= 0),
    CONSTRAINT valid_runtime CHECK (runtime_minutes IS NULL OR runtime_minutes >= 0),
    CONSTRAINT valid_is_adult CHECK (is_adult IN (true, false))
);

CREATE INDEX idx_title_type ON title_basics(title_type);
CREATE INDEX idx_start_year ON title_basics(start_year);