CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS silver.match_events (
    match_id         TEXT NOT NULL,
    date             TIMESTAMPTZ NOT NULL,
    home_team        TEXT,
    away_team        TEXT,
    home_score       INTEGER,
    away_score       INTEGER,
    match_year       INTEGER,
    match_month      INTEGER,
    day_of_week      TEXT,
    total_goals      INTEGER,
    goal_difference  INTEGER,
    outcome          TEXT
);


SELECT create_hypertable('silver.match_events', 'date', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_match_events_match_id ON silver.match_events (match_id, date DESC);

CREATE TABLE IF NOT EXISTS gold.match_summary (
    team                 TEXT PRIMARY KEY,
    matches_played       INTEGER,
    total_wins           INTEGER,
    total_losses         INTEGER,
    total_draws          INTEGER,
    total_points         INTEGER,
    avg_match_goals      DOUBLE PRECISION,
    avg_goal_difference  DOUBLE PRECISION
);


CREATE ROLE grafana_reader LOGIN PASSWORD 'change_me';
GRANT USAGE ON SCHEMA silver TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO grafana_reader;

CREATE ROLE powerbi_reader LOGIN PASSWORD 'change_me';
GRANT USAGE ON SCHEMA gold TO powerbi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO powerbi_reader;
