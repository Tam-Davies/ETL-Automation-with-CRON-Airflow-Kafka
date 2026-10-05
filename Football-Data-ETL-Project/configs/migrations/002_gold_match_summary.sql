CREATE SCHEMA IF NOT EXISTS gold;

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

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'gold' AND table_name = 'match_summary'
          AND column_name = 'home_team'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'gold' AND table_name = 'match_summary'
          AND column_name = 'team'
    ) THEN
        ALTER TABLE gold.match_summary RENAME COLUMN home_team TO team;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'gold' AND table_name = 'match_summary'
          AND column_name = 'home_wins'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'gold' AND table_name = 'match_summary'
          AND column_name = 'total_wins'
    ) THEN
        ALTER TABLE gold.match_summary RENAME COLUMN home_wins TO total_wins;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'gold' AND table_name = 'match_summary'
          AND column_name = 'home_losses'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'gold' AND table_name = 'match_summary'
          AND column_name = 'total_losses'
    ) THEN
        ALTER TABLE gold.match_summary RENAME COLUMN home_losses TO total_losses;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'gold' AND table_name = 'match_summary'
          AND column_name = 'draws'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'gold' AND table_name = 'match_summary'
          AND column_name = 'total_draws'
    ) THEN
        ALTER TABLE gold.match_summary RENAME COLUMN draws TO total_draws;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'gold' AND table_name = 'match_summary'
          AND column_name = 'avg_total_goals'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'gold' AND table_name = 'match_summary'
          AND column_name = 'avg_match_goals'
    ) THEN
        ALTER TABLE gold.match_summary
            RENAME COLUMN avg_total_goals TO avg_match_goals;
    END IF;
END
$$;

ALTER TABLE gold.match_summary
    ADD COLUMN IF NOT EXISTS total_points INTEGER;

UPDATE gold.match_summary
SET total_points = COALESCE(total_wins, 0) * 3 + COALESCE(total_draws, 0)
WHERE total_points IS NULL;
