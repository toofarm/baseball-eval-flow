-- Migration: add <column_name> to staging_player_stats
-- Run after 00_staging.sql. Idempotent: safe to run on fresh or already-migrated DBs.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'staging_schedule' AND column_name = 'winning_team_id'
    ) THEN
        ALTER TABLE staging_schedule ADD COLUMN winning_team_id INTEGER;
    END IF;
END $$;