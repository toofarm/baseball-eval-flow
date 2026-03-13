-- Migration: add model_type to predictions for storing Ridge and HGB predictions side-by-side.
-- Run after 05_predictions.sql. Idempotent: safe to run on fresh or already-migrated DBs.
-- PK becomes (game_pk, player_id, model_type); existing rows get model_type='ridge'.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'predictions' AND column_name = 'model_type'
    ) THEN
        ALTER TABLE predictions ADD COLUMN model_type VARCHAR(16) NOT NULL DEFAULT 'ridge';
        ALTER TABLE predictions DROP CONSTRAINT IF EXISTS predictions_pkey;
        ALTER TABLE predictions ADD PRIMARY KEY (game_pk, player_id, model_type);
    END IF;
END $$;
