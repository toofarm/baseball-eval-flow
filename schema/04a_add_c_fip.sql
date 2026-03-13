-- Migration: add c_fip to dim_stat_constants for FIP calculation
-- Run if dim_stat_constants was created before c_fip was added.
-- After this, run `dbt seed` to populate with full constants including c_fip.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'dim_stat_constants' AND column_name = 'c_fip'
    ) THEN
        ALTER TABLE dim_stat_constants ADD COLUMN c_fip NUMERIC;
    END IF;
END $$;
