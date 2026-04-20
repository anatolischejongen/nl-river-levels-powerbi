-- =============================================================================
-- Migration 02: Add grootheid column to fact_measurements
-- =============================================================================
-- Purpose: Allow fact_measurements to store multiple measurement types
--          (WATHTE, DEBIET, STROOMV) in a single table, distinguished by
--          the grootheid column.
--
-- Impact on existing data: None. All 2.2M existing rows receive the default
--          value 'WATHTE' automatically.
--
-- Run in: Supabase SQL Editor (once)
-- Safe to re-run: No — ALTER TABLE will fail if the column already exists.
--          Check first: SELECT column_name FROM information_schema.columns
--          WHERE table_name = 'fact_measurements' AND column_name = 'grootheid';
-- =============================================================================

-- Step 1: Add the grootheid column.
-- DEFAULT 'WATHTE' backfills all existing rows instantly (no table rewrite).
ALTER TABLE fact_measurements
    ADD COLUMN grootheid TEXT NOT NULL DEFAULT 'WATHTE';


-- Step 2: Drop the old UNIQUE constraint.
-- The old constraint is on (station_id, measured_at, proces_type).
-- After adding grootheid, the same station+timestamp+proces_type combination
-- can legitimately appear for both WATHTE and DEBIET — the old constraint
-- would reject that as a duplicate.
--
-- Note: constraint name may differ on your instance. Verify with:
--   SELECT conname FROM pg_constraint WHERE conrelid = 'fact_measurements'::regclass;
ALTER TABLE fact_measurements
    DROP CONSTRAINT fact_measurements_station_id_measured_at_proces_type_key;


-- Step 3: Add the new UNIQUE constraint that includes grootheid.
ALTER TABLE fact_measurements
    ADD CONSTRAINT fact_measurements_unique
    UNIQUE (station_id, measured_at, proces_type, grootheid);


-- =============================================================================
-- Verify the migration
-- =============================================================================
-- Run these queries after the migration to confirm:

-- 1. Column exists and all rows are tagged WATHTE:
--    SELECT grootheid, COUNT(*) FROM fact_measurements GROUP BY grootheid;
--    Expected: WATHTE | 2258144

-- 2. New constraint is in place:
--    SELECT conname FROM pg_constraint
--    WHERE conrelid = 'fact_measurements'::regclass AND contype = 'u';
--    Expected: fact_measurements_unique
-- =============================================================================
