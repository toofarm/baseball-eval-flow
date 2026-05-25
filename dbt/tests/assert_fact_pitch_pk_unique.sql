-- fact_pitch grain: one row per (game_pk, at_bat_index, pitch_number).
-- This singular test fails (returns rows) if the grain is ever violated —
-- e.g. an upstream change drops the dedupe in stg_play_by_play.

select game_pk, at_bat_index, pitch_number, count(*) as n
from {{ ref('fact_pitch') }}
group by game_pk, at_bat_index, pitch_number
having count(*) > 1
