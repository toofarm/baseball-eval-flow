-- pitcher_pitch_arsenal contract: usage_pct sums to 100 per (pitcher, season)
-- (modulo small rounding error from the 2-decimal-place numeric cast).
-- Fails if any pitcher's per-season usage_pct sum drifts more than 0.05 from 100.

select
    pitcher_id,
    season,
    sum(usage_pct) as total_usage_pct
from {{ ref('pitcher_pitch_arsenal') }}
group by pitcher_id, season
having abs(sum(usage_pct) - 100) > 0.05
