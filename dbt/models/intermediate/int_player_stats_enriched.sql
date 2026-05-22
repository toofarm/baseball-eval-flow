{{
    config(
        materialized='view',
    )
}}

with schedule as (
    select game_pk, game_date, season from {{ ref('stg_schedule') }}
),
constants as (
    select * from {{ ref('dim_stat_constants') }}
),
player_stats as (
    select
        p.game_pk,
        p.player_id,
        p.team_id,
        p.position_code,
        p.position_name,
        s.season,
        p.batting,
        p.pitching,
        p.fielding
    from {{ ref('stg_player_stats') }} p
    join schedule s on p.game_pk = s.game_pk
),
-- Parse innings pitched: MLB uses "5.1" = 5 + 1/3 innings
parsed as (
    select
        *,
        -- Batting raw
        {{ json_get('batting', 'gamesPlayed') }}::int as bat_gp,
        {{ json_get('batting', 'runs') }}::int as bat_runs,
        {{ json_get('batting', 'hits') }}::int as bat_hits,
        {{ json_get('batting', 'doubles') }}::int as bat_2b,
        {{ json_get('batting', 'triples') }}::int as bat_3b,
        {{ json_get('batting', 'homeRuns') }}::int as bat_hr,
        {{ json_get('batting', 'strikeOuts') }}::int as bat_k,
        {{ json_get('batting', 'baseOnBalls') }}::int as bat_bb,
        {{ json_get('batting', 'atBats') }}::int as bat_ab,
        {{ json_get('batting', 'plateAppearances') }}::int as bat_pa,
        {{ json_get('batting', 'rbi') }}::int as bat_rbi,
        {{ json_get('batting', 'stolenBases') }}::int as bat_sb,
        {{ json_get('batting', 'caughtStealing') }}::int as bat_cs,
        {{ json_get('batting', 'intentionalWalks') }}::int as bat_ibb,
        {{ json_get('batting', 'sacFlies') }}::int as bat_sf,
        {{ json_get('batting', 'hitByPitch') }}::int as bat_hbp,
        {{ json_get('batting', 'flyOuts') }}::int as bat_fo,
        {{ json_get('batting', 'groundOuts') }}::int as bat_go,
        {{ json_get('batting', 'airOuts') }}::int as bat_ao,
        {{ json_get('batting', 'groundIntoDoublePlay') }}::int as bat_gidp,
        {{ json_get('batting', 'totalBases') }}::int as bat_tb,
        {{ json_get('batting', 'leftOnBase') }}::int as bat_lob,
        {{ json_get('batting', 'sacBunts') }}::int as bat_sac_b,
        -- Pitching raw
        {{ json_get('pitching', 'gamesPlayed') }}::int as pit_gp,
        {{ json_get('pitching', 'gamesStarted') }}::int as pit_gs,
        {{ json_get('pitching', 'inningsPitched') }} as pit_ip_raw,
        {{ json_get('pitching', 'wins') }}::int as pit_w,
        {{ json_get('pitching', 'losses') }}::int as pit_l,
        {{ json_get('pitching', 'saves') }}::int as pit_sv,
        {{ json_get('pitching', 'hits') }}::int as pit_h,
        {{ json_get('pitching', 'earnedRuns') }}::int as pit_er,
        {{ json_get('pitching', 'strikeOuts') }}::int as pit_k,
        {{ json_get('pitching', 'baseOnBalls') }}::int as pit_bb,
        {{ json_get('pitching', 'hitBatsmen') }}::int as pit_hbp,
        {{ json_get('pitching', 'homeRuns') }}::int as pit_hr,
        {{ json_get('pitching', 'atBats') }}::int as pit_ab,
        {{ json_get('pitching', 'flyOuts') }}::int as pit_fo,
        {{ json_get('pitching', 'sacFlies') }}::int as pit_sf,
        {{ json_get('pitching', 'battersFaced') }}::int as pit_bf,
        {{ json_get('pitching', 'outs') }}::int as pit_outs,
        {{ json_get('pitching', 'holds') }}::int as pit_holds,
        {{ json_get('pitching', 'blownSaves') }}::int as pit_bs,
        {{ json_get('pitching', 'saveOpportunities') }}::int as pit_svo,
        {{ json_get('pitching', 'pitchesThrown') }}::int as pit_pitches,
        {{ json_get('pitching', 'balls') }}::int as pit_balls,
        {{ json_get('pitching', 'strikes') }}::int as pit_strikes,
        {{ json_get('pitching', 'balks') }}::int as pit_balks,
        {{ json_get('pitching', 'wildPitches') }}::int as pit_wp,
        {{ json_get('pitching', 'pickoffs') }}::int as pit_pk,
        {{ json_get('pitching', 'inheritedRunners') }}::int as pit_ir,
        {{ json_get('pitching', 'inheritedRunnersScored') }}::int as pit_irs,
        -- Fielding raw
        {{ json_get('fielding', 'assists') }}::int as fld_a,
        {{ json_get('fielding', 'putOuts') }}::int as fld_po,
        {{ json_get('fielding', 'errors') }}::int as fld_e,
        {{ json_get('fielding', 'chances') }}::int as fld_ch,
        {{ json_get('fielding', 'passedBall') }}::int as fld_pb,
        {{ json_get('fielding', 'pickoffs') }}::int as fld_pk
    from player_stats
),
with_ip as (
    select
        *,
        case
            when pit_ip_raw is null then null
            when {{ regex_match('pit_ip_raw', '^[0-9]+[.][0-9]+$') }} then
                floor((pit_ip_raw::numeric)) + (pit_ip_raw::numeric - floor(pit_ip_raw::numeric)) * 10 / 3
            else (pit_ip_raw::numeric)
        end as pit_ip
    from parsed
),
-- Closest-season lookup. Pre-rank constants once per distinct player season
-- (Snowflake can't decorrelate a `lateral ... order by ... limit 1` when this view
-- is inlined into a downstream materialization).
seasons_in_use as (
    select distinct season from with_ip
),
ranked_constants as (
    select
        s.season as p_season,
        c.woba,
        c.woba_scale,
        c.w_bb,
        c.w_hbp,
        c.w_1b,
        c.w_2b,
        c.w_3b,
        c.w_hr,
        c.r_per_pa,
        c.c_fip,
        row_number() over (partition by s.season order by abs(c.season - s.season)) as rn
    from seasons_in_use s
    cross join constants c
),
closest_constant as (
    select * from ranked_constants where rn = 1
),
with_constants as (
    select
        p.*,
        c.woba as c_woba,
        c.woba_scale as c_woba_scale,
        c.w_bb as c_w_bb,
        c.w_hbp as c_w_hbp,
        c.w_1b as c_w_1b,
        c.w_2b as c_w_2b,
        c.w_3b as c_w_3b,
        c.w_hr as c_w_hr,
        c.r_per_pa as c_r_per_pa,
        c.c_fip as c_c_fip
    from with_ip p
    left join closest_constant c on c.p_season = p.season
)
select
    game_pk,
    player_id,
    team_id,
    position_code,
    position_name,
    season,
    -- Batting
    bat_gp as bat_games_played,
    bat_runs,
    bat_hits,
    bat_2b as bat_doubles,
    bat_3b as bat_triples,
    bat_hr as bat_home_runs,
    bat_k as bat_strike_outs,
    bat_bb as bat_base_on_balls,
    bat_ab as bat_at_bats,
    bat_pa as bat_plate_appearances,
    bat_rbi,
    bat_sb as bat_stolen_bases,
    bat_cs as bat_caught_stealing,
    bat_ibb as bat_intentional_walks,
    bat_sf as bat_sac_flies,
    bat_hbp as bat_hit_by_pitch,
    bat_fo as bat_fly_outs,
    bat_go as bat_ground_outs,
    bat_ao as bat_air_outs,
    bat_gidp as bat_ground_into_double_play,
    bat_tb as bat_total_bases,
    bat_lob as bat_left_on_base,
    bat_sac_b as bat_sac_bunts,
    -- Batting computed
    case
        when coalesce(bat_ab, 0) + coalesce(bat_bb, 0) - coalesce(bat_ibb, 0) + coalesce(bat_sf, 0) + coalesce(bat_hbp, 0) > 0
        then (c_w_bb * coalesce(bat_bb, 0) + c_w_hbp * coalesce(bat_hbp, 0)
              + c_w_1b * (coalesce(bat_hits, 0) - coalesce(bat_hr, 0) - coalesce(bat_2b, 0) - coalesce(bat_3b, 0))
              + c_w_2b * coalesce(bat_2b, 0) + c_w_3b * coalesce(bat_3b, 0) + c_w_hr * coalesce(bat_hr, 0))
             / (bat_ab + bat_bb - coalesce(bat_ibb, 0) + bat_sf + coalesce(bat_hbp, 0))
        else null
    end::numeric(5, 4) as bat_woba,
    case
        when bat_pa > 0 and (bat_ab + bat_bb - coalesce(bat_ibb, 0) + bat_sf + coalesce(bat_hbp, 0)) > 0
        then round((((c_w_bb * coalesce(bat_bb, 0) + c_w_hbp * coalesce(bat_hbp, 0)
              + c_w_1b * (coalesce(bat_hits, 0) - coalesce(bat_hr, 0) - coalesce(bat_2b, 0) - coalesce(bat_3b, 0))
              + c_w_2b * coalesce(bat_2b, 0) + c_w_3b * coalesce(bat_3b, 0) + c_w_hr * coalesce(bat_hr, 0))
             / (bat_ab + bat_bb - coalesce(bat_ibb, 0) + bat_sf + coalesce(bat_hbp, 0))) - c_woba)
            / c_woba_scale + c_r_per_pa * bat_pa, 2)
        else null
    end as bat_wrc_plus,
    case
        when bat_ab > 0 and (bat_ab + bat_bb - coalesce(bat_ibb, 0) + bat_sf + coalesce(bat_hbp, 0)) > 0
        then round((bat_bb + coalesce(bat_hbp, 0) + bat_hits)::numeric / (bat_ab + bat_bb - coalesce(bat_ibb, 0) + bat_sf + coalesce(bat_hbp, 0))
            + bat_tb::numeric / bat_ab, 4)
        else null
    end::numeric(5, 4) as bat_ops,
    case
        when bat_ab > 0 and (bat_ab - bat_k - coalesce(bat_sf, 0) - bat_hr) > 0
        then round((bat_hits - bat_hr)::numeric / (bat_ab - bat_k - coalesce(bat_sf, 0) - bat_hr), 4)
        else null
    end::numeric(5, 4) as bat_babip,
    case
        when coalesce(bat_fo, 0) + coalesce(bat_sf, 0) + bat_hr > 0
        then round(bat_hr::numeric / (bat_fo + bat_sf + bat_hr), 4)
        else null
    end::numeric(8, 4) as bat_home_run_rate,
    -- Pitching
    pit_gp as pit_games_played,
    pit_gs as pit_games_started,
    pit_ip::numeric(4, 2) as pit_innings_pitched,
    pit_w as pit_wins,
    pit_l as pit_losses,
    pit_sv as pit_saves,
    pit_h as pit_hits,
    pit_er as pit_earned_runs,
    pit_k as pit_strike_outs,
    pit_bb as pit_base_on_balls,
    pit_bf as pit_batters_faced,
    pit_outs,
    pit_holds,
    pit_bs as pit_blown_saves,
    pit_svo as pit_save_opportunities,
    pit_pitches as pit_pitches_thrown,
    pit_balls,
    pit_strikes,
    pit_hbp as pit_hit_batsmen,
    pit_balks,
    pit_wp as pit_wild_pitches,
    pit_pk as pit_pickoffs,
    pit_ir as pit_inherited_runners,
    pit_irs as pit_inherited_runners_scored,
    -- Pitching computed
    case
        when pit_ip > 0 then round(((13 * pit_hr) + (3 * (pit_bb + coalesce(pit_hbp, 0))) - (2 * pit_k)) / pit_ip + c_c_fip, 2)
        else null
    end::numeric(5, 2) as pit_fip,
    case
        when coalesce(pit_ab, 0) > 0 and (pit_ab - pit_k - coalesce(pit_sf, 0) - pit_hr) > 0
        then round((pit_h - pit_hr)::numeric / (pit_ab - pit_k - coalesce(pit_sf, 0) - pit_hr), 4)
        else null
    end::numeric(5, 4) as pit_babip,
    case
        when coalesce(pit_fo, 0) + coalesce(pit_sf, 0) + pit_hr > 0
        then round((pit_hr::numeric / (pit_fo + coalesce(pit_sf, 0) + pit_hr)) * 100, 4)
        else null
    end::numeric(8, 4) as pit_home_run_rate,
    -- Fielding
    fld_a as fld_assists,
    fld_po as fld_put_outs,
    fld_e as fld_errors,
    fld_ch as fld_chances,
    case
        when fld_ch > 0 then round((fld_a + fld_e)::numeric / fld_ch, 4)
        else null
    end::numeric(6, 4) as fld_fielding_runs,
    fld_pb as fld_passed_ball,
    fld_pk as fld_pickoffs
from with_constants
