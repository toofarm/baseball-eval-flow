
  create view "airflow"."public"."int_player_stats_enriched__dbt_tmp"
    
    
  as (
    

with schedule as (
    select game_pk, game_date, season from "airflow"."public"."stg_schedule"
),
constants as (
    select * from "airflow"."public"."dim_stat_constants"
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
    from "airflow"."public"."stg_player_stats" p
    join schedule s on p.game_pk = s.game_pk
),
-- Parse innings pitched: MLB uses "5.1" = 5 + 1/3 innings
parsed as (
    select
        *,
        -- Batting raw
        (batting->>'gamesPlayed')::int as bat_gp,
        (batting->>'runs')::int as bat_runs,
        (batting->>'hits')::int as bat_hits,
        (batting->>'doubles')::int as bat_2b,
        (batting->>'triples')::int as bat_3b,
        (batting->>'homeRuns')::int as bat_hr,
        (batting->>'strikeOuts')::int as bat_k,
        (batting->>'baseOnBalls')::int as bat_bb,
        (batting->>'atBats')::int as bat_ab,
        (batting->>'plateAppearances')::int as bat_pa,
        (batting->>'rbi')::int as bat_rbi,
        (batting->>'stolenBases')::int as bat_sb,
        (batting->>'caughtStealing')::int as bat_cs,
        (batting->>'intentionalWalks')::int as bat_ibb,
        (batting->>'sacFlies')::int as bat_sf,
        (batting->>'hitByPitch')::int as bat_hbp,
        (batting->>'flyOuts')::int as bat_fo,
        (batting->>'groundOuts')::int as bat_go,
        (batting->>'airOuts')::int as bat_ao,
        (batting->>'groundIntoDoublePlay')::int as bat_gidp,
        (batting->>'totalBases')::int as bat_tb,
        (batting->>'leftOnBase')::int as bat_lob,
        (batting->>'sacBunts')::int as bat_sac_b,
        -- Pitching raw
        (pitching->>'gamesPlayed')::int as pit_gp,
        (pitching->>'gamesStarted')::int as pit_gs,
        (pitching->>'inningsPitched') as pit_ip_raw,
        (pitching->>'wins')::int as pit_w,
        (pitching->>'losses')::int as pit_l,
        (pitching->>'saves')::int as pit_sv,
        (pitching->>'hits')::int as pit_h,
        (pitching->>'earnedRuns')::int as pit_er,
        (pitching->>'strikeOuts')::int as pit_k,
        (pitching->>'baseOnBalls')::int as pit_bb,
        (pitching->>'hitBatsmen')::int as pit_hbp,
        (pitching->>'homeRuns')::int as pit_hr,
        (pitching->>'atBats')::int as pit_ab,
        (pitching->>'flyOuts')::int as pit_fo,
        (pitching->>'sacFlies')::int as pit_sf,
        (pitching->>'battersFaced')::int as pit_bf,
        (pitching->>'outs')::int as pit_outs,
        (pitching->>'holds')::int as pit_holds,
        (pitching->>'blownSaves')::int as pit_bs,
        (pitching->>'saveOpportunities')::int as pit_svo,
        (pitching->>'pitchesThrown')::int as pit_pitches,
        (pitching->>'balls')::int as pit_balls,
        (pitching->>'strikes')::int as pit_strikes,
        (pitching->>'balks')::int as pit_balks,
        (pitching->>'wildPitches')::int as pit_wp,
        (pitching->>'pickoffs')::int as pit_pk,
        (pitching->>'inheritedRunners')::int as pit_ir,
        (pitching->>'inheritedRunnersScored')::int as pit_irs,
        -- Fielding raw
        (fielding->>'assists')::int as fld_a,
        (fielding->>'putOuts')::int as fld_po,
        (fielding->>'errors')::int as fld_e,
        (fielding->>'chances')::int as fld_ch,
        (fielding->>'passedBall')::int as fld_pb,
        (fielding->>'pickoffs')::int as fld_pk
    from player_stats
),
with_ip as (
    select
        *,
        case
            when pit_ip_raw is null then null
            when pit_ip_raw ~ '^\d+\.\d+$' then
                floor((pit_ip_raw::numeric)) + (pit_ip_raw::numeric - floor(pit_ip_raw::numeric)) * 10 / 3
            else (pit_ip_raw::numeric)
        end as pit_ip
    from parsed
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
    left join lateral (
        select * from constants c0
        order by abs(c0.season - p.season)
        limit 1
    ) c on true
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
  );