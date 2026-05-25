{{
    config(
        materialized='view',
    )
}}

-- Flatten staging_play_by_play.all_plays (JSON array) into one row per pitch.
-- The MLB API reports balls/strikes/outs as the count AFTER each pitch; we emit
-- those as post_balls/post_strikes/post_outs here and let fact_pitch derive the
-- pre-pitch counts via window functions.
--
-- JSON-array-of-array flattening differs enough between Snowflake (LATERAL
-- FLATTEN) and Postgres (jsonb_array_elements) that we branch on dialect rather
-- than try to abstract through a cross_adapter macro.

{% if target.type == 'snowflake' %}

select
    s.game_pk,
    play.value:atBatIndex::integer                                       as at_bat_index,
    event.value:pitchNumber::integer                                     as pitch_number,
    play.value:matchup:pitcher:id::integer                               as pitcher_id,
    play.value:matchup:batter:id::integer                                as batter_id,
    play.value:matchup:batSide:code::varchar                             as bat_side,
    play.value:matchup:pitchHand:code::varchar                           as pitch_hand,
    play.value:about:inning::integer                                     as inning,
    play.value:about:halfInning::varchar                                 as half_inning,
    play.value:result:event::varchar                                     as at_bat_event,
    event.value:details:type:code::varchar                               as pitch_type_code,
    event.value:details:call:code::varchar                               as call_code,
    event.value:details:isStrike::boolean                                as is_strike,
    event.value:details:isBall::boolean                                  as is_ball,
    event.value:details:isInPlay::boolean                                as is_in_play,
    event.value:pitchData:startSpeed::numeric(5, 2)                      as start_speed,
    event.value:pitchData:endSpeed::numeric(5, 2)                        as end_speed,
    event.value:pitchData:zone::integer                                  as zone,
    event.value:pitchData:coordinates:pX::numeric(6, 3)                  as plate_x,
    event.value:pitchData:coordinates:pZ::numeric(6, 3)                  as plate_z,
    event.value:pitchData:breaks:spinRate::integer                       as spin_rate,
    event.value:pitchData:breaks:spinDirection::integer                  as spin_direction,
    event.value:pitchData:breaks:breakAngle::numeric(6, 2)               as break_angle,
    event.value:pitchData:breaks:breakLength::numeric(6, 2)              as break_length,
    event.value:pitchData:breaks:breakVertical::numeric(6, 2)            as break_vertical,
    event.value:pitchData:breaks:breakVerticalInduced::numeric(6, 2)     as break_vertical_induced,
    event.value:pitchData:breaks:breakHorizontal::numeric(6, 2)          as break_horizontal,
    event.value:count:balls::integer                                     as post_balls,
    event.value:count:strikes::integer                                   as post_strikes,
    event.value:count:outs::integer                                      as post_outs
from {{ source('mlb', 'staging_play_by_play') }} s,
    lateral flatten(input => s.all_plays) play,
    lateral flatten(input => play.value:playEvents) event
where event.value:isPitch::boolean = true

{% else %}

select
    s.game_pk,
    (play->>'atBatIndex')::integer                                                 as at_bat_index,
    (event->>'pitchNumber')::integer                                               as pitch_number,
    ((play->'matchup'->'pitcher')->>'id')::integer                                 as pitcher_id,
    ((play->'matchup'->'batter')->>'id')::integer                                  as batter_id,
    (play->'matchup'->'batSide'->>'code')                                          as bat_side,
    (play->'matchup'->'pitchHand'->>'code')                                        as pitch_hand,
    (play->'about'->>'inning')::integer                                            as inning,
    (play->'about'->>'halfInning')                                                 as half_inning,
    (play->'result'->>'event')                                                     as at_bat_event,
    (event->'details'->'type'->>'code')                                            as pitch_type_code,
    (event->'details'->'call'->>'code')                                            as call_code,
    (event->'details'->>'isStrike')::boolean                                       as is_strike,
    (event->'details'->>'isBall')::boolean                                         as is_ball,
    (event->'details'->>'isInPlay')::boolean                                       as is_in_play,
    (event->'pitchData'->>'startSpeed')::numeric(5, 2)                             as start_speed,
    (event->'pitchData'->>'endSpeed')::numeric(5, 2)                               as end_speed,
    (event->'pitchData'->>'zone')::integer                                         as zone,
    (event->'pitchData'->'coordinates'->>'pX')::numeric(6, 3)                      as plate_x,
    (event->'pitchData'->'coordinates'->>'pZ')::numeric(6, 3)                      as plate_z,
    (event->'pitchData'->'breaks'->>'spinRate')::integer                           as spin_rate,
    (event->'pitchData'->'breaks'->>'spinDirection')::integer                      as spin_direction,
    (event->'pitchData'->'breaks'->>'breakAngle')::numeric(6, 2)                   as break_angle,
    (event->'pitchData'->'breaks'->>'breakLength')::numeric(6, 2)                  as break_length,
    (event->'pitchData'->'breaks'->>'breakVertical')::numeric(6, 2)                as break_vertical,
    (event->'pitchData'->'breaks'->>'breakVerticalInduced')::numeric(6, 2)         as break_vertical_induced,
    (event->'pitchData'->'breaks'->>'breakHorizontal')::numeric(6, 2)              as break_horizontal,
    (event->'count'->>'balls')::integer                                            as post_balls,
    (event->'count'->>'strikes')::integer                                          as post_strikes,
    (event->'count'->>'outs')::integer                                             as post_outs
from {{ source('mlb', 'staging_play_by_play') }} s
cross join lateral jsonb_array_elements(s.all_plays) as play
cross join lateral jsonb_array_elements(play->'playEvents') as event
where (event->>'isPitch')::boolean = true

{% endif %}
