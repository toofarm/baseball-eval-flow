{{
    config(
        materialized='view',
    )
}}

-- App-facing denormalization of the predictions table. Adds player_name from
-- dim_player. Grain unchanged: (game_pk, player_id, model_type).
--
-- predictions is loaded by the ml_predictions_pipeline DAG via Python, not built
-- by dbt — see {{ source('ml', 'predictions') }}.

select
    pr.game_pk,
    pr.player_id,
    p.full_name as player_name,
    pr.model_type,
    pr.as_of_date,
    pr.pred_bat_woba,
    pr.pred_pit_fip,
    pr.model_version_bat,
    pr.model_version_pit,
    pr.created_at
from {{ source('ml', 'predictions') }} pr
left join {{ ref('dim_player') }} p on p.player_id = pr.player_id
