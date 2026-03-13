
      -- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into "airflow"."public"."player_rolling_stats" as DBT_INTERNAL_DEST
        using "player_rolling_stats__dbt_tmp203746032517" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.player_id = DBT_INTERNAL_DEST.player_id
                ) and (
                    DBT_INTERNAL_SOURCE.as_of_date = DBT_INTERNAL_DEST.as_of_date
                ) and (
                    DBT_INTERNAL_SOURCE.window_days = DBT_INTERNAL_DEST.window_days
                )

    
    when matched then update set
        "player_id" = DBT_INTERNAL_SOURCE."player_id","as_of_date" = DBT_INTERNAL_SOURCE."as_of_date","window_days" = DBT_INTERNAL_SOURCE."window_days","bat_games_played" = DBT_INTERNAL_SOURCE."bat_games_played","bat_plate_appearances" = DBT_INTERNAL_SOURCE."bat_plate_appearances","bat_at_bats" = DBT_INTERNAL_SOURCE."bat_at_bats","bat_runs" = DBT_INTERNAL_SOURCE."bat_runs","bat_hits" = DBT_INTERNAL_SOURCE."bat_hits","bat_doubles" = DBT_INTERNAL_SOURCE."bat_doubles","bat_triples" = DBT_INTERNAL_SOURCE."bat_triples","bat_home_runs" = DBT_INTERNAL_SOURCE."bat_home_runs","bat_rbi" = DBT_INTERNAL_SOURCE."bat_rbi","bat_strike_outs" = DBT_INTERNAL_SOURCE."bat_strike_outs","bat_base_on_balls" = DBT_INTERNAL_SOURCE."bat_base_on_balls","bat_stolen_bases" = DBT_INTERNAL_SOURCE."bat_stolen_bases","bat_caught_stealing" = DBT_INTERNAL_SOURCE."bat_caught_stealing","bat_avg" = DBT_INTERNAL_SOURCE."bat_avg","bat_ops" = DBT_INTERNAL_SOURCE."bat_ops","bat_woba" = DBT_INTERNAL_SOURCE."bat_woba","bat_wrc_plus" = DBT_INTERNAL_SOURCE."bat_wrc_plus","pit_games_played" = DBT_INTERNAL_SOURCE."pit_games_played","pit_innings_pitched" = DBT_INTERNAL_SOURCE."pit_innings_pitched","pit_wins" = DBT_INTERNAL_SOURCE."pit_wins","pit_losses" = DBT_INTERNAL_SOURCE."pit_losses","pit_saves" = DBT_INTERNAL_SOURCE."pit_saves","pit_hits" = DBT_INTERNAL_SOURCE."pit_hits","pit_earned_runs" = DBT_INTERNAL_SOURCE."pit_earned_runs","pit_strike_outs" = DBT_INTERNAL_SOURCE."pit_strike_outs","pit_base_on_balls" = DBT_INTERNAL_SOURCE."pit_base_on_balls","pit_era" = DBT_INTERNAL_SOURCE."pit_era","pit_fip" = DBT_INTERNAL_SOURCE."pit_fip","pit_whip" = DBT_INTERNAL_SOURCE."pit_whip","fld_assists" = DBT_INTERNAL_SOURCE."fld_assists","fld_put_outs" = DBT_INTERNAL_SOURCE."fld_put_outs","fld_errors" = DBT_INTERNAL_SOURCE."fld_errors","fld_chances" = DBT_INTERNAL_SOURCE."fld_chances"
    

    when not matched then insert
        ("player_id", "as_of_date", "window_days", "bat_games_played", "bat_plate_appearances", "bat_at_bats", "bat_runs", "bat_hits", "bat_doubles", "bat_triples", "bat_home_runs", "bat_rbi", "bat_strike_outs", "bat_base_on_balls", "bat_stolen_bases", "bat_caught_stealing", "bat_avg", "bat_ops", "bat_woba", "bat_wrc_plus", "pit_games_played", "pit_innings_pitched", "pit_wins", "pit_losses", "pit_saves", "pit_hits", "pit_earned_runs", "pit_strike_outs", "pit_base_on_balls", "pit_era", "pit_fip", "pit_whip", "fld_assists", "fld_put_outs", "fld_errors", "fld_chances")
    values
        ("player_id", "as_of_date", "window_days", "bat_games_played", "bat_plate_appearances", "bat_at_bats", "bat_runs", "bat_hits", "bat_doubles", "bat_triples", "bat_home_runs", "bat_rbi", "bat_strike_outs", "bat_base_on_balls", "bat_stolen_bases", "bat_caught_stealing", "bat_avg", "bat_ops", "bat_woba", "bat_wrc_plus", "pit_games_played", "pit_innings_pitched", "pit_wins", "pit_losses", "pit_saves", "pit_hits", "pit_earned_runs", "pit_strike_outs", "pit_base_on_balls", "pit_era", "pit_fip", "pit_whip", "fld_assists", "fld_put_outs", "fld_errors", "fld_chances")


  