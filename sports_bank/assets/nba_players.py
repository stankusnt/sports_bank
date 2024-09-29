# Global imports
import requests
import pandas as pd
from pandas import DataFrame
import requests 
import time
from requests.adapters import HTTPAdapter, Retry
from . import constants
from ..resources import snowflake
# Imports for connectors
from nba_api.stats.static import players
from nba_api.stats.endpoints import playercareerstats
from snowflake.connector.pandas_tools import write_pandas
# Imports for tool
from dagster import asset, MaterializeResult, AssetSpec, multi_asset
from dagster_snowflake import SnowflakeResource
import snowflake.connector.errors

## TODO: Check to see if processing players one by one to Snowflake may be easier.

def call_nba_api(dataset: str) -> DataFrame:
    """
        Call nba API for a given dataset.
    """
    # Retrieve list of of all players, each attached to a dictionary

    nba_players = players.get_players()
    
    full_dataset = []
    for player in nba_players:
        if player['is_active'] == True:
            print(f"Loading data for player: {player['id']}")
            try:
                career_stats = playercareerstats.PlayerCareerStats(player_id=player['id'])
                career_dict = career_stats.get_normalized_dict()
                full_dataset += career_dict[f'{dataset}']                 
            except Exception as e:
                print(f"Failed API call at {player['id']} with the following exception: {e}")
            finally:
                # Set time to sleep to prevent rate limits
                time.sleep(30)
    
    return full_dataset


def load_to_snowflake(nba_dataframe: DataFrame, table_name: str, database_name: str, schema: str, database: SnowflakeResource):
    try: 
        with database.get_connection() as conn:
            success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            nba_players_stats_reg_season_df,
            table_name=table_name,
            database=database_name,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
            )
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Failed to write to database because of empty dataframe with the following exception: {e}")
        raise
    except Exception as e:
        print(f"Failed to write to database for with the following exception: {e}")
        raise
    return MaterializeResult(
        metadata={"rows_inserted": rows_inserted},
    )

@asset
def player_stats_reg_season(database: SnowflakeResource):
    """
        Snowflake table for regular season stats for each player.
    """

    nba_player_stats_reg_season = call_nba_api(dataset="SeasonTotalsRegularSeason")

    nba_players_stats_reg_season_df = pd.DataFrame.from_dict(nba_player_stats_reg_season)

    return load_to_snowflake(nba_players_stats_reg_season_df, table_name="player_stats_reg_season", database_name="DB_SPORTSBANK_DEV", schema="NBA")

@asset
def player_stats_post_season(database: SnowflakeResource):
    """
        Snowflake table for post season stats for each player.
    """

    nba_player_stats_post_season = call_nba_api(dataset="SeasonTotalsPostSeason")

    nba_player_stats_post_season_df = pd.DataFrame.from_dict(nba_player_stats_post_season)

    return load_to_snowflake(nba_player_stats_post_season_df, table_name="player_stats_post_season", database_name="DB_SPORTSBANK_DEV", schema="NBA")


@asset
def player_stats_all_star_season(database: SnowflakeResource):
    """
        Snowflake table for all star season stats for each player.
    """

    nba_player_stats_all_star_season = call_nba_api(dataset="SeasonTotalsAllStarSeason")

    nba_player_stats_all_star_season_df = pd.DataFrame.from_dict(nba_player_stats_all_star_season)

    return load_to_snowflake(career_totals_all_star_season_df, table_name="player_stats_all_star_season", database_name="DB_SPORTSBANK_DEV", schema="NBA")


@asset
def player_stats_college_season(database: SnowflakeResource):
    """
        Snowflake table for college season stats for each player.
    """

    nba_player_stats_college_season = call_nba_api(dataset="SeasonTotalsCollegeSeason")

    nba_player_stats_college_season_df = pd.DataFrame.from_dict(nba_player_stats_college_season)

    return load_to_snowflake(career_totals_all_star_season_df, table_name="player_stats_college_season", database_name="DB_SPORTSBANK_DEV", schema="NBA")


@asset
def player_info(database: SnowflakeResource):
    """
        Pandas dataframe for player information.
    """
    nba_players = players.get_players()
    nba_players_df = pd.DataFrame.from_dict(nba_players)

    with database.get_connection() as conn:
        table_name = "players_file"
        database = "DB_SPORTSBANK_DEV"
        schema = "NBA"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            nba_players_df,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
    return MaterializeResult(
        metadata={"rows_inserted": rows_inserted},
    )