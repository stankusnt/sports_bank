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


def call_nba_api() -> DataFrame:
    """
        Call nba API for a given dataset.
    """
    # Retrieve list of of all players, each attached to a dictionary
    nba_players = players.get_players()

    # Loop through each player and create dataframe
    CareerTotalsAllStarSeasonDataset = []
    CareerTotalsCollegeSeasonDataset = []
    CareerTotalsRegularSeasonDataset = []
    SeasonRankingsPostSeasonDataset = []
    SeasonRankingsRegularSeasonDataset = []
    SeasonTotalsAllStarSeasonDataset = []
    SeasonTotalsCollegeSeasonDataset = []
    SeasonTotalsPostSeasonDataset = []
    SeasonTotalsRegularSeasonDataset = []
    for player in nba_players:
        if player['is_active'] == True:
            print(f"Loading data for player: {player['id']}")
            try:
                career_stats = playercareerstats.PlayerCareerStats(player_id=player['id'])
                career_dict = career_stats.get_normalized_dict()

                CareerTotalsAllStarSeasonDataset += career_dict['CareerTotalsAllStarSeason']
                CareerTotalsCollegeSeasonDataset += career_dict['CareerTotalsCollegeSeason']
                CareerTotalsRegularSeasonDataset += career_dict['CareerTotalsRegularSeason']
                SeasonRankingsPostSeasonDataset += career_dict['SeasonRankingsPostSeason']    
                SeasonRankingsRegularSeasonDataset += career_dict['SeasonRankingsRegularSeason']
                SeasonTotalsAllStarSeasonDataset += career_dict['SeasonTotalsAllStarSeason']
                SeasonTotalsCollegeSeasonDataset += career_dict['SeasonTotalsCollegeSeason']
                SeasonTotalsPostSeasonDataseet += career_dict['SeasonTotalsPostSeason']    
                SeasonTotalsRegularSeasonDataset += career_dict['SeasonTotalsRegularSeason']                  
            except Exception as e:
                print(f"Failed API call at {player['id']} with the following exception: {e}")
            finally:
                # Set time to sleep to prevent rate limits
                time.sleep(30)
    
    return CareerTotalsAllStarSeasonDataset, CareerTotalsCollegeSeasonDataset, CareerTotalsRegularSeasonDataset, SeasonRankingsPostSeasonDataset, SeasonRankingsRegularSeasonDataset, SeasonTotalsAllStarSeasonDataset, SeasonTotalsCollegeSeasonDataset, SeasonTotalsPostSeasonDataseet, SeasonTotalsRegularSeasonDataset

@multi_asset(
    specs=[
        AssetSpec("CareerTotalsAllStarSeason", skippable=True), 
        AssetSpec("CareerTotalsCollegeSeason", skippable=True),
        AssetSpec("CareerTotalsRegularSeason", skippable=True), 
        AssetSpec("SeasonRankingsPostSeason", skippable=True),
        AssetSpec("SeasonRankingsRegularSeason", skippable=True), 
        AssetSpec("SeasonTotalsAllStarSeason", skippable=True),
        AssetSpec("SeasonTotalsCollegeSeason", skippable=True), 
        AssetSpec("SeasonTotalsPostSeason", skippable=True),
        AssetSpec("SeasonTotalsRegularSeason", skippable=True)     
    ]
)
def player_career_stats():

    CareerTotalsAllStarSeasonDataset, CareerTotalsCollegeSeasonDataset, \
    CareerTotalsRegularSeasonDataset, SeasonRankingsPostSeasonDataset, \
    SeasonRankingsRegularSeasonDataset, SeasonTotalsAllStarSeasonDataset, \
    SeasonTotalsCollegeSeasonDataset, SeasonTotalsPostSeasonDataseet, SeasonTotalsRegularSeasonDataset = call_nba_api()
    
    yield load_to_snowflake(pd.DataFrame.from_dict(CareerTotalsAllStarSeasonDataset))
    yield load_to_snowflake(pd.DataFrame.from_dict(CareerTotalsCollegeSeasonDataset))
    yield load_to_snowflake(pd.DataFrame.from_dict(CareerTotalsRegularSeasonDataset))
    yield load_to_snowflake(pd.DataFrame.from_dict(SeasonRankingsPostSeasonDataset))
    yield load_to_snowflake(pd.DataFrame.from_dict(SeasonRankingsRegularSeasonDataset))
    yield load_to_snowflake(pd.DataFrame.from_dict(SeasonTotalsAllStarSeasonDataset))
    yield load_to_snowflake(pd.DataFrame.from_dict(SeasonTotalsCollegeSeasonDataset))
    yield load_to_snowflake(pd.DataFrame.from_dict(SeasonTotalsPostSeasonDataseet))
    yield load_to_snowflake(pd.DataFrame.from_dict(SeasonTotalsRegularSeasonDataset))


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
        Pandas dataframe for regular season stats for each player.
    """

    nba_player_stats_reg_season_df = call_nba_api_playercareerstats("SeasonTotalsRegularSeason")
    # Retrieve list of of all players, each attached to a dictionary
    nba_players = players.get_players()


    # Loop through each player and create dataframe
    nba_players_stats_reg_season = []
    for player in nba_players:
        if player['is_active'] == True:
            print(f"Loading data for player: {player['id']}")
            try:
                career_stats = playercareerstats.PlayerCareerStats(player_id=player['id'])
                career_dict = career_stats.get_normalized_dict()
                reg_season_totals = career_dict['SeasonTotalsRegularSeason']
                nba_players_stats_reg_season += reg_season_totals
            except Exception as e:
                print(f"Failed API call at {player['id']} with the following exception: {e}")
            finally:
                # Set time to sleep to prevent rate limits
                time.sleep(30)

    nba_players_stats_reg_season_df = pd.DataFrame.from_dict(nba_players_stats_reg_season) 

    return load_to_snowflake(nba_players_stats_reg_season_df, table_name="player_stats_regular_season", database="DB_SPORTSBANK_DEV", schema="NBA")

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