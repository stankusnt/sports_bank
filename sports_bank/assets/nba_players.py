# Global imports
import requests
import pandas as pd
from pandas import DataFrame
import requests 
from requests.adapters import HTTPAdapter, Retry
from . import constants
from ..resources import snowflake
# Imports for connectors
from nba_api.stats.static import players
from nba_api.stats.endpoints import playercareerstats
from snowflake.connector.pandas_tools import write_pandas
# Imports for tool
from dagster import asset, MaterializeResult
from dagster_snowflake import SnowflakeResource


## TODO
def call_nba_api(player, max_retries=5, backoff_factor=1):
        #requests.get(f'https://stats.nba.com/stats/playercareerstats?LeagueID=&PerMode=PerGame&PlayerID{player['id']}', 
        #career_stats = playercareerstats.PlayerCareerStats(player_id=player['id'])
        session = requests.Session()
        retries = Retry(total=max_retries, backoff_factor=backoff_factor)
        session.mount('https://stats.nba.com/stats', HTTPAdapter(max_retries=retries))
        try:
            response = session.get(f'https://stats.nba.com/stats/playercareerstats?LeagueID=&PerMode=PerGame&PlayerID={player['id']}')
        except RetryError as e:
            print(f'Error: {e}')
        return response

@asset
def player_stats_reg_season_file(database: SnowflakeResource):
    """
        Pandas dataframe for regular season stats for each player.
    """
    # Retrieve list of of all players, each attached to a dictionary
    nba_players = players.get_players()

    # Loop through each player and create dataframe
    player_stats_reg_season = []
    for player in nba_players:
        if player['IsActive'] == 'TRUE':
            print(f"Loading data for player: {player['id']}")
            try:
                career_stats = playercareerstats.PlayerCareerStats(player_id=player['id'], timeout=30)
                career_dict = career_stats.get_normalized_dict()
                reg_season_totals = career_dict['SeasonTotalsRegularSeason']
                player_stats_reg_season += reg_season_totals
            except Exception as e:
                print(f"Failed API call at {player['id']} with the following exception: {e}")
            finally:
                # Set time to sleep to prevent rate limits
                time.sleep(30)
    player_stats_reg_season_df = pd.DataFrame.from_dict(player_stats_reg_season)

    with database.get_connection() as conn:
        table_name = "players_stats_reg_season"
        database = "DB_SPORTSBANK_DEV"
        schema = "NBA"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            player_stats_reg_season_df,
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

@asset
def players_file(database: SnowflakeResource):
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