import base64
import os
import sys
import sqlite3
import pandas as pd
import requests
from dagster import AssetExecutionContext, MetadataValue, asset


@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_common_player_info(
    context: AssetExecutionContext
) -> pd.DataFrame:
    """ 
    """
    nba_db_location = "/Users/stankusnt/nba-project/nba.sqlite"
    # Establish Sqlite connection 
    nba_db_con = sqlite3.connect(f"{nba_db_location}")
    # Create a cursor
    cur = nba_db_con.cursor()
    # Retrieve table data
    table_name = cur.execute(f"SELECT * FROM common_player_info").fetchall()
    # Retrieve table schema
    table_metadata = cur.execute(f"PRAGMA table_info('common_player_info')").fetchall()
    table_columns = [record[1] for record in table_metadata]
    # Manifest dataframe
    table_df = pd.DataFrame(table_name, columns=table_columns)

    # Dagster supports attaching arbitrary metadata to asset materializations. This metadata will be
    # shown in the run logs and also be displayed on the "Activity" tab of the "Asset Details" page in the UI.
    # This metadata would be useful for monitoring and maintaining the asset as you iterate.
    # Read more about in asset metadata in https://docs.dagster.io/concepts/metadata-tags/asset-metadata
    context.add_output_metadata(
        {
            "num_records": len(table_df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )
    return table_df 

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_draft_combine_stats(
    context: AssetExecutionContext
) -> pd.DataFrame:
    return None

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_draft_history(
    context: AssetExecutionContext
) -> pd.DataFrame:
    return None

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_game(
    context: AssetExecutionContext
) -> pd.DataFrame:
    return None

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_game_info(
    context: AssetExecutionContext
) -> pd.DataFrame:
    return None

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_game_summary(
    context: AssetExecutionContext
) -> pd.DataFrame:
    return None

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_inactive_players(
    context: AssetExecutionContext
) -> pd.DataFrame:
    return None

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_line_score(
    context: AssetExecutionContext
) -> pd.DataFrame:
    return None

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_officials(
    context: AssetExecutionContext
) -> pd.DataFrame:
    return None

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_other_stats(
    context: AssetExecutionContext
) -> pd.DataFrame:
    return None