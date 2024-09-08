import base64
import os
import sys
import sqlite3
import pandas as pd
import requests
from dagster import AssetExecutionContext, MetadataValue, asset
from wordcloud import STOPWORDS, WordCloud


@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_common_player_info(
    context: AssetExecutionContext, hackernews_topstory_ids: pd.DataFrame
) -> pd.DataFrame:
    """Get items based on story ids from the HackerNews items endpoint. It may take 1-2 minutes to fetch all 500 items.

    API Docs: https://github.com/HackerNews/API#items
    """
    results = []
    for item_id in hackernews_topstory_ids["item_ids"]:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)
        if len(results) % 20 == 0:
            context.log.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)

    # Rename the column to avoid conflict with the reserved keyword "by"
    df.rename(columns={"by": "by_"}, inplace=True)

    # Dagster supports attaching arbitrary metadata to asset materializations. This metadata will be
    # shown in the run logs and also be displayed on the "Activity" tab of the "Asset Details" page in the UI.
    # This metadata would be useful for monitoring and maintaining the asset as you iterate.
    # Read more about in asset metadata in https://docs.dagster.io/concepts/metadata-tags/asset-metadata
    context.add_output_metadata(
        {
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )
    return df


@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_draft_combine_stats(
    context: AssetExecutionContext, hackernews_topstory_ids: pd.DataFrame
) -> pd.DataFrame:

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_draft_history(
    context: AssetExecutionContext, hackernews_topstory_ids: pd.DataFrame
) -> pd.DataFrame:

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_game(
    context: AssetExecutionContext, hackernews_topstory_ids: pd.DataFrame
) -> pd.DataFrame:

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_game_info(
    context: AssetExecutionContext, hackernews_topstory_ids: pd.DataFrame
) -> pd.DataFrame:

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_game_summary(
    context: AssetExecutionContext, hackernews_topstory_ids: pd.DataFrame
) -> pd.DataFrame:

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_inactive_players(
    context: AssetExecutionContext, hackernews_topstory_ids: pd.DataFrame
) -> pd.DataFrame:

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_line_score(
    context: AssetExecutionContext, hackernews_topstory_ids: pd.DataFrame
) -> pd.DataFrame:

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_officials(
    context: AssetExecutionContext, hackernews_topstory_ids: pd.DataFrame
) -> pd.DataFrame:

@asset(group_name="nba", compute_kind="NBA SQLLite")
def nba_other_stats(
    context: AssetExecutionContext, hackernews_topstory_ids: pd.DataFrame
) -> pd.DataFrame: