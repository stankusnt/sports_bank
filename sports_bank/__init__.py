# fmt: off
from dagster import Definitions, load_assets_from_modules, In
from dagster_snowflake import SnowflakeResource

from .assets import metrics, nba_players
from .resources import snowflake

nba_assets = load_assets_from_modules([nba_players])
metric_assets = load_assets_from_modules([metrics])

defs = Definitions(
    assets=[*nba_assets, *metric_assets],
    resources={
        "database": snowflake,
    },
)
