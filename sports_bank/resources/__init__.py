from dagster_snowflake import SnowflakeResource
from dagster import EnvVar

snowflake = SnowflakeResource(
    account = EnvVar("SNOWFLAKE_ACCOUNT"),
    user = EnvVar("SNOWFLAKE_USER"),
    password = EnvVar("SNOWFLAKE_PASSWORD"),
    warehouse = EnvVar("SNOWFLAKE_WAREHOUSE"),
    database = EnvVar("SNOWFLAKE_DATABASE"),
    schema = EnvVar("SNOWFLAKE_SCHEMA"),
    role = EnvVar("SNOWFLAKE_ROLE"),
)