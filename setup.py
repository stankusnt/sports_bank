from setuptools import find_packages, setup

setup(
    name="sports_bank",
    packages=find_packages(exclude=["sports_bank_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-snowflake",
        "dagster-webserver",
        "geopandas",
        "kaleido",
        "pandas",
        "plotly",
        "shapely",
        "nba_api",
        "time",
        "requests"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
