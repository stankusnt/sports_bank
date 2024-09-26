from setuptools import find_packages, setup

setup(
    name="sports_bank",
    packages=find_packages(exclude=["sports_bank_tests"]),
    install_requires=[
        "dagster==1.8.6",
        "dagster-cloud",
        "dagster-snowflake==0.24.6",
        "dagster-webserver",
        "geopandas",
        "kaleido",
        "pandas",
        "plotly",
        "shapely",
        "nba_api",
        "requests"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
