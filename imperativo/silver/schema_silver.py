from common.config import SILVER_TABLE
from common.spark import spark

SILVER_SCHEMA = ".".join(SILVER_TABLE.split(".")[:2])


def create_silver_schema() -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
