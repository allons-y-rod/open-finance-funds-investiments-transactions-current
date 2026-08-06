from common.config import BRONZE_TABLE
from common.spark import spark

BRONZE_SCHEMA = ".".join(BRONZE_TABLE.split(".")[:2])


def create_bronze_schema() -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
