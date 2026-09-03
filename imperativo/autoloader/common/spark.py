from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """
    Returns the active SparkSession or creates one if it does not exist.
    """
    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = (
            SparkSession.builder
            .appName("POC OpenFinance")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.databricks.delta.merge.enableLowShuffle", "true")
            .getOrCreate()
        )

    return spark


spark = get_spark()