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
            .getOrCreate()
        )

    return spark


spark = get_spark()