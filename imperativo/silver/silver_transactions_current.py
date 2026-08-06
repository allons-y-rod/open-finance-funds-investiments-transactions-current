import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DecimalType

import sys
sys.path.append("/Workspace/Users/<user_email>/imperative_open_finance_funds_investiments_transactions_current")

from common.config import BRONZE_TABLE, SILVER_CHECKPOINT_PATH, SILVER_REJECTED_TABLE, SILVER_TABLE
from common.spark import spark
from tables_silver_config import create_silver_rejected_table, create_silver_schema, create_silver_table

create_silver_schema()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_transactions_current")

EXPECTATIONS = {
    "valid_business_key": (
        "transaction_id IS NOT NULL AND transaction_id != '' "
        "AND client_id IS NOT NULL AND client_id != ''"
    )
}

REJECTED_PAYLOAD_EXCLUDED_COLUMNS = {
    "ingestion_ts",
    "ingestion_date",
    "_rescued_data",
    "transaction_conversion_month",
}


def _cast_columns(batch_df: DataFrame) -> DataFrame:
    return (
        batch_df
        .withColumn("transaction_conversion_date"     , F.col("transaction_conversion_date").cast("date"))
        .withColumn("transaction_quota_price_amount"  , F.col("transaction_quota_price_amount").cast(DecimalType(20, 2)))
        .withColumn("transaction_quota_quantity"      , F.col("transaction_quota_quantity").cast(DecimalType(20, 2)))
        .withColumn("transaction_value_amount"        , F.col("transaction_value_amount").cast(DecimalType(20, 2)))
        .withColumn("transaction_gross_value_amount"  , F.col("transaction_gross_value_amount").cast(DecimalType(20, 2)))
        .withColumn("income_tax_amount"               , F.col("income_tax_amount").cast(DecimalType(20, 2)))
        .withColumn("financial_transaction_tax_amount", F.col("financial_transaction_tax_amount").cast(DecimalType(20, 2)))
        .withColumn("transaction_exit_fee_amount"     , F.col("transaction_exit_fee_amount").cast(DecimalType(20, 2)))
        .withColumn("transaction_net_value_amount"    , F.col("transaction_net_value_amount").cast(DecimalType(20, 2)))
        .withColumn(
            "transaction_conversion_month",
            F.date_format(F.col("transaction_conversion_date"), "yyyy-MM"),
        )
    )


def _split_batch(batch_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    failed_names = F.array(
        *[
            F.when(~F.expr(condition), F.lit(name))
            for name, condition in EXPECTATIONS.items()
        ]
    )
    enriched = batch_df.withColumn(
        "_failure_reason",
        F.concat_ws(", ", F.filter(failed_names, lambda x: x.isNotNull())),
    )

    valid_df = enriched.where(F.col("_failure_reason") == "").drop("_failure_reason")
    rejected_df = enriched.where(F.col("_failure_reason") != "")

    return valid_df, rejected_df


def read_silver_stream() -> DataFrame:
    return spark.readStream.table(BRONZE_TABLE)


def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
    casted_df = _cast_columns(batch_df)
    payload_columns = [c for c in casted_df.columns if c not in REJECTED_PAYLOAD_EXCLUDED_COLUMNS]
    valid_df, rejected_df = _split_batch(casted_df)

    rejected_count = rejected_df.count()
    if rejected_count > 0:
        logger.warning(f"[batch {batch_id}] {rejected_count} rows failed expectations, sending to {SILVER_REJECTED_TABLE}")
        (
            rejected_df
            .select(
                F.to_json(F.struct(*payload_columns)).cast("string").alias("data"),
                F.col("_failure_reason").cast("string").alias("failure_reason"),
                F.lit(batch_id).cast("bigint").alias("batch_id"),
                F.current_timestamp().cast("timestamp").alias("rejected_at"),
            )
            .withColumn(
                "rejected_at_month",
                F.date_format(F.col("rejected_at"), "yyyy-MM").cast("string"),
            )
            .write.format("delta").mode("append").saveAsTable(SILVER_REJECTED_TABLE)
        )

    valid_df.write.format("delta").mode("append").saveAsTable(SILVER_TABLE)


def start_silver_stream() -> StreamingQuery:
    create_silver_table()
    create_silver_rejected_table()
    silver_stream = read_silver_stream()

    return (
        silver_stream.writeStream
        .foreachBatch(_write_batch)
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )


if __name__ == "__main__":
    query = start_silver_stream()
    query.awaitTermination()
