import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DecimalType

import sys
sys.path.append("/Workspace/Users/<user_email>/imperative_open_finance_funds_investiments_transactions_current")

from common.config import BRONZE_TABLE, SILVER_CHECKPOINT_PATH, SILVER_REJECTED_TABLE, SILVER_TABLE
from common.spark import spark
from tables_silver_config import (
    create_silver_checkpoints_volume,
    create_silver_rejected_table,
    create_silver_schema,
    create_silver_table,
)

create_silver_schema()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_transactions_current")

EXPECTATIONS = {
    "valid_business_key": (
        "transaction_id IS NOT NULL AND transaction_id != '' "
        "AND client_id IS NOT NULL AND client_id != ''"
    )
}

TRANSACTION_BUSINESS_KEY = ["client_id", "transaction_id"]
DEDUP_ORDER = ["ingestion_ts", "source_file"]

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


def _deduplicate_transactions(df: DataFrame) -> DataFrame:
    window = (
        Window
        .partitionBy(*TRANSACTION_BUSINESS_KEY)
        .orderBy(*[F.col(c).desc() for c in DEDUP_ORDER])
    )

    return (
        df.withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter("_row_number = 1")
        .drop("_row_number")
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


def _upsert_valid(valid_df: DataFrame) -> None:
    merge_key_condition = " AND ".join(f"target.{c} = source.{c}" for c in TRANSACTION_BUSINESS_KEY)
    source_order = ", ".join(f"source.{c}" for c in DEDUP_ORDER)
    target_order = ", ".join(f"target.{c}" for c in DEDUP_ORDER)
    newer_condition = f"struct({source_order}) >= struct({target_order})"

    target_table = DeltaTable.forName(spark, SILVER_TABLE)
    (
        target_table.alias("target")
        .merge(valid_df.alias("source"), merge_key_condition)
        .whenMatchedUpdateAll(condition=newer_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
    casted_df = _cast_columns(batch_df)
    deduped_df = _deduplicate_transactions(casted_df)
    payload_columns = [c for c in deduped_df.columns if c not in REJECTED_PAYLOAD_EXCLUDED_COLUMNS]
    valid_df, rejected_df = _split_batch(deduped_df)

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

    _upsert_valid(valid_df)


def start_silver_stream() -> StreamingQuery:
    create_silver_checkpoints_volume()
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
