import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DecimalType

import sys
sys.path.append("/Workspace/Users/<user_email>/imperative_open_finance_funds_investiments_transactions_current/autoloader")

from imperativo.autoloader.common.config import BRONZE_TABLE, SILVER_CHECKPOINT_PATH, SILVER_REJECTED_TABLE, SILVER_TABLE
from imperativo.autoloader.common.rules_module import get_rules
from imperativo.autoloader.common.spark import spark
from imperativo.autoloader.silver.tables_silver_config import (
    create_silver_checkpoints_volume,
    create_silver_rejected_table,
    create_silver_schema,
    create_silver_table,
    DEDUP_ORDER,
    TRANSACTION_BUSINESS_KEY,
)

create_silver_schema()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_transactions_current")

RULES = get_rules("validity")
QUARANTINE_RULE = "NOT({0})".format(" AND ".join(RULES.values()))

MONETARY_COLUMNS = (
    "transaction_quota_price_amount",
    "transaction_quota_quantity",
    "transaction_value_amount",
    "transaction_gross_value_amount",
    "income_tax_amount",
    "financial_transaction_tax_amount",
    "transaction_exit_fee_amount",
    "transaction_net_value_amount",
)


def _cast_columns(batch_df: DataFrame) -> DataFrame:
    return batch_df.withColumns(
        {c: F.col(c).cast(DecimalType(20, 2)) for c in MONETARY_COLUMNS}
    )


def _flag_quarantine(batch_df: DataFrame) -> DataFrame:
    failed_rule_names = F.array(
        *[F.when(~F.expr(condition), F.lit(name)) for name, condition in RULES.items()]
    )
    return batch_df.withColumns({
        "is_quarantined": F.expr(QUARANTINE_RULE),
        "failure_reason": F.concat_ws(", ", F.filter(failed_rule_names, lambda x: x.isNotNull())),
    })


def _split_batch(batch_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid_df = batch_df.filter("is_quarantined = false").drop("is_quarantined", "failure_reason")
    invalid_df = batch_df.filter("is_quarantined = true").drop("is_quarantined")
    return valid_df, invalid_df


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


def read_silver_stream() -> DataFrame:
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table(BRONZE_TABLE)
    )


def _upsert_valid(valid_df: DataFrame) -> None:

    merge_condition = F.expr(
        " AND ".join(f"target.{c} = source.{c}" for c in TRANSACTION_BUSINESS_KEY)
    )

   
    batch_client_ids = [
        row.client_id
        for row in valid_df.select("client_id").distinct().collect()
        if row.client_id is not None
    ]
    if batch_client_ids:
        merge_condition = F.col("target.client_id").isin(batch_client_ids) & merge_condition

    source_order = ", ".join(f"source.{c}" for c in DEDUP_ORDER)
    target_order = ", ".join(f"target.{c}" for c in DEDUP_ORDER)
    newer_condition = f"struct({source_order}) >= struct({target_order})"

    target_table = DeltaTable.forName(spark, SILVER_TABLE)
    (
        target_table.alias("target")
        .merge(valid_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll(condition=newer_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
    
    casted_df = _cast_columns(batch_df)
    flagged_df = _flag_quarantine(casted_df)
    valid_df, invalid_df = _split_batch(flagged_df)

    invalid_count = invalid_df.count()
    if invalid_count > 0:
        logger.warning(f"[batch {batch_id}] {invalid_count} rows failed expectations, sending to {SILVER_REJECTED_TABLE}")
        invalid_df.write.format("delta").mode("append").saveAsTable(SILVER_REJECTED_TABLE)

    _upsert_valid(_deduplicate_transactions(valid_df))

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
