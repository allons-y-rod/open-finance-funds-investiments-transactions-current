import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DecimalType

import sys
sys.path.append("/Workspace/Users/<user_email>/imperative_open_finance_funds_investiments_transactions_current")

from common.config import CHECKPOINT_PATH, INPUT_PATH, REJECTED_TABLE, TARGET_TABLE, cloudfiles_reader
from common.schemas import transactions_current_schema
from common.spark import spark

SCHEMA = transactions_current_schema()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_transactions_current")

EXPECTATIONS = {
    "valid_business_key": (
        "transaction_id IS NOT NULL AND transaction_id != '' "
        "AND client_id IS NOT NULL AND client_id != ''"
    )
}


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


def create_target_table() -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            client_id                          STRING,
            investiment_id                     STRING,
            transaction_id                     STRING,
            type                               STRING,
            transaction_type                   STRING,
            transaction_type_additional_info   STRING,
            transaction_conversion_date        DATE,
            transaction_quota_price_amount     DECIMAL(20, 2),
            transaction_quota_price_currency   STRING,
            transaction_quota_quantity         DECIMAL(20, 2),
            transaction_value_amount           DECIMAL(20, 2),
            transaction_value_currency         STRING,
            transaction_gross_value_amount     DECIMAL(20, 2),
            transaction_gross_value_currency   STRING,
            income_tax_amount                  DECIMAL(20, 2),
            income_tax_currency                STRING,
            financial_transaction_tax_amount   DECIMAL(20, 2),
            financial_transaction_tax_currency STRING,
            transaction_exit_fee_amount        DECIMAL(20, 2),
            transaction_exit_fee_currency      STRING,
            transaction_net_value_amount       DECIMAL(20, 2),
            transaction_net_value_currency     STRING,
            source_file                        STRING,
            ingestion_ts                       TIMESTAMP,
            ingestion_date                     DATE,
            _rescued_data                      STRING,
            transaction_conversion_month       STRING
        )
        USING DELTA
        CLUSTER BY (transaction_conversion_month, transaction_id)
        COMMENT 'Bronze layer - Fundos de Investimentos - Transactions Current'
        TBLPROPERTIES ('quality' = 'bronze')
    """)


def create_rejected_table() -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REJECTED_TABLE} (
            client_id                          STRING,
            investiment_id                     STRING,
            transaction_id                     STRING,
            type                               STRING,
            transaction_type                   STRING,
            transaction_type_additional_info   STRING,
            transaction_conversion_date        DATE,
            transaction_quota_price_amount     DECIMAL(20, 2),
            transaction_quota_price_currency   STRING,
            transaction_quota_quantity         DECIMAL(20, 2),
            transaction_value_amount           DECIMAL(20, 2),
            transaction_value_currency         STRING,
            transaction_gross_value_amount     DECIMAL(20, 2),
            transaction_gross_value_currency   STRING,
            income_tax_amount                  DECIMAL(20, 2),
            income_tax_currency                STRING,
            financial_transaction_tax_amount   DECIMAL(20, 2),
            financial_transaction_tax_currency STRING,
            transaction_exit_fee_amount        DECIMAL(20, 2),
            transaction_exit_fee_currency      STRING,
            transaction_net_value_amount       DECIMAL(20, 2),
            transaction_net_value_currency     STRING,
            source_file                        STRING,
            ingestion_ts                       TIMESTAMP,
            ingestion_date                     DATE,
            _rescued_data                      STRING,
            transaction_conversion_month       STRING,
            failure_reason                     STRING,
            batch_id                           BIGINT,
            rejected_at                        TIMESTAMP
        )
        USING DELTA
        COMMENT 'Quarentena - Bronze layer - Fundos de Investimentos - Transactions Current - linhas que falharam expectations'
        TBLPROPERTIES ('quality' = 'bronze_rejected')
    """)


def read_bronze_stream() -> DataFrame:
    raw = (
        cloudfiles_reader(spark.readStream)
        .schema(SCHEMA)
        .load(INPUT_PATH)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("ingestion_date", F.to_date("ingestion_ts"))
    )

    exploded = raw.select(
        F.explode_outer("data").alias("transaction"),
        "source_file",
        "ingestion_ts",
        "ingestion_date",
        "_rescued_data",
    )

    return (
        exploded
        .select(
            F.col("transaction.clientId").alias("client_id"),
            F.col("transaction.investimentId").alias("investiment_id"),
            F.col("transaction.transactionId").alias("transaction_id"),
            F.col("transaction.type").alias("type"),
            F.col("transaction.transactionType").alias("transaction_type"),
            F.col("transaction.transactionTypeAdditionalInfo").alias("transaction_type_additional_info"),
            F.col("transaction.transactionConversionDate").cast("date").alias("transaction_conversion_date"),
            F.col("transaction.transactionQuotaPrice.amount").cast(DecimalType(20, 2)).alias("transaction_quota_price_amount"),
            F.col("transaction.transactionQuotaPrice.currency").alias("transaction_quota_price_currency"),
            F.col("transaction.transactionQuotaQuantity").cast(DecimalType(20, 2)).alias("transaction_quota_quantity"),
            F.col("transaction.transactionValue.amount").cast(DecimalType(20, 2)).alias("transaction_value_amount"),
            F.col("transaction.transactionValue.currency").alias("transaction_value_currency"),
            F.col("transaction.transactionGrossValue.amount").cast(DecimalType(20, 2)).alias("transaction_gross_value_amount"),
            F.col("transaction.transactionGrossValue.currency").alias("transaction_gross_value_currency"),
            F.col("transaction.incomeTax.amount").cast(DecimalType(20, 2)).alias("income_tax_amount"),
            F.col("transaction.incomeTax.currency").alias("income_tax_currency"),
            F.col("transaction.financialTransactionTax.amount").cast(DecimalType(20, 2)).alias("financial_transaction_tax_amount"),
            F.col("transaction.financialTransactionTax.currency").alias("financial_transaction_tax_currency"),
            F.col("transaction.transactionExitFee.amount").cast(DecimalType(20, 2)).alias("transaction_exit_fee_amount"),
            F.col("transaction.transactionExitFee.currency").alias("transaction_exit_fee_currency"),
            F.col("transaction.transactionNetValue.amount").cast(DecimalType(20, 2)).alias("transaction_net_value_amount"),
            F.col("transaction.transactionNetValue.currency").alias("transaction_net_value_currency"),
            "source_file",
            "ingestion_ts",
            "ingestion_date",
            "_rescued_data",
        )
        .withColumn(
            "transaction_conversion_month",
            F.date_format("transaction_conversion_date", "yyyy-MM"),
        )
    )


def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
    original_columns = batch_df.columns
    valid_df, rejected_df = _split_batch(batch_df)

    rejected_count = rejected_df.count()
    if rejected_count > 0:
        logger.warning(f"[batch {batch_id}] {rejected_count} rows failed expectations, sending to {REJECTED_TABLE}")
        (
            rejected_df
            .select(
                *original_columns,
                F.col("_failure_reason").alias("failure_reason"),
                F.lit(batch_id).alias("batch_id"),
                F.current_timestamp().alias("rejected_at"),
            )
            .write.format("delta").mode("append").saveAsTable(REJECTED_TABLE)
        )

    valid_df.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)


def start_bronze_stream() -> StreamingQuery:
    create_target_table()
    create_rejected_table()
    bronze_stream = read_bronze_stream()

    return (
        bronze_stream.writeStream
        .foreachBatch(_write_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )


if __name__ == "__main__":
    query = start_bronze_stream()
    query.awaitTermination()
