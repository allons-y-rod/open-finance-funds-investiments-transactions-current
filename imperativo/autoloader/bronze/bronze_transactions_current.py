from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

import sys
sys.path.append("/Workspace/Users/<user_email>/imperative_open_finance_funds_investiments_transactions_current/autoloader")

from common.config import BRONZE_CHECKPOINT_PATH, BRONZE_TABLE, INPUT_PATH, cloudfiles_reader
from common.spark import spark
from tables_bronze_config import (
    create_bronze_checkpoints_volume,
    create_bronze_schema,
    create_bronze_table,
    transactions_current_schema,
)

create_bronze_schema()

SCHEMA = transactions_current_schema()


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

    return exploded.select(
        F.col("transaction.clientId").alias("client_id"),
        F.col("transaction.investimentId").alias("investiment_id"),
        F.col("transaction.transactionId").alias("transaction_id"),
        F.col("transaction.type").alias("type"),
        F.col("transaction.transactionType").alias("transaction_type"),
        F.col("transaction.transactionTypeAdditionalInfo").alias("transaction_type_additional_info"),
        F.col("transaction.transactionConversionDate").alias("transaction_conversion_date"),
        F.col("transaction.transactionQuotaPrice.amount").alias("transaction_quota_price_amount"),
        F.col("transaction.transactionQuotaPrice.currency").alias("transaction_quota_price_currency"),
        F.col("transaction.transactionQuotaQuantity").alias("transaction_quota_quantity"),
        F.col("transaction.transactionValue.amount").alias("transaction_value_amount"),
        F.col("transaction.transactionValue.currency").alias("transaction_value_currency"),
        F.col("transaction.transactionGrossValue.amount").alias("transaction_gross_value_amount"),
        F.col("transaction.transactionGrossValue.currency").alias("transaction_gross_value_currency"),
        F.col("transaction.incomeTax.amount").alias("income_tax_amount"),
        F.col("transaction.incomeTax.currency").alias("income_tax_currency"),
        F.col("transaction.financialTransactionTax.amount").alias("financial_transaction_tax_amount"),
        F.col("transaction.financialTransactionTax.currency").alias("financial_transaction_tax_currency"),
        F.col("transaction.transactionExitFee.amount").alias("transaction_exit_fee_amount"),
        F.col("transaction.transactionExitFee.currency").alias("transaction_exit_fee_currency"),
        F.col("transaction.transactionNetValue.amount").alias("transaction_net_value_amount"),
        F.col("transaction.transactionNetValue.currency").alias("transaction_net_value_currency"),
        "source_file",
        "ingestion_ts",
        "ingestion_date",
        "_rescued_data",
        F.to_date(
            F.date_format(F.col("transaction.transactionConversionDate"), "yyyy-MM"), "yyyy-MM"
        ).alias("transaction_conversion_month"),
    )


def start_bronze_stream() -> StreamingQuery:
    create_bronze_checkpoints_volume()
    create_bronze_table()
    bronze_stream = read_bronze_stream()

    return (
        bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
    )


if __name__ == "__main__":
    query = start_bronze_stream()
    query.awaitTermination()
