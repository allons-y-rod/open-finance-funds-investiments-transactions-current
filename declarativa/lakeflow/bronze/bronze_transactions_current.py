from pyspark import pipelines as dp
from pyspark.sql import functions as F

from declarativa.lakeflow.common.config import INPUT_PATH, cloudfiles_reader
from declarativa.lakeflow.bronze.table_bronze_tc_config import transactions_current_schema

SCHEMA = transactions_current_schema()


@dp.table(
    name="dlt_open_finance_funds_investiments_transactions_current.bronze.bronze_transactions_current",
    comment="Bronze layer - Fundos de Investimentos - Transactions Current",
    table_properties={"quality": "bronze"},
    cluster_by=["transaction_conversion_month", "transaction_id"],

)
def bronze_transactions_current():

    raw = (
        cloudfiles_reader(spark.readStream)
        .schema(SCHEMA)
        .load(INPUT_PATH)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("ingestion_date", F.to_date("ingestion_ts"))
    )

    return (
        raw
        .select(
            F.explode_outer("data").alias("transaction"),
            "source_file",
            "ingestion_ts",
            "ingestion_date",
            "_rescued_data",
        )
        .select(
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
        )
        .withColumn(
            "transaction_conversion_month",
            F.to_date(F.date_format("transaction_conversion_date", "yyyy-MM"), "yyyy-MM"),
        )
    )