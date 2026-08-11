from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from declarativa.lakeflow.silver.table_silver_tc_config import (
    DEDUP_ORDER,
    EXPECTATIONS,
    REJECTED_PAYLOAD_EXCLUDED_COLUMNS,
    TRANSACTION_BUSINESS_KEY,
)

BRONZE_TABLE = "bronze_transactions_current"


@dp.view(name="silver_transactions_current_casted")
def silver_transactions_current_casted():
    return (
        spark.read.table(BRONZE_TABLE)
        .withColumn("transaction_conversion_date"     , F.col("transaction_conversion_date").cast("date"))
        .withColumn("transaction_quota_price_amount"  , F.col("transaction_quota_price_amount").cast(DecimalType(20, 2)))
        .withColumn("transaction_quota_quantity"      , F.col("transaction_quota_quantity").cast(DecimalType(20, 2)))
        .withColumn("transaction_value_amount"        , F.col("transaction_value_amount").cast(DecimalType(20, 2)))
        .withColumn("transaction_gross_value_amount"  , F.col("transaction_gross_value_amount").cast(DecimalType(20, 2)))
        .withColumn("income_tax_amount"               , F.col("income_tax_amount").cast(DecimalType(20, 2)))
        .withColumn("financial_transaction_tax_amount", F.col("financial_transaction_tax_amount").cast(DecimalType(20, 2)))
        .withColumn("transaction_exit_fee_amount"     , F.col("transaction_exit_fee_amount").cast(DecimalType(20, 2)))
        .withColumn("transaction_net_value_amount"    , F.col("transaction_net_value_amount").cast(DecimalType(20, 2)))
    )


@dp.view(name="silver_transactions_current_valid")
@dp.expect_all_or_drop(EXPECTATIONS)
def silver_transactions_current_valid():
    return spark.read.table("silver_transactions_current_casted")


@dp.table(
    name="dlt_open_finance_funds_investiments_transactions_current.silver.silver_transactions_current",
    comment="Silver layer - Fundos de Investimentos - Transactions Current",
    table_properties={"quality": "silver"},
    cluster_by=["transaction_conversion_month"],
)
def silver_transactions_current():
    window = (
        Window
        .partitionBy(*TRANSACTION_BUSINESS_KEY)
        .orderBy(*[F.col(c).desc() for c in DEDUP_ORDER])
    )

    return (
        spark.read.table("silver_transactions_current_valid")
        .withColumn("_row_number", F.row_number().over(window))
        .filter("_row_number = 1")
        .drop("_row_number")
    )


@dp.table(
    name="dlt_open_finance_funds_investiments_transactions_current.silver.silver_transactions_current_rechaco",
    comment=(
        "Quarentena - Silver layer - Fundos de Investimentos - Transactions Current - "
        "linhas que falharam expectations"
    ),
    table_properties={"quality": "silver_rejected"},
    partition_cols=["rejected_at_month"],
)
def silver_transactions_current_rechaco():
    casted = spark.read.table("silver_transactions_current_casted")
    payload_columns = [c for c in casted.columns if c not in REJECTED_PAYLOAD_EXCLUDED_COLUMNS]

    failed_names = F.array(
        *[
            F.when(~F.expr(condition), F.lit(name))
            for name, condition in EXPECTATIONS.items()
        ]
    )
    enriched = casted.withColumn(
        "_failure_reason",
        F.concat_ws(", ", F.filter(failed_names, lambda x: x.isNotNull())),
    )

    return (
        enriched
        .where(F.col("_failure_reason") != "")
        .select(
            F.to_json(F.struct(*payload_columns)).alias("data"),
            F.col("_failure_reason").alias("failure_reason"),
            F.current_timestamp().alias("rejected_at"),
        )
        .withColumn("rejected_at_month", F.date_format("rejected_at", "yyyy-MM"))
    )
