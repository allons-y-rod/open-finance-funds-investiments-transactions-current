from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from declarativa.lakeflow.silver.table_silver_tc_config import (
    DEDUP_ORDER,
    EXPECTATIONS,
    REJECTED_PAYLOAD_EXCLUDED_COLUMNS,
    SILVER_REJECTED_SCHEMA,
    SILVER_SCHEMA,
    TRANSACTION_BUSINESS_KEY,
    BRONZE_TABLE,
    SILVER_TABLE,
    SILVER_REJECTED_TABLE
)


@dp.view(name="silver_transactions_current_casted")
def silver_transactions_current_casted():
    return (
        dp.read_stream(BRONZE_TABLE)
        .withColumns(
            {
            "transaction_quota_price_amount"  : F.col("transaction_quota_price_amount").cast(DecimalType(20, 2)),
            "transaction_quota_quantity"      : F.col("transaction_quota_quantity").cast(DecimalType(20, 2)),
            "transaction_value_amount"        : F.col("transaction_value_amount").cast(DecimalType(20, 2)),
            "transaction_gross_value_amount"  : F.col("transaction_gross_value_amount").cast(DecimalType(20, 2)),
            "income_tax_amount"               : F.col("income_tax_amount").cast(DecimalType(20, 2)),
            "financial_transaction_tax_amount": F.col("financial_transaction_tax_amount").cast(DecimalType(20, 2)),
            "transaction_exit_fee_amount"     : F.col("transaction_exit_fee_amount").cast(DecimalType(20, 2)),
            "transaction_net_value_amount"    : F.col("transaction_net_value_amount").cast(DecimalType(20, 2)),
            }
        )
    )


@dp.view(name="silver_transactions_current_valid")
@dp.expect_all_or_drop(EXPECTATIONS)
def silver_transactions_current_valid():
    return dp.read_stream("silver_transactions_current_casted")


dp.create_streaming_table(
    name=SILVER_TABLE,
    comment="Silver layer - Fundos de Investimentos - Transactions Current",
    table_properties={"quality": "silver"},
    cluster_by=["transaction_conversion_month"],
    schema=SILVER_SCHEMA,
)

dp.create_auto_cdc_flow(
    target=SILVER_TABLE,
    source="silver_transactions_current_valid",
    keys=TRANSACTION_BUSINESS_KEY,
    sequence_by=F.struct(*DEDUP_ORDER),
    stored_as_scd_type=1,
)


@dp.table(
    name=SILVER_REJECTED_TABLE,
    comment=(
        "Quarentena - Silver layer - Fundos de Investimentos - Transactions Current - "
        "linhas que falharam expectations"
    ),
    table_properties={"quality": "silver_rejected"},
    partition_cols=["rejected_at_month"],
    schema=SILVER_REJECTED_SCHEMA,
)
def silver_transactions_current_rechaco():
    casted = dp.read_stream("silver_transactions_current_casted")
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
        .withColumn(
            "rejected_at_month",
            F.to_date(F.date_format("rejected_at", "yyyy-MM"), "yyyy-MM"),
        )
    )
