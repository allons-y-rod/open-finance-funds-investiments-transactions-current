from pyspark import pipelines as dp
from pyspark.sql import functions as F

from declarativa.lakeflow.common.rules_module import get_rules

from declarativa.lakeflow.common.config import (
    BRONZE_TABLE,
    SILVER_TABLE,
    SILVER_REJECTED_TABLE
) 

from declarativa.lakeflow.silver.table_silver_tc_config import (
    DEDUP_ORDER,
    SILVER_SCHEMA,
    SILVER_SCHEMA_REJECTED,
    TRANSACTION_BUSINESS_KEY,
)

rules = get_rules("validity")
quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))


@dp.view(name="silver_transactions_current_casted")
def silver_transactions_current_casted():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table(BRONZE_TABLE)
        .withColumns(
            {
                c: F.expr(f"try_cast({c} AS DECIMAL(20, 2))")
                for c in (
                    "transaction_quota_price_amount",
                    "transaction_quota_quantity",
                    "transaction_value_amount",
                    "transaction_gross_value_amount",
                    "income_tax_amount",
                    "financial_transaction_tax_amount",
                    "transaction_exit_fee_amount",
                    "transaction_net_value_amount",
                )
            }
        )
    )


@dp.table(
    name="silver_transactions_current_temporary",
    comment=(
        "Quarentena intermediária - Silver - Fundos de Investimentos - Transactions Current - "
        "todas as linhas casted com a flag is_quarantined (tabela temporária, não publicada)"
    ),
    temporary=True,
    partition_cols=["is_quarantined"],
)
@dp.expect_all(rules)
def silver_transactions_current_temporary():
    failed_rule_names = F.array(
        *[F.when(~F.expr(condition), F.lit(name)) for name, condition in rules.items()]
    )
    return (
        dp.read_stream("silver_transactions_current_casted")
        .withColumns({
            "is_quarantined": F.expr(quarantine_rules),
            "failure_reason": F.concat_ws(", ", F.filter(failed_rule_names, lambda x: x.isNotNull()))
        })
    )


@dp.view(name="silver_transactions_current_valid")
def silver_transactions_current_valid():
    return (
        spark.readStream
        .table("silver_transactions_current_temporary")
        .filter("is_quarantined = false")
        .drop("is_quarantined", "failure_reason")
    )


@dp.view(name="silver_transactions_current_invalid")
def silver_transactions_current_invalid():
    return (
        spark.readStream
        .table("silver_transactions_current_temporary")
        .filter("is_quarantined = true")
        .drop("is_quarantined")
    )


dp.create_streaming_table(
    name=SILVER_TABLE,
    comment="Silver layer - Fundos de Investimentos - Transactions Current",
    table_properties={"quality": "silver"},
    cluster_by=["client_id", "transaction_conversion_month"],
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
        "Rejeitados - Silver layer - Fundos de Investimentos - Transactions Current - "
        "linhas que falharam expectations (is_quarantined = true)"
    ),
    schema=SILVER_SCHEMA_REJECTED,
    table_properties={"quality": "silver_rejected"},
    cluster_by=["client_id", "transaction_conversion_month"],
)
def silver_transactions_current_rechaco():
    return dp.read_stream("silver_transactions_current_invalid")
