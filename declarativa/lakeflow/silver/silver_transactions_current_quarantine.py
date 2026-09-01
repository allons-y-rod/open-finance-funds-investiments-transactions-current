# Camada silver — VERSÃO "quarantine table" (padrão APPLY EXPECTATIONS / split via coluna booleana
# da doc do Databricks). Alternativa a `silver_transactions_current.py`.
#
# Cadeia: _casted (@dp.view) -> _quarantine (@dp.table temporary + @dp.expect_all, coluna
# is_quarantined) -> _valid / _invalid (@dp.view, filtram a flag) -> create_auto_cdc_flow (a partir
# de _valid) + _rechaco (@dp.table, materializa _invalid como está).
#
# _rechaco tem a MESMA lógica de _valid: só o filtro da flag e o drop da coluna — sem serialização
# em JSON, sem failure_reason/rejected_at. As duas tabelas finais têm o mesmo conjunto de colunas
# de negócio (as de _casted); a diferença é só quais linhas vão para cada uma.
#
# SILVER_TABLE e SILVER_REJECTED_TABLE mantêm o nome da versão `silver_transactions_current.py`,
# mas SILVER_REJECTED_TABLE passa a ter o schema largo (colunas de _casted), não o schema
# data(JSON)/failure_reason/rejected_at da versão padrão. Só um dos dois scripts deve estar ativo
# na pipeline por vez (ambos definem os mesmos datasets). Usa `table_silver_tc_quarantine_config.py`.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from declarativa.lakeflow.silver.table_silver_tc_quarantine_config import (
    DEDUP_ORDER,
    EXPECTATIONS,
    QUARANTINE_RULE,
    SILVER_SCHEMA,
    TRANSACTION_BUSINESS_KEY,
    BRONZE_TABLE,
    SILVER_TABLE,
    SILVER_REJECTED_TABLE
)


@dp.view(name="silver_transactions_current_casted")
def silver_transactions_current_casted():
    return (
        spark.readStream
            .option("skipChangeCommits", "true")
            .table(BRONZE_TABLE)
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


@dp.table(
    name="silver_transactions_current_quarantine",
    comment=(
        "Quarentena intermediária - Silver - Fundos de Investimentos - Transactions Current - "
        "todas as linhas casted com a flag is_quarantined (tabela temporária, não publicada)"
    ),
    temporary=True,
    partition_cols=["is_quarantined"],
)
@dp.expect_all(EXPECTATIONS)
def silver_transactions_current_quarantine():
    return (
        dp.read_stream("silver_transactions_current_casted")
        .withColumn("is_quarantined", F.expr(QUARANTINE_RULE))
    )


@dp.view(name="silver_transactions_current_valid")
def silver_transactions_current_valid():
    return (
        spark.readStream
            .table("silver_transactions_current_quarantine")
            .filter("is_quarantined = false")
            .drop("is_quarantined")
    )


@dp.view(name="silver_transactions_current_invalid")
def silver_transactions_current_invalid():
    return (
        spark.readStream
            .table("silver_transactions_current_quarantine")
            .filter("is_quarantined = true")
            .drop("is_quarantined")
    )


dp.create_streaming_table(
    name=SILVER_TABLE,
    comment="Silver layer - Fundos de Investimentos - Transactions Current",
    table_properties={"quality": "silver"},
    cluster_by=["client_id","transaction_conversion_month"],
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
    table_properties={"quality": "silver_rejected"},
    cluster_by=["client_id","transaction_conversion_month"],
)
def silver_transactions_current_rechaco():
    return dp.read_stream("silver_transactions_current_invalid")
