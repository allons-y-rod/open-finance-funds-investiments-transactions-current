import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType

import sys
sys.path.append("/Workspace/Users/<user_email>/imperative_open_finance_funds_investiments_transactions_current/batch")

from common.config import BRONZE_REJECTED_TABLE, BRONZE_TABLE, INPUT_PATH
from common.spark import spark
from tables_bronze_config import (
    create_bronze_rejected_table,
    create_bronze_schema,
    create_bronze_table,
    transactions_current_schema,
)

create_bronze_schema()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_transactions_current_v2")

SCHEMA = transactions_current_schema()
TRANSACTION_FIELDS = SCHEMA["data"].dataType.elementType.fieldNames()

# rescuedDataColumn faz o from_json se comportar como o leitor nativo cloudFiles.format=json:
# campos que nao estao no schema (colunas novas) vao para _rescued_data, nao viram rejeicao.
FROM_JSON_OPTIONS = {
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": "_corrupt_record",
    "rescuedDataColumn": "_rescued_data",
}

# schema tipado (valores reais) + colunas de controle exigidas pelas options acima
STRUCT_PARSE_SCHEMA = StructType(
    SCHEMA.fields
    + [
        StructField("_corrupt_record", StringType(), True),
        StructField("_rescued_data", StringType(), True),
    ]
)

# mesma posicao do array "data", mas cada transacao vira um Map<String,String> em vez de
# struct tipado: chave ausente no JSON nao aparece no mapa; chave presente aparece mesmo
# que o valor nao seja castavel pra string (caso dos campos monetarios aninhados) - por
# isso a checagem de presenca fica restrita ao campo pai, sem descer em amount/currency.
FIELDS_PARSE_SCHEMA = StructType([
    StructField("data", ArrayType(MapType(StringType(), StringType())), True),
])

REJECTED_PAYLOAD_EXCLUDED_COLUMNS = {"_valid_json", "_missing_fields", "_failure_reason"}


def read_raw_files() -> DataFrame:
    return (
        spark.read
        .option("wholetext", "true")
        .option("pathGlobFilter", "*.json")
        .text(INPUT_PATH)
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("ingestion_date", F.to_date("ingestion_ts"))
    )


def _parse(raw: DataFrame) -> DataFrame:
    parsed = (
        raw
        .withColumn("_typed", F.from_json(F.col("value"), STRUCT_PARSE_SCHEMA, FROM_JSON_OPTIONS))
        .withColumn("_fields", F.from_json(F.col("value"), FIELDS_PARSE_SCHEMA))
    )

    zipped = parsed.withColumn(
        "_transactions",
        F.arrays_zip(
            F.col("_typed.data").alias("transaction"),
            F.col("_fields.data").alias("fields"),
        ),
    )

    exploded = zipped.select(
        F.explode_outer("_transactions").alias("_item"),
        F.col("_typed._corrupt_record").alias("_corrupt_record"),
        F.col("_typed._rescued_data").alias("_rescued_data"),
        "source_file",
        "ingestion_ts",
        "ingestion_date",
    )

    missing_fields = F.filter(
        F.array(*[
            F.when(~F.map_contains_key(F.col("_item.fields"), name), F.lit(name))
            for name in TRANSACTION_FIELDS
        ]),
        lambda x: x.isNotNull(),
    )

    return exploded.select(
        F.col("_item.transaction.clientId").alias("client_id"),
        F.col("_item.transaction.investimentId").alias("investiment_id"),
        F.col("_item.transaction.transactionId").alias("transaction_id"),
        F.col("_item.transaction.type").alias("type"),
        F.col("_item.transaction.transactionType").alias("transaction_type"),
        F.col("_item.transaction.transactionTypeAdditionalInfo").alias("transaction_type_additional_info"),
        F.col("_item.transaction.transactionConversionDate").alias("transaction_conversion_date"),
        F.col("_item.transaction.transactionQuotaPrice.amount").alias("transaction_quota_price_amount"),
        F.col("_item.transaction.transactionQuotaPrice.currency").alias("transaction_quota_price_currency"),
        F.col("_item.transaction.transactionQuotaQuantity").alias("transaction_quota_quantity"),
        F.col("_item.transaction.transactionValue.amount").alias("transaction_value_amount"),
        F.col("_item.transaction.transactionValue.currency").alias("transaction_value_currency"),
        F.col("_item.transaction.transactionGrossValue.amount").alias("transaction_gross_value_amount"),
        F.col("_item.transaction.transactionGrossValue.currency").alias("transaction_gross_value_currency"),
        F.col("_item.transaction.incomeTax.amount").alias("income_tax_amount"),
        F.col("_item.transaction.incomeTax.currency").alias("income_tax_currency"),
        F.col("_item.transaction.financialTransactionTax.amount").alias("financial_transaction_tax_amount"),
        F.col("_item.transaction.financialTransactionTax.currency").alias("financial_transaction_tax_currency"),
        F.col("_item.transaction.transactionExitFee.amount").alias("transaction_exit_fee_amount"),
        F.col("_item.transaction.transactionExitFee.currency").alias("transaction_exit_fee_currency"),
        F.col("_item.transaction.transactionNetValue.amount").alias("transaction_net_value_amount"),
        F.col("_item.transaction.transactionNetValue.currency").alias("transaction_net_value_currency"),
        "source_file",
        "ingestion_ts",
        "ingestion_date",
        "_rescued_data",
        F.col("_corrupt_record").isNull().alias("_valid_json"),
        F.concat_ws(", ", missing_fields).alias("_missing_fields"),
    )


def _split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    reasons = F.array(
        F.when(~F.col("_valid_json"), F.lit("invalid_json")),
        F.when(
            F.col("_missing_fields") != "",
            F.concat(F.lit("missing_fields: "), F.col("_missing_fields")),
        ),
    )
    enriched = df.withColumn(
        "_failure_reason",
        F.concat_ws("; ", F.filter(reasons, lambda x: x.isNotNull())),
    )

    valid_df = enriched.where(F.col("_failure_reason") == "").drop(
        "_failure_reason", "_valid_json", "_missing_fields"
    )
    rejected_df = enriched.where(F.col("_failure_reason") != "")

    return valid_df, rejected_df


def _write(df: DataFrame) -> None:
    valid_df, rejected_df = _split(df)
    payload_columns = [c for c in df.columns if c not in REJECTED_PAYLOAD_EXCLUDED_COLUMNS]

    rejected_count = rejected_df.count()
    if rejected_count > 0:
        logger.warning(
            f"{rejected_count} rows failed structural validation "
            f"(invalid JSON or missing schema field), sending to {BRONZE_REJECTED_TABLE}"
        )
        (
            rejected_df
            .select(
                F.to_json(F.struct(*payload_columns)).cast("string").alias("data"),
                F.col("_failure_reason").cast("string").alias("failure_reason"),
                F.current_timestamp().cast("timestamp").alias("rejected_at"),
            )
            .withColumn("rejected_at_month", F.date_format(F.col("rejected_at"), "yyyy-MM").cast("string"))
            .write.format("delta").mode("append").saveAsTable(BRONZE_REJECTED_TABLE)
        )

    valid_df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)


def run() -> None:
    create_bronze_table()
    create_bronze_rejected_table()
    _write(_parse(read_raw_files()))


if __name__ == "__main__":
    run()
