from common.config import REJECTED_TABLE, TARGET_TABLE
from common.spark import spark


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
            data                STRING,
            failure_reason      STRING,
            batch_id            BIGINT,
            rejected_at         TIMESTAMP,
            rejected_at_month   STRING
        )
        USING DELTA
        PARTITIONED BY (rejected_at_month)
        COMMENT 'Quarentena - Bronze layer - Fundos de Investimentos - Transactions Current - linhas que falharam expectations'
        TBLPROPERTIES ('quality' = 'bronze_rejected')
    """)
