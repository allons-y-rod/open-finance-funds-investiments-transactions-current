from common.config import BRONZE_TABLE
from common.spark import spark


def create_bronze_table() -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            client_id                          STRING,
            investiment_id                     STRING,
            transaction_id                     STRING,
            type                                STRING,
            transaction_type                   STRING,
            transaction_type_additional_info   STRING,
            transaction_conversion_date        STRING,
            transaction_quota_price_amount     STRING,
            transaction_quota_price_currency   STRING,
            transaction_quota_quantity         STRING,
            transaction_value_amount           STRING,
            transaction_value_currency         STRING,
            transaction_gross_value_amount     STRING,
            transaction_gross_value_currency   STRING,
            income_tax_amount                  STRING,
            income_tax_currency                STRING,
            financial_transaction_tax_amount   STRING,
            financial_transaction_tax_currency STRING,
            transaction_exit_fee_amount        STRING,
            transaction_exit_fee_currency      STRING,
            transaction_net_value_amount       STRING,
            transaction_net_value_currency     STRING,
            source_file                        STRING,
            ingestion_ts                       TIMESTAMP,
            ingestion_date                     DATE,
            _rescued_data                      STRING
        )
        USING DELTA
        CLUSTER BY (ingestion_date)
        COMMENT 'Bronze layer - Fundos de Investimentos - Transactions Current'
        TBLPROPERTIES ('quality' = 'bronze')
    """)
