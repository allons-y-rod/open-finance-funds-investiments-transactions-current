BRONZE_TABLE = "dlt_open_finance_funds_investiments_transactions_current.bronze.bronze_transactions_current"
SILVER_TABLE = "dlt_open_finance_funds_investiments_transactions_current.silver.silver_transactions_current"
SILVER_REJECTED_TABLE = "dlt_open_finance_funds_investiments_transactions_current.silver.silver_transactions_current_rechaco"

TRANSACTION_BUSINESS_KEY = ["client_id", "transaction_id"]
DEDUP_ORDER = ["ingestion_ts", "source_file"]

SILVER_SCHEMA = """
    client_id                          STRING NOT NULL,
    investiment_id                     STRING,
    transaction_id                     STRING NOT NULL,
    type                                STRING,
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
"""

SILVER_REJECTED_SCHEMA = """
    data                STRING,
    failure_reason      STRING,
    rejected_at         TIMESTAMP,
    rejected_at_month   DATE
"""

EXPECTATIONS = {
    "valid_business_key": (
        "transaction_id IS NOT NULL AND transaction_id != '' "
        "AND client_id IS NOT NULL AND client_id != ''"
    )
}

REJECTED_PAYLOAD_EXCLUDED_COLUMNS = {
    "ingestion_ts",
    "ingestion_date",
    "_rescued_data",
    "transaction_conversion_month",
}
