from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

def transactions_current_schema():
    monetary_type = StructType([
        StructField("amount", StringType(), True),
        StructField("currency", StringType(), True)
    ])

    transaction_schema = StructType([
        StructField("clientId", StringType(), False),
        StructField("investimentId", StringType(), False),
        StructField("transactionId", StringType(), False),
        StructField("type", StringType(), True),
        StructField("transactionType", StringType(), True),
        StructField("transactionTypeAdditionalInfo", StringType(), True),
        StructField("transactionConversionDate", StringType(), True),
        StructField("transactionQuotaPrice", monetary_type, True),
        StructField("transactionQuotaQuantity", StringType(), True),
        StructField("transactionValue", monetary_type, True),
        StructField("transactionGrossValue", monetary_type, True),
        StructField("incomeTax", monetary_type, True),
        StructField("financialTransactionTax", monetary_type, True),
        StructField("transactionExitFee", monetary_type, True),
        StructField("transactionNetValue", monetary_type, True)
    ])

    meta_schema = StructType([
        StructField("requestDateTime", StringType(), True)
    ])

    return StructType([
        StructField("data", ArrayType(transaction_schema), True),
        StructField("meta", meta_schema, True)
    ])

BRONZE_SCHEMA = """
    client_id                          STRING,
    investiment_id                     STRING,
    transaction_id                     STRING,
    type                               STRING,
    transaction_type                   STRING,
    transaction_type_additional_info   STRING,
    transaction_conversion_date        DATE,
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
    _rescued_data                      STRING,
    transaction_conversion_month       STRING
"""