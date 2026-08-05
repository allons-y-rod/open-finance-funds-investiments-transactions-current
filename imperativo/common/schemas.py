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
