TRANSACTION_BUSINESS_KEY = ["client_id", "transaction_id"]
DEDUP_ORDER = ["ingestion_ts", "source_file"]

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
