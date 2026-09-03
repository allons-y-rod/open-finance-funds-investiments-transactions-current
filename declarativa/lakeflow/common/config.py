from pyspark.sql.streaming import DataStreamReader

INPUT_PATH = "/Volumes/dlt_open_finance_funds_investiments_transactions_current/bronze/landing"

BRONZE_TABLE = "dlt_open_finance_funds_investiments_transactions_current.bronze.bronze_transactions_current"
SILVER_TABLE = "dlt_open_finance_funds_investiments_transactions_current.silver.silver_transactions_current"
SILVER_REJECTED_TABLE = "dlt_open_finance_funds_investiments_transactions_current.silver.silver_transactions_current_rechaco"

CLOUDFILES_OPTIONS = {
    "cloudFiles.format": "json",
    "cloudFiles.includeExistingFiles": "true",
    "cloudFiles.schemaEvolutionMode": "rescue",
    "cloudFiles.allowOverwrites": "true",
    "pathGlobFilter": "*.json",
    "multiline":"true"
}

def cloudfiles_reader(reader: DataStreamReader) -> DataStreamReader:
    reader = reader.format("cloudFiles")

    for option, value in CLOUDFILES_OPTIONS.items():
        reader = reader.option(option,value)

    return reader