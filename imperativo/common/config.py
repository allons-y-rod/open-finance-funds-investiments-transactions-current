from pyspark.sql.streaming import DataStreamReader

INPUT_PATH = "/Volumes/imperative_open_finance_funds_investiments_transactions_current/bronze/landing"
CHECKPOINT_PATH = "/Volumes/imperative_open_finance_funds_investiments_transactions_current/bronze/checkpoints/bronze_transactions_current_imperative"

TARGET_TABLE = "imperative_open_finance_funds_investiments_transactions_current.bronze.bronze_transactions_current"
REJECTED_TABLE = "imperative_open_finance_funds_investiments_transactions_current.bronze.bronze_transactions_current_rechaco"

CLOUDFILES_OPTIONS = {
    "cloudFiles.format": "json",
    "cloudFiles.includeExistingFiles": "true",
    "cloudFiles.schemaEvolutionMode": "rescue",
    "cloudFiles.schemaLocation": CHECKPOINT_PATH,
    "cloudFiles.allowOverwrites": "true",
    "cloudFiles.maxFilesPerTrigger": "1000",
    "pathGlobFilter": "*.json",
    "multiline": "true",
}

def cloudfiles_reader(reader: DataStreamReader) -> DataStreamReader:
    reader = reader.format("cloudFiles")

    for option, value in CLOUDFILES_OPTIONS.items():
        reader = reader.option(option, value)

    return reader
