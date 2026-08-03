from pyspark.sql.streaming import DataStreamReader

INPUT_PATH = "/Volumes/funds-investiments-transactions-current/landing/"

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