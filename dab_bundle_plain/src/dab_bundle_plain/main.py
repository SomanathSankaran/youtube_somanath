from databricks.sdk.runtime import spark,dbutils,display
from pyspark.sql import DataFrame
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless(True).getOrCreate()

def find_all_taxis(table: str) -> DataFrame:
    print(f"Reading table {table}...")
    return spark.read.table(table)


def main():
    print("Finding all taxis...")
    display(find_all_taxis("samples.nyctaxi.trips").limit(10))


if __name__ == "__main__":
    main()
