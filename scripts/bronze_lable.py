import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from utils.data_processing_bronze_table import process_bronze_table

def parse_args():
    parser = argparse.ArgumentParser(description="Bronze label store job")
    parser.add_argument(
        "--snapshotdate",
        required=True,
        help="Snapshot date in YYYY-MM-DD format (Airflow {{ ds }})"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    snapshot_date = args.snapshotdate
    output_dir = "/opt/airflow/datamart/bronze/lms/"

    # create SparkSession
    spark = SparkSession.builder \
        .appName("bronze_label_store") \
        .getOrCreate()

    # call your function
    process_bronze_table(snapshot_date, output_dir, spark)

    spark.stop()

if __name__ == "__main__":
    main()
