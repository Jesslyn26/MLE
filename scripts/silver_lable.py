import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from utils.data_processing_silver_table import process_silver_table

def parse_args():
    parser = argparse.ArgumentParser(description="Silver financial feature store job")
    parser.add_argument(
        "--snapshotdate",
        required=True,
        help="Snapshot date in YYYY-MM-DD format (Airflow {{ ds }})"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    snapshot_date = args.snapshotdate
    bronze_lms_dir = "/opt/airflow/datamart/bronze/lms/"
    silver_lms_dir = "/opt/airflow/datamart/silver/loan_daily/"

    # create SparkSession
    spark = SparkSession.builder \
        .appName("silver_lable_store") \
        .getOrCreate()

    # call your function
    process_silver_table(
        snapshot_date,
        bronze_lms_dir,
        silver_lms_dir,
        spark
    )

    spark.stop()

if __name__ == "__main__":
    main()