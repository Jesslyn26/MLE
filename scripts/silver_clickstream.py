import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from utils.data_processing_silver_table import process_silver_table_feat_clickstream

def parse_args():
    parser = argparse.ArgumentParser(description="Silver clickstream feature store job")
    parser.add_argument(
        "--snapshotdate",
        required=True,
        help="Snapshot date in YYYY-MM-DD format (Airflow {{ ds }})"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    snapshot_date = args.snapshotdate
    bronze_clickstream_dir = "/opt/airflow/datamart/bronze/clickstream/"
    silver_clickstream_dir = "/opt/airflow/datamart/silver/clickstream/"

    # create SparkSession
    spark = SparkSession.builder \
        .appName("silver_feat_store") \
        .getOrCreate()

    # call your function
    process_silver_table_feat_clickstream(
        snapshot_date,
        bronze_clickstream_dir,
        silver_clickstream_dir,
        spark
    )

    spark.stop()

if __name__ == "__main__":
    main()