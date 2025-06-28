import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from utils.data_processing_bronze_table import process_bronze_table_feat_clickstream

def parse_args():
    parser = argparse.ArgumentParser(description="Bronze feat store job")
    parser.add_argument(
        "--snapshotdate",
        required=True,
        help="Snapshot date in YYYY-MM-DD format (Airflow {{ ds }})"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    snapshot_date = args.snapshotdate
    output_dir = "/opt/airflow/datamart/bronze/clickstream/"

    # create SparkSession
    spark = SparkSession.builder \
        .appName("bronze_feat_store") \
        .getOrCreate()

    # call your function
    process_bronze_table_feat_clickstream(snapshot_date, output_dir, spark)

    spark.stop()

if __name__ == "__main__":
    main()
