import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from utils.data_processing_silver_table import process_silver_table_feat_attributes

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
    bronze_attributes_dir = "/opt/airflow/datamart/bronze/feat_attribute/"
    silver_attributes_dir = "/opt/airflow/datamart/silver/feat_attribute/"

    # create SparkSession
    spark = SparkSession.builder \
        .appName("silver_feat_store") \
        .getOrCreate()

    # call your function
    process_silver_table_feat_attributes(
        snapshot_date,
        bronze_attributes_dir,
        silver_attributes_dir,
        spark
    )

    spark.stop()

if __name__ == "__main__":
    main()