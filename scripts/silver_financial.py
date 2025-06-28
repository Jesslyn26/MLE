import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from utils.data_processing_silver_table import process_silver_table_feat_financial

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
    bronze_financial_dir = "/opt/airflow/datamart/bronze/feat_financial/"
    silver_financial_dir = "/opt/airflow/datamart/silver/feat_financial/"

    # create SparkSession
    spark = SparkSession.builder \
        .appName("silver_feat_store") \
        .getOrCreate()

    # call your function
    process_silver_table_feat_financial(
        snapshot_date,
        bronze_financial_dir,
        silver_financial_dir,
        spark
    )

    spark.stop()

if __name__ == "__main__":
    main()