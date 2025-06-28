import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from utils.data_processing_gold_table import process_labels_gold_table

def parse_args():
    parser = argparse.ArgumentParser(description="Gold label store job")
    parser.add_argument(
        "--snapshotdate",
        required=True,
        help="Snapshot date in YYYY-MM-DD format (Airflow {{ ds }})"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    snapshot_date = args.snapshotdate

    silver_label_dir = "/opt/airflow/datamart/silver/loan_daily/"
    gold_label_dir = "/opt/airflow/datamart/gold/label_store/"

    # create SparkSession
    spark = SparkSession.builder \
        .appName("gold_label_store") \
        .getOrCreate()

    # call your function
    process_labels_gold_table(
        snapshot_date,
        silver_label_dir,
        gold_label_dir,
        spark,
        dpd = 30, 
        mob = 6
    )

    spark.stop()

if __name__ == "__main__":
    main()