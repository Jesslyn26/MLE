import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from utils.data_processing_gold_table import process_gold_table

def parse_args():
    parser = argparse.ArgumentParser(description="Gold feature store job")
    parser.add_argument(
        "--snapshotdate",
        required=True,
        help="Snapshot date in YYYY-MM-DD format (Airflow {{ ds }})"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    snapshot_date = args.snapshotdate

    silver_clickstream_dir = "/opt/airflow/datamart/silver/clickstream/"
    silver_attributes_dir = "/opt/airflow/datamart/silver/feat_attribute/"
    silver_financial_dir = "/opt/airflow/datamart/silver/feat_financial/"
    gold_feature_dir = "/opt/airflow/datamart/gold/feature/"

    # create SparkSession
    spark = SparkSession.builder \
        .appName("gold_feat_store") \
        .getOrCreate()

    # call your function
    process_gold_table(
        snapshot_date,
        silver_clickstream_dir,
        silver_attributes_dir,
        silver_financial_dir,
        gold_feature_dir,
        spark
    )

    spark.stop()

if __name__ == "__main__":
    main()