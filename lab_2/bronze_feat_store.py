import argparse
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
import pyspark.sql.functions as F

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

import utils.data_processing_bronze_table

def main(snapshotdate):
    print('\n\n---starting job for feat---\n\n')
    
    # Initialize SparkSession
    spark = pyspark.sql.SparkSession.builder \
        .appName("dev") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level to ERROR to hide warnings
    spark.sparkContext.setLogLevel("ERROR")

    # load arguments
    date_str = snapshotdate
    
    # create bronze datalake
    bronze_feat_clicksteam_directory = "datamart/bronze/clickstream/"
    bronze_feat_attributes_directory = "datamart/bronze/feat_attributes/"
    bronze_feat_financial_directory = "datamart/bronze/feat_financial/"
    
    if not os.path.exists(bronze_feat_clicksteam_directory):
        os.makedirs(bronze_feat_clicksteam_directory)
    
    if not os.path.exists(bronze_feat_attributes_directory):
        os.makedirs(bronze_feat_attributes_directory)

    if not os.path.exists(bronze_feat_financial_directory):
        os.makedirs(bronze_feat_financial_directory)

    # run data processing
    utils.data_processing_bronze_table.process_bronze_table_feat_clickstream(date_str, bronze_feat_clicksteam_directory, spark)
    utils.data_processing_bronze_table.process_bronze_table_feat_attri(date_str, bronze_feat_attributes_directory, spark)
    utils.data_processing_bronze_table.process_bronze_table_feat_financial(date_str, bronze_feat_financial_directory, spark)

    
    # end spark session
    spark.stop()
    
    print('\n\n---completed job for feat bronze table---\n\n')

if __name__ == "__main__":
    # Setup argparse to parse command-line arguments
    parser = argparse.ArgumentParser(description="run job")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")
    
    args = parser.parse_args()
    
    # Call main with arguments explicitly passed
    main(args.snapshotdate)
