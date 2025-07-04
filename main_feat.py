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
from scipy.stats import chi2_contingency
import seaborn as sns

from pyspark.sql.functions import col, lit, sqrt, pow, when, regexp_extract, mean, col, array_contains
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType, NumericType

import utils.data_processing_bronze_table
import utils.data_processing_silver_table
import utils.data_processing_gold_table


# Initialize SparkSession
spark = pyspark.sql.SparkSession.builder \
    .appName("dev") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to ERROR to hide warnings
spark.sparkContext.setLogLevel("ERROR")

# set up config
snapshot_date_str = "2023-01-01"

start_date_str = "2023-01-01"
end_date_str = "2024-12-01"

# generate list of dates to process
def generate_first_of_month_dates(start_date_str, end_date_str):
    # Convert the date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # List to store the first of month dates
    first_of_month_dates = []

    # Start from the first of the month of the start_date
    current_date = datetime(start_date.year, start_date.month, 1)

    while current_date <= end_date:
        # Append the date in yyyy-mm-dd format
        first_of_month_dates.append(current_date.strftime("%Y-%m-%d"))
        
        # Move to the first of the next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)

    return first_of_month_dates

dates_str_lst = generate_first_of_month_dates(start_date_str, end_date_str)
print(dates_str_lst)


###############################
# create bronze table
###############################
bronze_clickstream_directory = "datamart/bronze/clickstream/"
bronze_attributes_directory = "datamart/bronze/feat_attributes/"
bronze_financial_directory = "datamart/bronze/feat_financial/"

if not os.path.exists(bronze_clickstream_directory):
    os.makedirs(bronze_feat_clicksteam_directory)

# run data processing
for date_str in dates_str_lst:
    utils.data_processing_bronze_table.process_bronze_table_feat_clickstream(date_str, bronze_clickstream_directory, spark)

print('\n\n---completed job for feat clickstream bronze table---\n\n')


if not os.path.exists(bronze_attributes_directory):
    os.makedirs(bronze_attributes_directory)

for date_str in dates_str_lst:
    utils.data_processing_bronze_table.process_bronze_table_feat_attributes(date_str, bronze_attributes_directory, spark)

print('\n\n---completed job for feat attribute bronze table---\n\n')


bronze_feat_financial_directory = "datamart/bronze/feat_financial/"

if not os.path.exists(bronze_financial_directory):
    os.makedirs(bronze_feat_financial_directory)

for date_str in dates_str_lst:
 utils.data_processing_bronze_table.process_bronze_table_feat_financial(date_str, bronze_financial_directory, spark)

print('\n\n---completed job for feat financial bronze table---\n\n')





###############################
# create silver table
###############################

silver_clickstream_directory = "datamart/silver/clickstream/"
silver_attributes_directory = "datamart/silver/feat_attributes/"
silver_financial_directory = "datamart/silver/feat_financial/"

if not os.path.exists(silver_attributes_directory):
    os.makedirs(silver_attributes_directory)

for date_str in dates_str_lst:
    utils.data_processing_silver_table.process_silver_table_feat_attributes(
        date_str, bronze_attributes_directory, silver_attributes_directory, spark
    )
print('\n\n---completed job for feat attribute bronze silver---\n\n')


if not os.path.exists(silver_financial_directory):
    os.makedirs(silver_financial_directory)

for date_str in dates_str_lst:
    utils.data_processing_silver_table.process_silver_table_feat_financial(
        date_str, bronze_financial_directory, silver_financial_directory, spark
    )

print('\n\n---completed job for feat financial silver silver---\n\n')


if not os.path.exists(bronze_clickstream_directory):
    os.makedirs(bronze_clickstream_directory)

for date_str in dates_str_lst:
    utils.data_processing_silver_table.process_silver_table_feat_clickstream(
        date_str, bronze_clickstream_directory, silver_clickstream_directory, spark
    )

print('\n\n---completed job for feat clickstream silver silver---\n\n')





###############################
# create gold table
###############################

####

# Define directories for the gold table
gold_table_directory = "datamart/gold/feature_store/"

# Ensure the gold table directory exists
if not os.path.exists(gold_table_directory):
    os.makedirs(gold_table_directory)

# Process the gold table for each date in the list
for date_str in dates_str_lst:
    utils.data_processing_gold_table.process_gold_table(
        date_str,
        silver_clickstream_directory,
        silver_attributes_directory,
        silver_financial_directory,
        gold_table_directory,
        spark
    )


