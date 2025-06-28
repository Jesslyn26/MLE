import argparse
import os
import glob
import pandas as pd
import pickle
import pprint
from datetime import datetime
import pyspark
from pyspark.sql.functions import col

def main(snapshotdate, modelname):
    print('\n\n---starting job---\n\n')
    
    # Initialize SparkSession
    spark = pyspark.sql.SparkSession.builder \
        .appName("dev") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # --- set up config ---
    config = {}
    config["snapshot_date_str"] = snapshotdate
    config["snapshot_date"] = datetime.strptime(config["snapshot_date_str"], "%Y-%m-%d")
    config["model_name"] = modelname
    config["model_bank_directory"] = "model_bank/"
    config["model_artefact_filepath"] = config["model_bank_directory"] + config["model_name"]
    pprint.pprint(config)
    
    # --- load model artefact from model bank ---
    with open(config["model_artefact_filepath"], 'rb') as file:
        model_artefact = pickle.load(file)
    print("Model loaded successfully! " + config["model_artefact_filepath"])
    
    # --- load feature store ---
    feature_location = f"datamart/gold/feature_store/gold_table_{config['snapshot_date_str'].replace('-', '_')}.parquet"
    features_store_sdf = spark.read.parquet(feature_location)
    features_sdf = features_store_sdf.filter(col("snapshot_date") == config["snapshot_date"])
    print("extracted features_sdf", features_sdf.count(), config["snapshot_date"])
    features_pdf = features_sdf.toPandas()
    
    # --- preprocess data for modeling ---
    features_pdf['snapshot_date'] = features_pdf['snapshot_date'].astype(str)

    # Drop only non-feature columns, keep all features used in training
    non_feature_cols = [
        'Customer_ID',
        'clickstream_snapshot_date',
        'attributes_snapshot_date',
        'financial_snapshot_date',
        'snapshot_date'
    ]

    X_inference = features_pdf.drop(columns=non_feature_cols).values  # Convert to numpy array

    # Apply the scaler
    transformer_stdscaler = model_artefact["preprocessing_transformers"]["stdscaler"]
    X_inference = transformer_stdscaler.transform(X_inference)
    print('X_inference', X_inference.shape[0])

    # --- model prediction inference ---
    model = model_artefact["model"]
    y_inference = model.predict_proba(X_inference)[:, 1]

    # prepare output
    y_inference_pdf = features_pdf[["Customer_ID", "snapshot_date"]].copy()
    y_inference_pdf["model_name"] = config["model_name"]
    y_inference_pdf["model_predictions"] = y_inference

    # --- save model inference to datamart gold table ---
    gold_directory = f"datamart/gold/model_predictions/{config['model_name'][:-4]}/"
    print(gold_directory)
    if not os.path.exists(gold_directory):
        os.makedirs(gold_directory)
    partition_name = config["model_name"][:-4] + "_predictions_" + config["snapshot_date_str"].replace('-','_') + '.parquet'
    filepath = gold_directory + partition_name
    spark.createDataFrame(y_inference_pdf).write.mode("overwrite").parquet(filepath)
    print('saved to:', filepath)

    # --- end spark session --- 
    spark.stop()
    print('\n\n---completed job---\n\n')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="run job")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")
    parser.add_argument("--modelname", type=str, required=True, help="model_name")
    args = parser.parse_args()
    main(args.snapshotdate, args.modelname)