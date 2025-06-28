import argparse
import os
import pandas as pd
import mlflow

parser = argparse.ArgumentParser(description="Run model monitoring job")
parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")
parser.add_argument("--modelname", type=str, required=True, help="model_name.pkl")
args = parser.parse_args()
snapshotdate = args.snapshotdate
modelname = args.modelname

mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("model_inference_monitoring")

# Ensure artifact directory exists
artifact_dir = "artifacts"
os.makedirs(artifact_dir, exist_ok=True)

model_dir = f"/opt/airflow/datamart/gold/model_predictions_airflow/{modelname[:-4]}"
pred_file = f"{modelname[:-4]}_predictions_{snapshotdate.replace('-', '_')}.parquet"
pred_path = os.path.join(model_dir, pred_file)

if not os.path.exists(pred_path):
    print(f"Prediction file missing for {snapshotdate}, skipping.")
    exit(0)

pred_df = pd.read_parquet(pred_path)

with mlflow.start_run(run_name=f"Inference_{modelname}_{snapshotdate}"):
    # Log config and prediction stats
    mlflow.log_param("model_name", modelname)
    mlflow.log_param("snapshot_date", snapshotdate)
    mlflow.log_metric("mean_prediction", float(pred_df["model_predictions"].mean()))
    mlflow.log_metric("max_prediction", float(pred_df["model_predictions"].max()))
    mlflow.log_metric("min_prediction", float(pred_df["model_predictions"].min()))
    mlflow.log_metric("n_predictions", len(pred_df))

    # Save and log predictions as artifact
    pred_parquet = os.path.join(artifact_dir, f"{modelname}_inference_{snapshotdate}.parquet")
    pred_df.to_parquet(pred_parquet, index=False)
    mlflow.log_artifact(pred_parquet)
