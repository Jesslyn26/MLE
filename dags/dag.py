from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag',
    default_args=default_args,
    description='data pipeline run once a month',
    schedule_interval='0 0 1 * *',  
    start_date=datetime(2023, 2, 1),
    end_date=datetime(2024, 12, 1),
    catchup=True,
) as dag:
    
    spark = SparkSession.builder.appName("airflow").getOrCreate()
    
    # --- label store --
    def check_bronze_label(**kwargs):
        path = "/opt/airflow/datamart/bronze/lms/"
        if not os.path.exists(path) or not os.listdir(path):
            raise FileNotFoundError(f"Bronze lms directory {path} is missing or empty.")
        
    dep_check_source_label_data = PythonOperator(
        task_id='dep_check_source_label_data',
        python_callable=check_bronze_label,
        provide_context=True,
        dag=dag,
    )

    bronze_label_store = BashOperator(
        task_id='bronze_label_store_label',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'PYTHONPATH=/opt/airflow python3 bronze_lable.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    silver_label_store =BashOperator(
        task_id='silver_label_store_label',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'PYTHONPATH=/opt/airflow python3 silver_lable.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    gold_label_store = BashOperator(
        task_id='gold_label_store_label',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'PYTHONPATH=/opt/airflow python3 gold_lable.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )
    label_store_completed = DummyOperator(task_id="label_store_completed")

    dep_check_source_label_data >> bronze_label_store >> silver_label_store >> gold_label_store >> label_store_completed
  
    # --- feature store ---
    def check_bronze_clickstream(**kwargs):
        path = "/opt/airflow/datamart/bronze/clickstream/"
        if not os.path.exists(path) or not os.listdir(path):
            raise FileNotFoundError(f"Bronze clickstream directory {path} is missing or empty.")

    def check_bronze_attributes(**kwargs):
        path = "/opt/airflow/datamart/bronze/feat_attributes/"
        if not os.path.exists(path) or not os.listdir(path):
            raise FileNotFoundError(f"Bronze attributes directory {path} is missing or empty.")

    def check_bronze_financial(**kwargs):
        path = "/opt/airflow/datamart/bronze/feat_financial/"
        if not os.path.exists(path) or not os.listdir(path):
            raise FileNotFoundError(f"Bronze financial directory {path} is missing or empty.")

    dep_check_bronze_clickstream = PythonOperator(
        task_id='dep_check_bronze_clickstream',
        python_callable=check_bronze_clickstream,
        provide_context=True,
        dag=dag,
    )
    dep_check_bronze_attributes = PythonOperator(
        task_id="dep_check_bronze_attributes",
        python_callable=check_bronze_attributes,
        provide_context=True,
        dag=dag,
    )

    dep_check_bronze_financial = PythonOperator(
        task_id="dep_check_bronze_financial",
        python_callable=check_bronze_financial,
        provide_context=True,
        dag=dag,
    )

    # Bronze Features
    bronze_clickstream = BashOperator(
        task_id='bronze_clickstream',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'PYTHONPATH=/opt/airflow python3 bronze_clickstream.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    bronze_attributes = BashOperator(
        task_id='bronze_attribute',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'PYTHONPATH=/opt/airflow python3 bronze_attribute.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    bronze_financial = BashOperator(
        task_id='bronze_financial',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'PYTHONPATH=/opt/airflow python3 bronze_financial.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    # Silver Feature
    silver_clickstream = BashOperator(
        task_id='silver_clickstream',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'PYTHONPATH=/opt/airflow python3 silver_clickstream.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )
    silver_attributes = BashOperator(
        task_id='silver_attribute',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'PYTHONPATH=/opt/airflow python3 silver_attribute.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    silver_financial = BashOperator(
        task_id='silver_financial',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'PYTHONPATH=/opt/airflow python3 silver_financial.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    # Gold Feature Store
    gold_feature_store = BashOperator(
        task_id='gold_feature_store',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'PYTHONPATH=/opt/airflow python3 gold_feature.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    feature_store_completed = DummyOperator(task_id="feature_store_completed")

    # Set dependencies: bronze -> silver -> gold
    dep_check_bronze_clickstream >> bronze_clickstream >> silver_clickstream >> gold_feature_store
    dep_check_bronze_attributes >> bronze_attributes >> silver_attributes >> gold_feature_store
    dep_check_bronze_financial >> bronze_financial >> silver_financial >> gold_feature_store
    gold_feature_store >> feature_store_completed

    # --- model inference ---
    model_inference_start = DummyOperator(task_id="model_inference_start")
    model_inference = BashOperator(
        task_id='model_inference',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'PYTHONPATH=/opt/airflow python3 model_inference.py '
            '--snapshotdate "{{ ds }}" --modelname credit_model_2024_09_02.pkl'
        ),
    )
   
    model_inference_completed = DummyOperator(task_id="model_inference_completed")
    feature_store_completed >> model_inference_start
    model_inference_start >> model_inference >> model_inference_completed
    

    # --- model monitoring ---
    model_monitor_start = DummyOperator(task_id="model_monitor_start")
    model_monitoring = BashOperator(
        task_id='model_monitoring',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'PYTHONPATH=/opt/airflow python3 model_monitoring.py '
            '--snapshotdate "{{ ds }}" '
            '--modelname credit_model_2024_09_02.pkl'
        ),
    )
    model_monitor_completed = DummyOperator(task_id="model_monitor_completed")
    model_inference_completed >> model_monitor_start
    model_monitor_start >> model_monitoring >> model_monitor_completed
    