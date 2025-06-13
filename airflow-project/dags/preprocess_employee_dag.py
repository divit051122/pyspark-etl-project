from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

# Define the DAG
with DAG(
    dag_id="preprocess_employee_data_dag",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually
    catchup=False
) as dag:

    # This task triggers a Databricks job by job_id
    trigger_databricks_job = DatabricksRunNowOperator(
        task_id="run_databricks_job",
        databricks_conn_id="databricks_default",  # Must be created in Airflow UI
        job_id=12345,  # 🔁 Replace this with your actual Databricks job_id
        notebook_params={
            "input_file": "employee_data.csv",
            "output_csv_path": "output/employee_data_cleaned.csv"
        }
    )
