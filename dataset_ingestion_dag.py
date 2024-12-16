from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from operators.download_operator import DownloadOperator
from operators.acquisition_operator import AcquisitionOperator
from operators.load_operator import LoadOperator
from operators.snowflake_copy_operator import SnowflakeCopyOperator

from datetime import datetime

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

date_pattern = datetime.strftime(datetime.today(), "%Y-%m-%d")

# Define the DAG
with DAG(
    dag_id="data_ingestion_dag",
    default_args=default_args,
    description="A simple DAG with a Data ingestion",
    schedule_interval=None,  # No schedule, triggered manually
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    # Task 1: Using the AcquisitionOperator
    acq_task = AcquisitionOperator(
        task_id="s3_file_check",
        s3_conn_id="S3_CONN_ID",
        bucket_name="rposam-devops-airflow",
        dataset_dir=f"datasets/{date_pattern}/",
        file_pattern="Netflix_Movies_and_TV_Shows.csv"
    )

    download_task = DownloadOperator(
        task_id="download_file_from_s3",
        s3_conn_id="S3_CONN_ID",
        bucket_name="rposam-devops-airflow",
        dataset_dir=f"datasets/{date_pattern}/",
        file_name="Netflix_Movies_and_TV_Shows.csv"
    )

    load_task = LoadOperator(
            task_id="move_file_to_snowflake",
            snowflake_conn_id="SNOWFLAKE_CONN_ID",
            stage_name="DEVOPS_DB.DEVOPS.STG_DATASET_DUMP"
        )

    copy_task = SnowflakeCopyOperator(
        task_id="copy_file_from_stage",
        snowflake_conn_id="SNOWFLAKE_CONN_ID",
        stage_name="DEVOPS_DB.DEVOPS.STG_DATASET_DUMP",
        table_name="DEVOPS_DB.DEVOPS.T_NETFLIX_DATA"
    )

    start = EmptyOperator(
        task_id="start"
    )


    # End task
    end = EmptyOperator(
        task_id="end"
    )

    # Define task dependencies
    start >>  acq_task >> download_task >> load_task >> copy_task >> end