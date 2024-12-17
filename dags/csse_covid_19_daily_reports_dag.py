
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from operators.acquisition_operator import AcquisitionOperator
from operators.download_operator import DownloadOperator
from operators.load_operator import LoadOperator
from operators.snowflake_copy_operator import SnowflakeCopyOperator

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


# Define the DAG 
with DAG(
    dag_id="csse_covid_19_daily_reports_dag",
    default_args=default_args,
    description="A simple DAG with a Data ingestion",
    schedule_interval="0 23 * * 1-5",  # No schedule, triggered manually
    start_date=datetime(2021,1,1),
    max_active_runs=1 ,
    catchup=True,
        ) as dag:

        start = EmptyOperator(
            task_id="start"
        )


        # End task
        end = EmptyOperator(
            task_id="end"
        )

        
         # Task 1: Using the AcquisitionOperator
        acq_task = AcquisitionOperator(
            task_id="s3_file_check",
            s3_conn_id="S3_CONN_ID",
            bucket_name="rposam-devops-airflow",
            dataset_dir="datasets/csse_covid_19_daily_reports",
            file_pattern="datetime_pattern.csv",
            datetime_pattern="%m-%d-%Y"
        ) 
            
        download_task = DownloadOperator(
            task_id="download_file_from_s3",
            s3_conn_id="S3_CONN_ID",
            bucket_name="rposam-devops-airflow",
            dataset_dir="datasets/csse_covid_19_daily_reports",
            file_name="datetime_pattern.csv",
            datetime_pattern="%m-%d-%Y"
        )
            
        load_task = LoadOperator(
            task_id="move_file_to_snowflake",
            snowflake_conn_id="SNOWFLAKE_CONN_ID",
            stage_name="MIRROR_DB.MIRROR.STG_CSSE_COVID_19_DAILY_REPORTS"
        )
            
        copy_task = SnowflakeCopyOperator(
            task_id="copy_file_from_stage",
            snowflake_conn_id="SNOWFLAKE_CONN_ID",
            stage_name="MIRROR_DB.MIRROR.STG_CSSE_COVID_19_DAILY_REPORTS",
            table_name="MIRROR_DB.MIRROR.T_ML_CSSE_COVID_19_DAILY_REPORTS"
        )
              
        # Define task dependencies
        start >>  acq_task >> download_task >> load_task >> copy_task >> end

        