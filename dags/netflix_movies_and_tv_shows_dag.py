
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from operators.acquisition_operator import AcquisitionOperator
from operators.download_operator import DownloadOperator
from operators.move_file_to_snowflake_operator import MoveFileToSnowflakeOperator
from operators.file_table_schema_check_operator import FileTableSchemaCheckOperator
from operators.snowflake_copy_operator import SnowflakeCopyOperator
from operators.mirror_load_operator import MirrorLoadOperator
from operators.stage_load_operator import StageLoadOperator

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


# Define the DAG 
with DAG(
    dag_id="netflix_movies_and_tv_shows_dag",
    default_args=default_args,
    description="A simple DAG with a Data ingestion",
    schedule_interval="0 23 * * 1-5",  # No schedule, triggered manually
    start_date=datetime(2024,12,1),
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
            task_id="check_file_present_on_s3",
            s3_conn_id="S3_CONN_ID",
            bucket_name="rposam-devops-airflow",
            dataset_dir="datasets",
            file_pattern="Netflix_Movies_and_TV_Shows.csv",
            datetime_pattern=""
        ) 
            
        download_task = DownloadOperator(
            task_id="download_file_from_s3_to_airflow_tmp_area",
            s3_conn_id="S3_CONN_ID",
            bucket_name="rposam-devops-airflow",
            dataset_dir="datasets",
            file_name="Netflix_Movies_and_TV_Shows.csv",
            datetime_pattern=""
        )
            
        move_task = MoveFileToSnowflakeOperator(
            task_id="move_file_to_snowflake_internal_stage",
            snowflake_conn_id="SNOWFLAKE_CONN_ID",
            stage_name="MIRROR_DB.MIRROR.STG_NETFLIX_MOVIES_AND_TV_SHOWS"
        )
            
        schema_check_task = FileTableSchemaCheckOperator(
            task_id="check_schema_of_config_n_received_file",
            snowflake_conn_id="SNOWFLAKE_CONN_ID",
            s3_conn_id="S3_CONN_ID",
            bucket_name="rposam-devops-airflow",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="netflix_movies_and_tv_shows",
            stage_name="MIRROR_DB.MIRROR.STG_NETFLIX_MOVIES_AND_TV_SHOWS",
            table_name="MIRROR_DB.MIRROR.T_ML_NETFLIX_MOVIES_AND_TV_SHOWS_TR"
        )
            
        copy_task = SnowflakeCopyOperator(
            task_id="copy_data_from_internal_stage",
            snowflake_conn_id="SNOWFLAKE_CONN_ID",
            s3_conn_id="S3_CONN_ID",
            bucket_name="rposam-devops-airflow",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="netflix_movies_and_tv_shows",
            stage_name="MIRROR_DB.MIRROR.STG_NETFLIX_MOVIES_AND_TV_SHOWS",
            table_name="MIRROR_DB.MIRROR.T_ML_NETFLIX_MOVIES_AND_TV_SHOWS_TR"
        )
            
        mirror_task = MirrorLoadOperator(
            task_id="load_to_mirror_table",
            s3_conn_id="S3_CONN_ID",
            snowflake_conn_id="SNOWFLAKE_CONN_ID",
            bucket_name="rposam-devops-airflow",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="netflix_movies_and_tv_shows"
        )
            
        stage_task = StageLoadOperator(
            task_id="load_to_stage_table",
            s3_conn_id="S3_CONN_ID",
            snowflake_conn_id="SNOWFLAKE_CONN_ID",
            bucket_name="rposam-devops-airflow",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="netflix_movies_and_tv_shows"
        )
              
        # Define task dependencies
        start >>  acq_task >> download_task >> move_task >> schema_check_task >> copy_task >> mirror_task >> stage_task >> end

        