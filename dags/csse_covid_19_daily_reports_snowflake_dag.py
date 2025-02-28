
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from operators.acquisition_operator import AcquisitionOperator
from operators.download_operator import DownloadOperator
from operators.move_file_to_snowflake_operator import MoveFileToSnowflakeOperator
from operators.file_snowflake_table_schema_check_operator import FileSnowflakeTableSchemaCheckOperator
from operators.snowflake_copy_operator import SnowflakeCopyOperator
from operators.file_snowflake_table_data_check_operator import FileSnowflakeTableDataCheckOperator
from operators.snowflake_load_to_mirror_operator import SnowflakeLoadToMirrorOperator
from operators.snowflake_load_to_stage_operator import SnowflakeLoadToStageOperator

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


# Define the DAG 
with DAG(
    dag_id="csse_covid_19_daily_reports_snowflake_dag",
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
            task_id="check_file_present_on_s3",
            s3_conn_id="S3_CONN_ID",
            bucket_name="rposam-etl-source-datasets-s3",
            dataset_dir="datasets/csse_covid_19_daily_reports",
            file_pattern="{datetime_pattern}.csv",
            datetime_pattern="%m-%d-%Y"
        ) 
            
        download_task = DownloadOperator(
            task_id="download_file_from_s3_to_airflow_tmp_area",
            s3_conn_id="S3_CONN_ID",
            bucket_name="rposam-etl-source-datasets-s3",
            dataset_dir="datasets/csse_covid_19_daily_reports",
            file_name="{datetime_pattern}.csv",
            datetime_pattern="%m-%d-%Y"
        )
            
        move_to_snowflake_task = MoveFileToSnowflakeOperator(
            task_id="move_file_to_snowflake_internal_stage",
            db_conn_id="SNOWFLAKE_CONN_ID",
            stage_name="MIRROR_DB.MIRROR.STG_CSSE_COVID_19_DAILY_REPORTS_SNOWFLAKE"
        )
            
        snowflake_schema_check_task = FileSnowflakeTableSchemaCheckOperator(
            task_id="check_schema_of_config_n_received_file",
            db_conn_id="SNOWFLAKE_CONN_ID",
            s3_conn_id="S3_CONN_ID",
            bucket_name="rposam-etl-source-datasets-s3",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="csse_covid_19_daily_reports_snowflake",
            stage_name="MIRROR_DB.MIRROR.STG_CSSE_COVID_19_DAILY_REPORTS_SNOWFLAKE",
            table_name="MIRROR_DB.MIRROR.T_ML_CSSE_COVID_19_DAILY_REPORTS_SNOWFLAKE_TR"
        )
            
        copy_to_snowflake_task = SnowflakeCopyOperator(
            task_id="copy_data_from_internal_stage",
            db_conn_id="SNOWFLAKE_CONN_ID",
            s3_conn_id="S3_CONN_ID",
            bucket_name="rposam-etl-source-datasets-s3",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="csse_covid_19_daily_reports_snowflake",
            stage_name="MIRROR_DB.MIRROR.STG_CSSE_COVID_19_DAILY_REPORTS_SNOWFLAKE",
            table_name="MIRROR_DB.MIRROR.T_ML_CSSE_COVID_19_DAILY_REPORTS_SNOWFLAKE_TR"
        )
            
        snowflake_file_mirror_data_check_task = FileSnowflakeTableDataCheckOperator(
            task_id="check_file_n_mirror_table_data",
            db_conn_id="SNOWFLAKE_CONN_ID",
            s3_conn_id="S3_CONN_ID",
            bucket_name="rposam-etl-source-datasets-s3",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="csse_covid_19_daily_reports_snowflake",
            table_name="MIRROR_DB.MIRROR.T_ML_CSSE_COVID_19_DAILY_REPORTS_SNOWFLAKE_TR"
        )
            
        snowflake_mirror_task = SnowflakeLoadToMirrorOperator(
            task_id="load_to_mirror_table",
            s3_conn_id="S3_CONN_ID",
            db_conn_id="SNOWFLAKE_CONN_ID",
            bucket_name="rposam-etl-source-datasets-s3",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="csse_covid_19_daily_reports_snowflake"
        )
            
        snowflake_stage_task = SnowflakeLoadToStageOperator(
            task_id="load_to_stage_table",
            s3_conn_id="S3_CONN_ID",
            db_conn_id="SNOWFLAKE_CONN_ID",
            bucket_name="rposam-etl-source-datasets-s3",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="csse_covid_19_daily_reports_snowflake"
        )
              
        # Define task dependencies
        start >>  acq_task >> download_task >> move_to_snowflake_task >> snowflake_schema_check_task >> copy_to_snowflake_task >> snowflake_file_mirror_data_check_task >> snowflake_mirror_task >> snowflake_stage_task >> end

        