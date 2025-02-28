
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from operators.acquisition_operator import AcquisitionOperator
from operators.download_operator import DownloadOperator
from operators.file_postgres_table_schema_check_operator import FilePostgresTableSchemaCheckOperator
from operators.copy_file_to_postgres_operator import CopyFileToPostgresOperator
from operators.file_postgres_table_data_check_operator import FilePostgresTableDataCheckOperator
from operators.postgres_load_to_mirror_operator import PostgresLoadToMirrorOperator
from operators.postgres_load_to_stage_operator import PostgresLoadToStageOperator

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


# Define the DAG 
with DAG(
    dag_id="csse_covid_19_daily_reports_postgres_dag",
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
            
        postgres_schema_check_task = FilePostgresTableSchemaCheckOperator(
            task_id="check_schema_of_config_n_received_file",
            db_conn_id="POSTGRES_CONN_ID",
            s3_conn_id="S3_CONN_ID",
            bucket_name="rposam-etl-source-datasets-s3",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="csse_covid_19_daily_reports_postgres"
        )
            
        copy_to_postgres_task = CopyFileToPostgresOperator(
            task_id="copy_data_from_file_to_postgres",
            db_conn_id="POSTGRES_CONN_ID",            
            table_name="COMMON_DB.MIRROR.T_ML_CSSE_COVID_19_DAILY_REPORTS_POSTGRES_TR",
            file_format_params={'delimiter': ',', 'skip_header': 1, 'compressed': True},
            datetime_pattern="MM-DD-YYYY"
        )
            
        postgres_file_mirror_data_check_task = FilePostgresTableDataCheckOperator(
            task_id="check_file_n_mirror_table_data",
            db_conn_id="POSTGRES_CONN_ID",
            s3_conn_id="S3_CONN_ID",
            bucket_name="rposam-etl-source-datasets-s3",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="csse_covid_19_daily_reports_postgres",
            table_name="COMMON_DB.MIRROR.T_ML_CSSE_COVID_19_DAILY_REPORTS_POSTGRES_TR"
        )
            
        postgres_mirror_task = PostgresLoadToMirrorOperator(
            task_id="load_to_mirror_table",
            s3_conn_id="S3_CONN_ID",
            db_conn_id="POSTGRES_CONN_ID",
            bucket_name="rposam-etl-source-datasets-s3",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="csse_covid_19_daily_reports_postgres"
        )
            
        postgres_stage_task = PostgresLoadToStageOperator(
            task_id="load_to_stage_table",
            s3_conn_id="S3_CONN_ID",
            db_conn_id="POSTGRES_CONN_ID",
            bucket_name="rposam-etl-source-datasets-s3",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="csse_covid_19_daily_reports_postgres"
        )
              
        # Define task dependencies
        start >>  acq_task >> download_task >> postgres_schema_check_task >> copy_to_postgres_task >> postgres_file_mirror_data_check_task >> postgres_mirror_task >> postgres_stage_task >> end

        