
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
from operators.postgres_mirror_tests_operator import PostgresMirrorTestsOperator
from operators.postgres_load_to_stage_operator import PostgresLoadToStageOperator
from operators.postgres_stage_tests_operator import PostgresStageTestsOperator

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


# Define the DAG 
with DAG(
    dag_id="users_dag",
    default_args=default_args,
    description="A simple DAG with a Data ingestion",
    schedule="@once",  # No schedule, triggered manually
    start_date=datetime(2026,6,1),
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
            task_id="check_file_present",
            s3_conn_id=None,
            bucket_name=None,
            dataset_dir=r"/opt/airflow/datasets/ecommerce_dataset",
            file_pattern="users.csv",
            datetime_pattern=""
        ) 
            
        download_task = DownloadOperator(
            task_id="download_file_to_airflow_tmp_area",
            s3_conn_id=None,
            bucket_name=None,
            dataset_dir=r"/opt/airflow/datasets/ecommerce_dataset",
            file_name="users.csv",
            datetime_pattern=""
        )
            
        postgres_schema_check_task = FilePostgresTableSchemaCheckOperator(
            task_id="check_schema_of_config_n_received_file",
            db_conn_id="POSTGRES_CONN_ID",
            s3_conn_id=None,
            bucket_name=None,
            configs_path="/opt/airflow/configs/",
            dataset_name="users",
            encoding="UTF-8"
        )
            
        copy_to_postgres_task = CopyFileToPostgresOperator(
            task_id="copy_data_from_file_to_postgres",
            db_conn_id="POSTGRES_CONN_ID",
            encoding="UTF-8",            
            table_name="ECOMMERCE_DB.BRONZE.T_ML_USERS_TR",
            file_format_params={'delimiter': ',', 'skip_header': 1, 'compressed': True},
            datetime_pattern=""
        )
            
        postgres_file_mirror_data_check_task = FilePostgresTableDataCheckOperator(
            task_id="check_file_n_mirror_table_data",
            db_conn_id="POSTGRES_CONN_ID",
            s3_conn_id=None,
            bucket_name=None,
            configs_path="/opt/airflow/configs/",
            dataset_name="users",
            encoding="UTF-8",
            table_name="ECOMMERCE_DB.BRONZE.T_ML_USERS_TR"
        )
            
        postgres_mirror_task = PostgresLoadToMirrorOperator(
            task_id="load_to_mirror_table",
            s3_conn_id=None,
            db_conn_id="POSTGRES_CONN_ID",
            bucket_name=None,
            configs_path="/opt/airflow/configs/",
            dataset_name="users"
        )
            
        postgres_mirror_tests_task = PostgresMirrorTestsOperator(
            task_id="mirror_data_tests",
            s3_conn_id=None,
            db_conn_id="POSTGRES_CONN_ID",
            bucket_name=None,
            configs_path="/opt/airflow/configs/",
            dataset_name="users"
        )
            
        postgres_stage_task = PostgresLoadToStageOperator(
            task_id="load_to_stage_table",
            s3_conn_id=None,
            db_conn_id="POSTGRES_CONN_ID",
            bucket_name=None,
            configs_path="/opt/airflow/configs/",
            dataset_name="users"
        )
            
        postgres_stage_tests_task = PostgresStageTestsOperator(
            task_id="stage_data_tests",
            s3_conn_id=None,
            db_conn_id="POSTGRES_CONN_ID",
            bucket_name=None,
            configs_path="/opt/airflow/configs/",
            dataset_name="users"
        )
              
        # Define task dependencies
        start >>  acq_task >> download_task >> postgres_schema_check_task >> copy_to_postgres_task >> postgres_file_mirror_data_check_task >> postgres_mirror_task >> postgres_mirror_tests_task >> postgres_stage_task >> postgres_stage_tests_task >> end

        